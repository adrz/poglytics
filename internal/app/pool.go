package app

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"poglytics-scraper/internal/config"
	"poglytics-scraper/internal/db"
	"poglytics-scraper/internal/metrics"
	"poglytics-scraper/internal/twitch"
)

// ConnectionPool manages multiple IRC connections to distribute channel load
type ConnectionPool struct {
	connections           []*IRCConnection
	channelsPerConnection int
	totalChannels         int
	sharedDB              DatabaseInterface
	twitchClient          *twitch.Client
	ctx                   context.Context
	cancel                context.CancelFunc
	mu                    sync.RWMutex

	// Centralized message handling
	centralMessageChan chan *ChatMessage
	dbWriterWg         sync.WaitGroup
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(totalChannels, channelsPerConnection int) (*ConnectionPool, error) {
	if channelsPerConnection <= 0 {
		channelsPerConnection = 50 // Default safe value
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Initialize shared database (all connections will use the same DB)
	env, err := config.LoadEnv()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to load environment: %v", err)
	}

	dbConfig := config.GetDBConfig(env)
	dbCfg := &db.DBConfig{
		Type:     dbConfig["type"],
		Host:     dbConfig["host"],
		Port:     dbConfig["port"],
		User:     dbConfig["user"],
		Password: dbConfig["password"],
		Database: dbConfig["database"],
		SSLMode:  dbConfig["sslmode"],
		Path:     dbConfig["path"],
	}
	database, err := db.NewDatabase(dbCfg)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize database: %v", err)
	}

	slog.Info("Database initialized successfully", "type", dbCfg.Type)

	// Initialize Twitch API client (shared across all connections)
	clientID := env["CLIENT_ID"]
	clientSecret := env["CLIENT_SECRET"]
	if clientID == "" || clientSecret == "" {
		cancel()
		return nil, fmt.Errorf("CLIENT_ID and CLIENT_SECRET must be set")
	}
	twitchClient := twitch.NewClientWithDB(clientID, clientSecret, database)
	if err := twitchClient.GetOAuth(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to get OAuth token: %v", err)
	}

	slog.Info("Twitch API authenticated successfully")

	pool := &ConnectionPool{
		connections:           make([]*IRCConnection, 0),
		channelsPerConnection: channelsPerConnection,
		totalChannels:         totalChannels,
		sharedDB:              database,
		twitchClient:          twitchClient,
		ctx:                   ctx,
		cancel:                cancel,
		centralMessageChan:    make(chan *ChatMessage, 100000), // Large centralized buffer
	}

	// Start centralized database writer
	pool.startCentralizedDBWriter()

	return pool, nil
}

// startCentralizedDBWriter starts a single DB writer that handles all messages from all connections
func (pool *ConnectionPool) startCentralizedDBWriter() {
	pool.dbWriterWg.Add(1)

	go func() {
		defer pool.dbWriterWg.Done()

		ticker := time.NewTicker(1 * time.Second) // Batch every second
		defer ticker.Stop()

		var batch []*ChatMessage
		const maxBatchSize = 500 // Larger batch size for better throughput

		messagesDropped := 0

		for {
			select {
			case <-pool.ctx.Done():
				// Process remaining messages before exiting
				if len(batch) > 0 {
					if err := pool.saveChatMessageBatch(batch); err != nil {
						slog.Error("DB Writer: Error saving final batch", "error", err)
					}
				}
				if messagesDropped > 0 {
					slog.Warn("DB Writer: Total messages dropped", "count", messagesDropped)
				}
				return

			case msg := <-pool.centralMessageChan:
				batch = append(batch, msg)
				if len(batch) >= maxBatchSize {
					startTime := time.Now()
					err := pool.saveChatMessageBatch(batch)
					duration := time.Since(startTime).Seconds()

					if err != nil {
						slog.Error("DB Writer: Error saving batch", "time", time.Now().Format("15:04:05"), "error", err)
						metrics.RecordDBBatchInsert(duration, len(batch), false)
					} else {
						metrics.RecordDBBatchInsert(duration, len(batch), true)

						// Log slow batch inserts that might indicate burst pressure
						if duration > 2.0 {
							slog.Warn("DB Writer: Slow batch insert", "time", time.Now().Format("15:04:05"), "duration", duration, "messages", len(batch))
						}
					}
					batch = batch[:0] // Clear batch
				}

			case <-ticker.C:
				if len(batch) > 0 {
					startTime := time.Now()
					err := pool.saveChatMessageBatch(batch)
					duration := time.Since(startTime).Seconds()

					if err != nil {
						slog.Error("DB Writer: Error saving batch", "error", err)
						metrics.RecordDBBatchInsert(duration, len(batch), false)
					} else {
						metrics.RecordDBBatchInsert(duration, len(batch), true)
					}
					batch = batch[:0] // Clear batch
				}

				// Report if central channel is getting full
				channelLen := len(pool.centralMessageChan)
				channelCap := cap(pool.centralMessageChan)
				percentFull := (channelLen * 100) / channelCap

				// Update metrics
				metrics.UpdateMessageBufferMetrics(channelLen, channelCap)

				if percentFull >= 80 {
					slog.Error("DB Writer: CRITICAL: Message buffer full", "percent", percentFull, "len", channelLen, "cap", channelCap)
				} else if percentFull >= 50 {
					slog.Warn("DB Writer: WARNING: Message buffer full", "percent", percentFull, "len", channelLen, "cap", channelCap)
				} else if percentFull >= 25 {
					slog.Info("DB Writer: Message buffer full", "percent", percentFull, "len", channelLen, "cap", channelCap)
				}
			}
		}
	}()
}

// saveChatMessageBatch converts app.ChatMessage to db.ChatMessage and saves them
func (pool *ConnectionPool) saveChatMessageBatch(messages []*ChatMessage) error {
	// Convert app.ChatMessage to db.ChatMessage
	dbMessages := make([]*db.ChatMessage, len(messages))
	for i, msg := range messages {
		dbMessages[i] = &db.ChatMessage{
			MessageType:      msg.MessageType,
			Timestamp:        msg.Timestamp,
			Channel:          msg.Channel,
			Nickname:         msg.Nickname,
			Message:          msg.Message,
			UserID:           msg.UserID,
			DisplayName:      msg.DisplayName,
			Color:            msg.Color,
			Badges:           msg.Badges,
			SubPlan:          msg.SubPlan,
			SubPlanName:      msg.SubPlanName,
			Months:           msg.Months,
			CumulativeMonths: msg.CumulativeMonths,
			StreakMonths:     msg.StreakMonths,
			IsGift:           msg.IsGift,
			GifterName:       msg.GifterName,
			GifterID:         msg.GifterID,
			BanDuration:      msg.BanDuration,
			BanReason:        msg.BanReason,
			TargetUser:       msg.TargetUser,
			TargetMessageID:  msg.TargetMessageID,
			RaiderName:       msg.RaiderName,
			ViewerCount:      msg.ViewerCount,
			BitsAmount:       msg.BitsAmount,
			NoticeMessageID:  msg.NoticeMessageID,
			SystemMessage:    msg.SystemMessage,
			RawMessage:       msg.RawMessage,
		}
	}

	return pool.sharedDB.SaveChatMessageBatch(dbMessages)
}

// Start initializes and starts all connections in the pool
func (pool *ConnectionPool) Start() error {
	// Calculate number of connections needed
	numConnections := (pool.totalChannels + pool.channelsPerConnection - 1) / pool.channelsPerConnection

	if numConnections == 0 {
		numConnections = 1
	}

	slog.Info("Starting connection pool", "connections", numConnections, "channels_per_connection", pool.channelsPerConnection)

	// Discover all channels first
	slog.Info("Discovering channels")
	allChannels, err := pool.discoverAllChannels(5) // Only get channels with 5+ viewers
	if err != nil {
		return fmt.Errorf("failed to discover channels: %v", err)
	}

	if len(allChannels) == 0 {
		return fmt.Errorf("no channels discovered")
	}

	slog.Info("Discovered channels, shuffling for balanced load", "count", len(allChannels))

	// Shuffle channels to distribute high-traffic and low-traffic channels evenly across connections
	// This prevents Connection 0 from getting all the popular channels
	rand.Shuffle(len(allChannels), func(i, j int) {
		allChannels[i], allChannels[j] = allChannels[j], allChannels[i]
	})

	slog.Info("Channels shuffled - load will be distributed evenly across connections")

	// Recalculate channels per connection based on actual discovered channels
	actualChannelsPerConnection := len(allChannels) / numConnections
	if actualChannelsPerConnection == 0 {
		actualChannelsPerConnection = 1
	}

	slog.Info("Distributing channels across connections", "total_channels", len(allChannels), "connections", numConnections, "channels_per_connection", actualChannelsPerConnection)

	// Create and start each connection with proper spacing
	connectionDelay := 2 * time.Second // Wait 2 seconds between each connection

	slog.Info("Starting connections", "delay", connectionDelay)

	for i := 0; i < numConnections; i++ {
		// Calculate channel slice for this connection based on actual channels
		startIdx := i * actualChannelsPerConnection
		endIdx := startIdx + actualChannelsPerConnection

		// Last connection gets all remaining channels
		if i == numConnections-1 {
			endIdx = len(allChannels)
		}

		if endIdx > len(allChannels) {
			endIdx = len(allChannels)
		}

		if startIdx >= len(allChannels) {
			break // No more channels to assign
		}

		connectionChannels := allChannels[startIdx:endIdx]

		slog.Info("Assigned channels to connection", "connection_id", i, "start", startIdx, "end", endIdx-1, "total", len(connectionChannels))

		// Create IRC connection instance for this pool connection
		conn := pool.createIRCConnection(i, connectionChannels)

		pool.mu.Lock()
		pool.connections = append(pool.connections, conn)
		pool.mu.Unlock()

		slog.Info("Starting connection", "current", i+1, "total", numConnections)

		// Start this connection in a goroutine
		go pool.runConnection(i, conn, connectionChannels)

		// Wait before starting next connection (except for last one)
		if i < numConnections-1 {
			slog.Info("Waiting before starting next connection", "delay", connectionDelay)
			time.Sleep(connectionDelay)
		}
	}

	// Start aggregated statistics reporting
	go pool.reportAggregatedStats()

	// Start periodic channel discovery and redistribution
	go pool.periodicChannelRediscovery()

	slog.Info("Connection pool started", "active_connections", len(pool.connections))

	return nil
}

// createIRCConnection creates a new conn instance for a specific connection
func (pool *ConnectionPool) createIRCConnection(connectionID int, channels []string) *IRCConnection {
	conn := NewIRCConnection(pool.sharedDB, pool.twitchClient, pool.centralMessageChan)
	conn.ID = fmt.Sprintf("conn-%d", connectionID)

	// Pre-assign channels to this IRC connection
	conn.Channels = channels

	return conn
}

// runConnection runs a single connection
func (pool *ConnectionPool) runConnection(connectionID int, conn *IRCConnection, channels []string) {
	defer func() {
		if r := recover(); r != nil {
			slog.Info("Panic recovered", "connection_id", connectionID, "panic", r)
		}
	}()

	slog.Info("Starting connection", "connection_id", connectionID, "channels", len(channels))

	for {
		select {
		case <-pool.ctx.Done():
			slog.Info("Connection shutting down", "connection_id", connectionID)
			return
		default:
			// Connect to IRC
			if err := conn.connect(); err != nil {
				slog.Error("Connection error, retrying", "connection_id", connectionID, "error", err)
				time.Sleep(5 * time.Second)
				continue
			}

			slog.Info("Connection connected successfully", "connection_id", connectionID)

			// Start reading chat in a separate goroutine
			chatDone := make(chan error, 1)
			go func() {
				// infiniteReadChat runs forever and handles its own reconnections
				// But if it returns, something serious happened
				slog.Info("Starting chat reader", "connection_id", connectionID)
				conn.infiniteReadChat()
				chatDone <- fmt.Errorf("infiniteReadChat exited")
			}()

			// Give the chat reader a moment to start and authenticate
			time.Sleep(500 * time.Millisecond)

			// Add staggered delay based on connection ID to prevent all connections
			// from joining channels simultaneously (which causes message burst)
			staggerDelay := time.Duration(connectionID*10) * time.Second
			if staggerDelay > 0 {
				slog.Info("Waiting before joining channels (staggered start)", "connection_id", connectionID, "delay", staggerDelay)

				// Use select to allow cancellation during wait
				select {
				case <-time.After(staggerDelay):
					// Continue to channel joining
				case <-pool.ctx.Done():
					slog.Info("Cancelled during stagger delay", "connection_id", connectionID)
					return
				}
			}

			// Join assigned channels
			slog.Info("Joining channels", "connection_id", connectionID, "count", len(channels))
			go conn.joinNewChannels(channels)
			slog.Info("Finished joining channels", "connection_id", connectionID)

			// Wait for either context cancellation or chat reader error
			select {
			case <-pool.ctx.Done():
				slog.Info("Connection shutting down", "connection_id", connectionID)
				return
			case err := <-chatDone:
				slog.Error("Chat reader exited, reconnecting", "connection_id", connectionID, "error", err)
				// Close connection and loop to reconnect
				if conn.Connection != nil {
					_ = conn.Connection.Close() // Explicitly ignore error in reconnection cleanup
				}
				conn.IsConnected = false
				time.Sleep(2 * time.Second)
				continue
			}
		}
	}
} // discoverAllChannels fetches all channels from Twitch API
func (pool *ConnectionPool) discoverAllChannels(minViewer int) ([]string, error) {
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime).Seconds()
		metrics.ChannelDiscoveryDuration.Observe(duration)
	}()

	var channels []string
	cursor := ""
	pageSize := 100

	for len(channels) < pool.totalChannels {
		slog.Info("Fetching channels", "count_so_far", len(channels))
		streamsResp, err := pool.twitchClient.GetStreams(pageSize, cursor)
		if err != nil {
			return nil, fmt.Errorf("failed to get streams: %v", err)
		}

		if len(streamsResp.Data) == 0 {
			slog.Info("No more live streams available")
			break
		}

		for _, stream := range streamsResp.Data {
			// Stop if we hit channels with fewer than 5 viewers (streams are sorted by viewer count)
			if stream.ViewerCount < minViewer {
				slog.Info("Reached channels with < 5 viewers, stopping discovery", "viewers", stream.ViewerCount)
				slog.Info("Final channel count", "count", len(channels))
				return channels, nil
			}

			channelName := "#" + stream.UserLogin
			channels = append(channels, channelName)

			if len(channels) >= pool.totalChannels {
				break
			}
		}

		if streamsResp.Pagination.Cursor == "" {
			slog.Info("Reached end of available streams")
			break
		}

		cursor = streamsResp.Pagination.Cursor
		time.Sleep(10 * time.Millisecond)
	}

	metrics.ChannelsDiscovered.Set(float64(len(channels)))
	return channels, nil
}

// reportAggregatedStats reports combined statistics from all connections
func (pool *ConnectionPool) reportAggregatedStats() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	lastTotalMessages := 0

	for {
		select {
		case <-pool.ctx.Done():
			return
		case <-ticker.C:
			pool.mu.RLock()

			totalMessages := 0
			totalChannels := 0
			activeConnections := 0

			for i, conn := range pool.connections {
				if conn.IsConnected {
					activeConnections++
				}
				totalMessages += conn.NMessages
				totalChannels += len(conn.connectedChannels)

				// Update per-connection metrics
				connID := fmt.Sprintf("conn-%d", i)
				metrics.JoinedChannelsPerConnection.WithLabelValues(connID).Set(float64(len(conn.connectedChannels)))
				metrics.UpdateConnectionStatus(connID, conn.IsConnected)

				// Individual connection stats (less frequent)
				if totalMessages%10 == 0 {
					slog.Info("Connection stats", "conn_id", i, "messages", conn.NMessages, "channels", len(conn.connectedChannels), "connected", conn.IsConnected)
				}
			}

			messagesPerSecond := totalMessages - lastTotalMessages
			lastTotalMessages = totalMessages

			// Detect and log message burst spikes
			if messagesPerSecond > 3000 {
				slog.Error("MESSAGE BURST DETECTED", "time", time.Now().Format("15:04:05"), "messages_per_sec", messagesPerSecond)
				slog.Info("Possible causes: Channel JOIN backlog, Rediscovery joining channels simultaneously, High traffic event")
			} else if messagesPerSecond > 1500 {
				slog.Warn("High message rate detected", "time", time.Now().Format("15:04:05"), "messages_per_sec", messagesPerSecond)
			}

			// Update global metrics
			metrics.MessagesPerSecond.Set(float64(messagesPerSecond))
			metrics.JoinedChannelsTotal.Set(float64(totalChannels))
			metrics.ActiveConnections.Set(float64(activeConnections))
			metrics.ConnectionsTotal.Set(float64(len(pool.connections)))

			slog.Info("Pool stats", "time", time.Now().Format("15:04:05"), "active_connections", activeConnections, "total_connections", len(pool.connections), "messages_per_sec", messagesPerSecond, "total_channels", totalChannels, "total_messages", totalMessages)

			pool.mu.RUnlock()
		}
	}
}

// periodicChannelRediscovery periodically rediscovers channels and redistributes them across connections
func (pool *ConnectionPool) periodicChannelRediscovery() {
	// Wait 10 minutes before first rediscovery to let initial connections stabilize
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	slog.Info("Periodic channel rediscovery enabled", "interval", "10 minutes")

	for {
		select {
		case <-pool.ctx.Done():
			return
		case <-ticker.C:
			slog.Info("Starting periodic channel rediscovery")

			// Discover current live channels
			newChannels, err := pool.discoverAllChannels(5) // Only get channels with 5+ viewers
			if err != nil {
				slog.Error("Error rediscovering channels", "error", err)
				continue
			}

			if len(newChannels) == 0 {
				slog.Warn("No channels discovered during periodic scan")
				continue
			}

			slog.Info("Rediscovered channels", "count", len(newChannels))

			// Shuffle for even distribution
			rand.Shuffle(len(newChannels), func(i, j int) {
				newChannels[i], newChannels[j] = newChannels[j], newChannels[i]
			})

			// Get currently connected channels across all connections
			pool.mu.RLock()
			currentChannels := make(map[string]bool)
			for _, conn := range pool.connections {
				conn.channelsMutex.RLock()
				for channel := range conn.connectedChannels {
					currentChannels[channel] = true
				}
				conn.channelsMutex.RUnlock()
			}
			numConnections := len(pool.connections)
			pool.mu.RUnlock()

			// Determine new channels to join and old channels to part
			newChannelSet := make(map[string]bool)
			for _, ch := range newChannels {
				newChannelSet[ch] = true
			}

			channelsToJoin := []string{}
			for _, ch := range newChannels {
				if !currentChannels[ch] {
					channelsToJoin = append(channelsToJoin, ch)
				}
			}

			offlineChannels := 0
			for ch := range currentChannels {
				if !newChannelSet[ch] {
					offlineChannels++
				}
			}

			slog.Info("Channel delta", "new_channels", len(channelsToJoin), "offline_channels", offlineChannels)

			// Note: We do NOT part from offline channels - they stay joined in case they come back online

			// Distribute new channels across connections WITH STAGGERING
			// This prevents message burst spikes by spacing out the JOINs
			if len(channelsToJoin) > 0 {
				channelsPerConn := len(channelsToJoin) / numConnections
				if channelsPerConn == 0 {
					channelsPerConn = 1
				}

				slog.Info("Joining new channels across connections with staggering", "new_channels", len(channelsToJoin), "connections", numConnections)

				pool.mu.RLock()
				for i, conn := range pool.connections {
					startIdx := i * channelsPerConn
					endIdx := startIdx + channelsPerConn

					if i == numConnections-1 {
						// Last connection gets remaining channels
						endIdx = len(channelsToJoin)
					}

					if startIdx >= len(channelsToJoin) {
						break
					}

					if endIdx > len(channelsToJoin) {
						endIdx = len(channelsToJoin)
					}

					connChannels := channelsToJoin[startIdx:endIdx]
					if len(connChannels) > 0 {
						// Stagger channel joining across connections to prevent message burst
						staggerDelay := time.Duration(i*15) * time.Second
						slog.Info("Assigning new channels", "id", conn.ID, "new_channels", len(connChannels), "stagger_delay", staggerDelay)

						// Capture variables for goroutine
						connection := conn
						channels := connChannels

						go func() {
							if staggerDelay > 0 {
								slog.Info("REDISCOVERY: Waiting before joining new channels", "id", connection.ID, "delay", staggerDelay)
								time.Sleep(staggerDelay)
							}

							joinStart := time.Now()
							slog.Info("REDISCOVERY: Starting to join new channels", "id", connection.ID, "channels", len(channels), "time", time.Now().Format("15:04:05"))

							connection.joinNewChannels(channels)

							joinDuration := time.Since(joinStart)
							slog.Info("REDISCOVERY: Completed joining channels", "id", connection.ID, "channels", len(channels), "duration", joinDuration.Seconds(), "time", time.Now().Format("15:04:05"))
						}()
					}
				}
				pool.mu.RUnlock()
			}

			slog.Info("Channel rediscovery and redistribution initiated (staggered joining in progress)")
		}
	}
}

// Shutdown gracefully shuts down all connections
func (pool *ConnectionPool) Shutdown() {
	slog.Info("Shutting down connection pool")

	pool.cancel() // Cancel context to stop all goroutines

	// Wait for centralized DB writer to finish
	slog.Info("Waiting for DB writer to finish")
	pool.dbWriterWg.Wait()

	time.Sleep(2 * time.Second) // Give time for other goroutines to finish

	pool.mu.Lock()
	defer pool.mu.Unlock()

	// Shutdown each connection
	for i, conn := range pool.connections {
		slog.Info("Shutting down connection", "index", i)
		conn.Shutdown()
	}

	// Close shared database
	if pool.sharedDB != nil {
		time.Sleep(100 * time.Millisecond)
		pool.sharedDB.Close()
	}

	slog.Info("Connection pool shutdown complete")
}

// GetStats returns aggregated statistics
func (pool *ConnectionPool) GetStats() map[string]interface{} {
	pool.mu.RLock()
	defer pool.mu.RUnlock()

	totalMessages := 0
	totalChannels := 0
	activeConnections := 0

	for _, conn := range pool.connections {
		if conn.IsConnected {
			activeConnections++
		}
		totalMessages += conn.NMessages
		totalChannels += len(conn.connectedChannels)
	}

	return map[string]interface{}{
		"total_connections":  len(pool.connections),
		"active_connections": activeConnections,
		"total_messages":     totalMessages,
		"total_channels":     totalChannels,
	}
}
