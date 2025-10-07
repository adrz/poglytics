package app

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"twitch-chat-scrapper/internal/config"
	"twitch-chat-scrapper/internal/db"
	"twitch-chat-scrapper/internal/metrics"
	"twitch-chat-scrapper/internal/twitch"
)

// ConnectionPool manages multiple IRC connections to distribute channel load
type ConnectionPool struct {
	connections           []*Subscriber
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
		return nil, fmt.Errorf("failed to initialize database: %v", err)
	}

	fmt.Printf("Database initialized successfully (type: %s)\n", dbCfg.Type)

	// Initialize Twitch API client (shared across all connections)
	clientID := env["CLIENT_ID"]
	clientSecret := env["CLIENT_SECRET"]

	if clientID == "" || clientSecret == "" {
		return nil, fmt.Errorf("CLIENT_ID and CLIENT_SECRET must be set")
	}

	// Initialize Twitch API client with database support (shared across all connections)
	twitchClient := twitch.NewClientWithDB(clientID, clientSecret, database)
	if err := twitchClient.GetOAuth(); err != nil {
		return nil, fmt.Errorf("failed to get OAuth token: %v", err)
	}

	fmt.Println("Twitch API authenticated successfully")

	pool := &ConnectionPool{
		connections:           make([]*Subscriber, 0),
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
						fmt.Printf("[DB Writer] Error saving final batch: %v\n", err)
					}
				}
				if messagesDropped > 0 {
					fmt.Printf("[DB Writer] Total messages dropped: %d\n", messagesDropped)
				}
				return

			case msg := <-pool.centralMessageChan:
				batch = append(batch, msg)
				if len(batch) >= maxBatchSize {
					startTime := time.Now()
					err := pool.saveChatMessageBatch(batch)
					duration := time.Since(startTime).Seconds()

					if err != nil {
						fmt.Printf("[DB Writer %s] Error saving batch: %v\n", time.Now().Format("15:04:05"), err)
						metrics.RecordDBBatchInsert(duration, len(batch), false)
					} else {
						metrics.RecordDBBatchInsert(duration, len(batch), true)

						// Log slow batch inserts that might indicate burst pressure
						if duration > 2.0 {
							fmt.Printf("[DB Writer %s] Slow batch insert: %.2fs for %d messages (possible burst overload)\n",
								time.Now().Format("15:04:05"), duration, len(batch))
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
						fmt.Printf("[DB Writer] Error saving batch: %v\n", err)
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
					fmt.Printf("[DB Writer] ⚠️  CRITICAL: Message buffer %d%% full (%d/%d) - risk of dropping messages!\n",
						percentFull, channelLen, channelCap)
				} else if percentFull >= 50 {
					fmt.Printf("[DB Writer] WARNING: Message buffer %d%% full (%d/%d)\n",
						percentFull, channelLen, channelCap)
				} else if percentFull >= 25 {
					fmt.Printf("[DB Writer] Message buffer %d%% full (%d/%d)\n",
						percentFull, channelLen, channelCap)
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

	fmt.Printf("Starting connection pool with %d connections (%d channels per connection)\n",
		numConnections, pool.channelsPerConnection)

	// Discover all channels first
	fmt.Println("Discovering channels...")
	allChannels, err := pool.discoverAllChannels()
	if err != nil {
		return fmt.Errorf("failed to discover channels: %v", err)
	}

	if len(allChannels) == 0 {
		return fmt.Errorf("no channels discovered")
	}

	fmt.Printf("Discovered %d channels, shuffling for balanced load distribution...\n", len(allChannels))

	// Shuffle channels to distribute high-traffic and low-traffic channels evenly across connections
	// This prevents Connection 0 from getting all the popular channels
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(allChannels), func(i, j int) {
		allChannels[i], allChannels[j] = allChannels[j], allChannels[i]
	})

	fmt.Println("Channels shuffled - load will be distributed evenly across connections")

	// Recalculate channels per connection based on actual discovered channels
	actualChannelsPerConnection := len(allChannels) / numConnections
	if actualChannelsPerConnection == 0 {
		actualChannelsPerConnection = 1
	}

	fmt.Printf("Distributing %d channels across %d connections (~%d channels each)\n",
		len(allChannels), numConnections, actualChannelsPerConnection)

	// Create and start each connection with proper spacing
	connectionDelay := 2 * time.Second // Wait 2 seconds between each connection

	fmt.Printf("Starting connections with %v delay between each...\n", connectionDelay)

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

		fmt.Printf("[Connection %d] Assigned channels %d to %d (total: %d channels)\n",
			i, startIdx, endIdx-1, len(connectionChannels))

		// Create subscriber for this connection
		subscriber := pool.createSubscriber(i, connectionChannels)

		pool.mu.Lock()
		pool.connections = append(pool.connections, subscriber)
		pool.mu.Unlock()

		fmt.Printf("Starting connection %d/%d...\n", i+1, numConnections)

		// Start this connection in a goroutine
		go pool.runConnection(i, subscriber, connectionChannels)

		// Wait before starting next connection (except for last one)
		if i < numConnections-1 {
			fmt.Printf("Waiting %v before starting next connection...\n", connectionDelay)
			time.Sleep(connectionDelay)
		}
	}

	// Start aggregated statistics reporting
	go pool.reportAggregatedStats()

	// Start periodic channel discovery and redistribution
	go pool.periodicChannelRediscovery()

	fmt.Printf("Connection pool started with %d active connections\n", len(pool.connections))

	return nil
}

// createSubscriber creates a new subscriber instance for a specific connection
func (pool *ConnectionPool) createSubscriber(connectionID int, channels []string) *Subscriber {
	subscriber := NewSubscriberWithSharedChannel(pool.sharedDB, pool.twitchClient, pool.centralMessageChan)
	subscriber.ID = fmt.Sprintf("conn-%d", connectionID)

	// Pre-assign channels to this subscriber
	subscriber.Channels = channels

	return subscriber
}

// runConnection runs a single connection
func (pool *ConnectionPool) runConnection(connectionID int, subscriber *Subscriber, channels []string) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Connection %d] Panic recovered: %v\n", connectionID, r)
		}
	}()

	fmt.Printf("[Connection %d] Starting with %d channels\n", connectionID, len(channels))

	for {
		select {
		case <-pool.ctx.Done():
			fmt.Printf("[Connection %d] Shutting down\n", connectionID)
			return
		default:
			// Connect to IRC
			if err := subscriber.connect(); err != nil {
				fmt.Printf("[Connection %d] Error connecting: %v, retrying in 5s...\n", connectionID, err)
				time.Sleep(5 * time.Second)
				continue
			}

			fmt.Printf("[Connection %d] Connected successfully\n", connectionID)

			// Start reading chat in a separate goroutine
			chatDone := make(chan error, 1)
			go func() {
				// infiniteReadChat runs forever and handles its own reconnections
				// But if it returns, something serious happened
				fmt.Printf("[Connection %d] Starting chat reader...\n", connectionID)
				subscriber.infiniteReadChat()
				chatDone <- fmt.Errorf("infiniteReadChat exited")
			}()

			// Give the chat reader a moment to start and authenticate
			time.Sleep(500 * time.Millisecond)

			// Add staggered delay based on connection ID to prevent all connections
			// from joining channels simultaneously (which causes message burst)
			staggerDelay := time.Duration(connectionID*10) * time.Second
			if staggerDelay > 0 {
				fmt.Printf("[Connection %d] Waiting %v before joining channels (staggered start)...\n",
					connectionID, staggerDelay)

				// Use select to allow cancellation during wait
				select {
				case <-time.After(staggerDelay):
					// Continue to channel joining
				case <-pool.ctx.Done():
					fmt.Printf("[Connection %d] Cancelled during stagger delay\n", connectionID)
					return
				}
			}

			// Join assigned channels
			fmt.Printf("[Connection %d] Joining %d channels\n", connectionID, len(channels))
			go subscriber.joinNewChannels(channels)
			fmt.Printf("[Connection %d] Finished joining channels\n", connectionID)

			// Wait for either context cancellation or chat reader error
			select {
			case <-pool.ctx.Done():
				fmt.Printf("[Connection %d] Shutting down\n", connectionID)
				return
			case err := <-chatDone:
				fmt.Printf("[Connection %d] Chat reader exited: %v, reconnecting...\n", connectionID, err)
				// Close connection and loop to reconnect
				if subscriber.Connection != nil {
					subscriber.Connection.Close()
				}
				subscriber.IsConnected = false
				time.Sleep(2 * time.Second)
				continue
			}
		}
	}
}

// discoverAllChannels fetches all channels from Twitch API
func (pool *ConnectionPool) discoverAllChannels() ([]string, error) {
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime).Seconds()
		metrics.ChannelDiscoveryDuration.Observe(duration)
	}()

	var channels []string
	cursor := ""
	pageSize := 100

	for len(channels) < pool.totalChannels {
		fmt.Printf("Fetching channels - got %d so far...\n", len(channels))

		streamsResp, err := pool.twitchClient.GetStreams(pageSize, cursor)
		if err != nil {
			return nil, fmt.Errorf("failed to get streams: %v", err)
		}

		if len(streamsResp.Data) == 0 {
			fmt.Println("No more live streams available")
			break
		}

		for _, stream := range streamsResp.Data {
			// Stop if we hit channels with fewer than 5 viewers (streams are sorted by viewer count)
			if stream.ViewerCount < 5 {
				fmt.Printf("[Pool] Reached channels with < 5 viewers (current: %d viewers), stopping discovery\n", stream.ViewerCount)
				fmt.Printf("[Pool] Final channel count: %d channels (all with 5+ viewers)\n", len(channels))
				return channels, nil
			}

			channelName := "#" + stream.UserLogin
			channels = append(channels, channelName)

			if len(channels) >= pool.totalChannels {
				break
			}
		}

		if streamsResp.Pagination.Cursor == "" {
			fmt.Println("Reached end of available streams")
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
					fmt.Printf("[Conn %d] Messages: %d | Channels: %d | Connected: %v\n",
						i, conn.NMessages, len(conn.connectedChannels), conn.IsConnected)
				}
			}

			messagesPerSecond := totalMessages - lastTotalMessages
			lastTotalMessages = totalMessages

			// Detect and log message burst spikes
			if messagesPerSecond > 3000 {
				fmt.Printf("[POOL STATS %s] MESSAGE BURST DETECTED! Messages/sec: %d (threshold: 3000)\n",
					time.Now().Format("15:04:05"), messagesPerSecond)
				fmt.Printf("Possible causes: 1) Channel JOIN backlog, 2) Rediscovery joining channels simultaneously, 3) High traffic event\n")
			} else if messagesPerSecond > 1500 {
				fmt.Printf("[POOL STATS %s] High message rate detected: %d msg/sec (watch for buffer overflow)\n",
					time.Now().Format("15:04:05"), messagesPerSecond)
			}

			// Update global metrics
			metrics.MessagesPerSecond.Set(float64(messagesPerSecond))
			metrics.JoinedChannelsTotal.Set(float64(totalChannels))
			metrics.ActiveConnections.Set(float64(activeConnections))
			metrics.ConnectionsTotal.Set(float64(len(pool.connections)))

			fmt.Printf("[POOL STATS %s] Connections: %d/%d active | Messages/sec: %d | Total channels: %d | Total messages: %d\n",
				time.Now().Format("15:04:05"), activeConnections, len(pool.connections), messagesPerSecond, totalChannels, totalMessages)

			pool.mu.RUnlock()
		}
	}
}

// periodicChannelRediscovery periodically rediscovers channels and redistributes them across connections
func (pool *ConnectionPool) periodicChannelRediscovery() {
	// Wait 10 minutes before first rediscovery to let initial connections stabilize
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	fmt.Println("[Pool] Periodic channel rediscovery enabled (every 10 minutes)")

	for {
		select {
		case <-pool.ctx.Done():
			return
		case <-ticker.C:
			fmt.Println("[Pool] Starting periodic channel rediscovery...")

			// Discover current live channels
			newChannels, err := pool.discoverAllChannels()
			if err != nil {
				fmt.Printf("[Pool] Error rediscovering channels: %v\n", err)
				continue
			}

			if len(newChannels) == 0 {
				fmt.Println("[Pool] No channels discovered during periodic scan")
				continue
			}

			fmt.Printf("[Pool] Rediscovered %d channels\n", len(newChannels))

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

			fmt.Printf("[Pool] Channel delta: +%d new channels to join, %d channels currently offline (keeping them)\n",
				len(channelsToJoin), offlineChannels)

			// Note: We do NOT part from offline channels - they stay joined in case they come back online

			// Distribute new channels across connections WITH STAGGERING
			// This prevents message burst spikes by spacing out the JOINs
			if len(channelsToJoin) > 0 {
				channelsPerConn := len(channelsToJoin) / numConnections
				if channelsPerConn == 0 {
					channelsPerConn = 1
				}

				fmt.Printf("[Pool] Joining %d new channels across %d connections with staggering to prevent message burst\n",
					len(channelsToJoin), numConnections)

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
						fmt.Printf("[%s] Assigning %d new channels (will join after %v stagger delay)\n",
							conn.ID, len(connChannels), staggerDelay)

						// Capture variables for goroutine
						connection := conn
						channels := connChannels

						go func() {
							if staggerDelay > 0 {
								fmt.Printf("[%s] [REDISCOVERY] Waiting %v before joining new channels to prevent message burst...\n",
									connection.ID, staggerDelay)
								time.Sleep(staggerDelay)
							}

							joinStart := time.Now()
							fmt.Printf("[%s] [REDISCOVERY] Starting to join %d new channels at %s\n",
								connection.ID, len(channels), time.Now().Format("15:04:05"))

							connection.joinNewChannels(channels)

							joinDuration := time.Since(joinStart)
							fmt.Printf("[%s] [REDISCOVERY] Completed joining %d channels in %.1fs at %s\n",
								connection.ID, len(channels), joinDuration.Seconds(), time.Now().Format("15:04:05"))
						}()
					}
				}
				pool.mu.RUnlock()
			}

			fmt.Println("[Pool] Channel rediscovery and redistribution initiated (staggered joining in progress)")
		}
	}
}

// Shutdown gracefully shuts down all connections
func (pool *ConnectionPool) Shutdown() {
	fmt.Println("Shutting down connection pool...")

	pool.cancel() // Cancel context to stop all goroutines

	// Wait for centralized DB writer to finish
	fmt.Println("Waiting for DB writer to finish...")
	pool.dbWriterWg.Wait()

	time.Sleep(2 * time.Second) // Give time for other goroutines to finish

	pool.mu.Lock()
	defer pool.mu.Unlock()

	// Shutdown each connection
	for i, conn := range pool.connections {
		fmt.Printf("Shutting down connection %d...\n", i)
		conn.Shutdown()
	}

	// Close shared database
	if pool.sharedDB != nil {
		time.Sleep(100 * time.Millisecond)
		pool.sharedDB.Close()
	}

	fmt.Println("Connection pool shutdown complete")
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
