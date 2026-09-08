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
	workerWg           sync.WaitGroup
	shutdownOnce       sync.Once
	started            bool
	stopping           bool
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(totalChannels, channelsPerConnection int) (*ConnectionPool, error) {
	if totalChannels <= 0 || channelsPerConnection <= 0 {
		return nil, fmt.Errorf("channel counts must be positive")
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
		database.Close()
		cancel()
		return nil, fmt.Errorf("CLIENT_ID and CLIENT_SECRET must be set")
	}
	twitchClient := twitch.NewClientWithDB(clientID, clientSecret, database)
	if err := twitchClient.GetOAuth(); err != nil {
		database.Close()
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
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		const maxBatchSize = 500
		batch := make([]*ChatMessage, 0, maxBatchSize)
		flush := func() {
			if len(batch) == 0 {
				return
			}
			start := time.Now()
			err := pool.saveChatMessageBatch(batch)
			metrics.RecordDBBatchInsert(time.Since(start).Seconds(), len(batch), err == nil)
			if err != nil {
				// Backends may already have committed part of the batch. Retrying
				// it here would duplicate those rows without idempotency support.
				slog.Error("DB batch could not be fully saved", "messages", len(batch), "error", err)
			}
			clear(batch)
			batch = batch[:0]
		}
		for {
			select {
			case msg, ok := <-pool.centralMessageChan:
				if !ok {
					flush()
					metrics.UpdateMessageBufferMetrics(0, cap(pool.centralMessageChan))
					return
				}
				if msg == nil {
					continue
				}
				batch = append(batch, msg)
				if len(batch) == maxBatchSize {
					flush()
				}
			case <-ticker.C:
				flush()
				metrics.UpdateMessageBufferMetrics(len(pool.centralMessageChan), cap(pool.centralMessageChan))
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
			Tags:             msg.Tags,
		}
	}

	return pool.sharedDB.SaveChatMessageBatch(dbMessages)
}

// Start initializes and starts all connections in the pool
func (pool *ConnectionPool) Start() error {
	pool.mu.Lock()
	if pool.started || pool.stopping {
		pool.mu.Unlock()
		return fmt.Errorf("connection pool already started or stopped")
	}
	pool.started = true
	// Track startup too: shutdown must wait for discovery and worker creation.
	pool.workerWg.Add(1)
	pool.mu.Unlock()
	defer pool.workerWg.Done()
	// Calculate number of connections needed
	numConnections := 1 + (pool.totalChannels-1)/pool.channelsPerConnection

	if numConnections == 0 {
		numConnections = 1
	}

	slog.Info("Starting connection pool", "connections", numConnections, "channels_per_connection", pool.channelsPerConnection)

	// Discover all channels first
	slog.Info("Discovering channels")
	allChannels, err := pool.discoverAllChannels(3) // Only get channels with 5+ viewers
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
	actualChannelsPerConnection := (len(allChannels) + numConnections - 1) / numConnections
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
		if pool.stopping {
			pool.mu.Unlock()
			conn.Shutdown()
			return pool.ctx.Err()
		}
		pool.connections = append(pool.connections, conn)
		pool.workerWg.Add(1)
		pool.mu.Unlock()

		slog.Info("Starting connection", "current", i+1, "total", numConnections)

		// Start this connection in a goroutine
		go func() {
			defer pool.workerWg.Done()
			pool.runConnection(i, conn, connectionChannels)
		}()

		// Wait before starting next connection (except for last one)
		if i < numConnections-1 {
			slog.Info("Waiting before starting next connection", "delay", connectionDelay)
			if !waitContext(pool.ctx, connectionDelay) {
				return pool.ctx.Err()
			}
		}
	}

	// Start aggregated statistics reporting
	pool.workerWg.Add(2)
	go func() {
		defer pool.workerWg.Done()
		pool.reportAggregatedStats()
	}()

	// Start periodic channel discovery and redistribution
	go func() {
		defer pool.workerWg.Done()
		pool.periodicChannelRediscovery()
	}()

	slog.Info("Connection pool started", "active_connections", len(pool.connections))

	return nil
}

// createIRCConnection creates a new conn instance for a specific connection
func (pool *ConnectionPool) createIRCConnection(connectionID int, channels []string) *IRCConnection {
	conn := NewIRCConnection(pool.sharedDB, pool.twitchClient, pool.centralMessageChan)
	conn.ID = fmt.Sprintf("conn-%d", connectionID)

	// Pre-assign channels to this IRC connection
	conn.cancel()
	conn.ctx, conn.cancel = context.WithCancel(pool.ctx)
	conn.assignChannels(channels)

	return conn
}

// runConnection runs a single connection
func (pool *ConnectionPool) runConnection(connectionID int, conn *IRCConnection, channels []string) {
	// Also handles independently constructed connections used by callers/tests.
	stopCancellation := context.AfterFunc(pool.ctx, conn.Shutdown)
	defer stopCancellation()
	defer conn.Shutdown()
	conn.assignChannels(channels)
	failures := 0
	for pool.ctx.Err() == nil && conn.ctx.Err() == nil {
		connectedAt := time.Now()
		err := conn.connect()
		if err == nil {
			sessionCtx, cancelSession := context.WithCancel(pool.ctx)
			done := make(chan error, 2)
			go func() { done <- conn.readChat() }()
			go func() { done <- conn.runChannelJoins(sessionCtx) }()
			completed := 0
			select {
			case err = <-done:
				completed = 1
			case <-pool.ctx.Done():
				err = pool.ctx.Err()
			}
			cancelSession()
			conn.closeSocket()
			// Neither reader nor JOIN writer can touch a replacement socket.
			for ; completed < 2; completed++ {
				<-done
			}
		}
		conn.closeSocket()
		metrics.UpdateConnectionStatus(conn.ID, false)
		if pool.ctx.Err() != nil || conn.ctx.Err() != nil {
			return
		}
		if time.Since(connectedAt) > time.Minute {
			failures = 0
		}
		failures++
		metrics.RecordReconnect(conn.ID)
		backoff := reconnectBackoff(failures)
		slog.Warn("IRC connection failed; retrying", "connection_id", connectionID, "error", err, "backoff", backoff)
		if !waitContext(conn.ctx, backoff) {
			return
		}
	}
}

// reconnectBackoff returns an exponential backoff with jitter for the given
// number of consecutive failures. Spacing retries out (instead of the old flat
// 2s) is what stops a single disconnect from cascading into a reconnect storm,
// and the jitter de-synchronises many connections that drop at the same time.
func reconnectBackoff(consecutiveFailures int) time.Duration {
	const base = 2 * time.Second
	const maxBackoff = 2 * time.Minute

	d := base
	for i := 1; i < consecutiveFailures && d < maxBackoff; i++ {
		d *= 2
	}
	if d > maxBackoff {
		d = maxBackoff
	}

	// +/- 25% jitter.
	jitter := time.Duration(rand.Int63n(int64(d)/2)) - d/4
	return d + jitter
}

// discoverAllChannels fetches all channels from Twitch API
func (pool *ConnectionPool) discoverAllChannels(minViewer int) ([]string, error) {
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime).Seconds()
		metrics.ChannelDiscoveryDuration.Observe(duration)
	}()

	var channels []string
	seen := make(map[string]bool)
	seenCursors := make(map[string]bool)
	cursor := ""
	pageSize := 100

	for len(channels) < pool.totalChannels {
		slog.Info("Fetching channels", "count_so_far", len(channels))
		streamsResp, err := pool.twitchClient.GetStreamsContext(pool.ctx, pageSize, cursor)
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
			if stream.UserLogin != "" && !seen[channelName] {
				channels = append(channels, channelName)
				seen[channelName] = true
			}

			if len(channels) >= pool.totalChannels {
				break
			}
		}

		if streamsResp.Pagination.Cursor == "" {
			slog.Info("Reached end of available streams")
			break
		}

		cursor = streamsResp.Pagination.Cursor
		if seenCursors[cursor] {
			return nil, fmt.Errorf("Twitch repeated a pagination cursor")
		}
		seenCursors[cursor] = true
		if !waitContext(pool.ctx, 10*time.Millisecond) {
			return nil, pool.ctx.Err()
		}
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
				connected := conn.IsConnected.Load()
				if connected {
					activeConnections++
				}
				nMessages := int(conn.NMessages.Load())
				totalMessages += nMessages

				conn.channelsMutex.RLock()
				nChannels := len(conn.connectedChannels)
				conn.channelsMutex.RUnlock()
				totalChannels += nChannels

				// Update per-connection metrics
				connID := fmt.Sprintf("conn-%d", i)
				metrics.JoinedChannelsPerConnection.WithLabelValues(connID).Set(float64(nChannels))
				metrics.UpdateConnectionStatus(connID, connected)

				// Individual connection stats (less frequent)
				if totalMessages%10 == 0 {
					slog.Info("Connection stats", "conn_id", i, "messages", nMessages, "channels", nChannels, "connected", connected)
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
			newChannels, err := pool.discoverAllChannels(3) // Only get channels with 5+ viewers
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

			pool.assignDiscoveredChannels(newChannels)

			slog.Info("Channel rediscovery and redistribution initiated (staggered joining in progress)")
		}
	}
}

// Shutdown gracefully shuts down all connections
func (pool *ConnectionPool) Shutdown() {
	pool.shutdownOnce.Do(func() {
		pool.mu.Lock()
		pool.stopping = true
		pool.cancel()
		connections := append([]*IRCConnection(nil), pool.connections...)
		pool.mu.Unlock()
		for _, conn := range connections {
			conn.Shutdown()
		}
		pool.workerWg.Wait()
		if pool.centralMessageChan != nil {
			close(pool.centralMessageChan)
		}
		pool.dbWriterWg.Wait()
		if pool.sharedDB != nil {
			pool.sharedDB.Close()
		}
		slog.Info("Connection pool shutdown complete")
	})
}

// Assign using desired membership, even when a socket is reconnecting. This
// prevents duplicates across connections and keeps configured capacity bounded.
func (pool *ConnectionPool) assignDiscoveredChannels(channels []string) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.stopping {
		return
	}
	assigned := make(map[string]bool)
	loads := make([]int, len(pool.connections))
	for i, conn := range pool.connections {
		conn.channelsMutex.RLock()
		loads[i] = len(conn.Channels)
		for _, channel := range conn.Channels {
			assigned[channel] = true
		}
		conn.channelsMutex.RUnlock()
	}
	skipped := 0
	for _, channel := range channels {
		if assigned[channel] {
			continue
		}
		best := -1
		for i, load := range loads {
			if load < pool.channelsPerConnection && (best == -1 || load < loads[best]) {
				best = i
			}
		}
		if best == -1 || len(assigned) >= pool.totalChannels {
			skipped++
			continue
		}
		pool.connections[best].assignChannels([]string{channel})
		loads[best]++
		assigned[channel] = true
	}
	if skipped > 0 {
		slog.Warn("Channel capacity reached; retaining existing assignments", "skipped", skipped)
	}
}

// GetStats returns aggregated statistics
func (pool *ConnectionPool) GetStats() map[string]interface{} {
	pool.mu.RLock()
	defer pool.mu.RUnlock()

	totalMessages := 0
	totalChannels := 0
	activeConnections := 0

	for _, conn := range pool.connections {
		if conn.IsConnected.Load() {
			activeConnections++
		}
		totalMessages += int(conn.NMessages.Load())

		conn.channelsMutex.RLock()
		totalChannels += len(conn.connectedChannels)
		conn.channelsMutex.RUnlock()
	}

	return map[string]interface{}{
		"total_connections":  len(pool.connections),
		"active_connections": activeConnections,
		"total_messages":     totalMessages,
		"total_channels":     totalChannels,
	}
}
