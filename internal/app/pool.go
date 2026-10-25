package app

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"poglytics-scraper/internal/config"
	"poglytics-scraper/internal/db"
	"poglytics-scraper/internal/metrics"
	"poglytics-scraper/internal/twitch"
)

// connectionStartDelay spaces out new sockets so Twitch never sees a burst of
// connections, both when the pool is built and when it grows later.
const connectionStartDelay = 2 * time.Second

// ConnectionPool manages multiple IRC connections to distribute channel load
type ConnectionPool struct {
	connections           []*IRCConnection
	channelsPerConnection int
	totalChannels         int
	sharedDB              DatabaseInterface
	ingestWriter          *db.IngestWriter
	twitchClient          *twitch.Client
	joinLimiter           *joinLimiter
	liveChannels          atomic.Pointer[map[string]bool]
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
func NewConnectionPool(totalChannels, channelsPerConnection, joinsPerSecond int) (*ConnectionPool, error) {
	// totalChannels == 0 means "no limit": monitor every channel discovery returns.
	if totalChannels < 0 || channelsPerConnection <= 0 || joinsPerSecond <= 0 {
		return nil, fmt.Errorf("channel limit must not be negative; channels per connection and joins per second must be positive")
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Initialize shared database (all connections will use the same DB)
	env, err := config.LoadEnv()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to load environment: %v", err)
	}

	batchOptions, err := config.GetBatchOptions(env)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("invalid batching configuration: %w", err)
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
	ingestWriter, err := db.NewIngestWriter(database, batchOptions, observeDBFlush)
	if err != nil {
		database.Close()
		cancel()
		return nil, err
	}
	slog.Info("Database batching configured", "legacy", batchOptions.Legacy,
		"message_rows", batchOptions.MessageRows, "message_interval", batchOptions.MessageInterval,
		"event_rows", batchOptions.EventRows, "event_interval", batchOptions.EventInterval,
		"snapshot_rows", batchOptions.SnapshotRows, "snapshot_interval", batchOptions.SnapshotInterval,
		"max_buffer_bytes", batchOptions.MaxBytes)

	// Initialize Twitch API client (shared across all connections)
	clientID := env["CLIENT_ID"]
	clientSecret := env["CLIENT_SECRET"]
	if clientID == "" || clientSecret == "" {
		database.Close()
		cancel()
		return nil, fmt.Errorf("CLIENT_ID and CLIENT_SECRET must be set")
	}
	twitchClient := twitch.NewClientWithDB(clientID, clientSecret, ingestWriter)
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
		ingestWriter:          ingestWriter,
		twitchClient:          twitchClient,
		joinLimiter:           newJoinLimiter(joinsPerSecond),
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
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		const maxBatchSize = 500
		batch := make([]*ChatMessage, 0, maxBatchSize)
		flush := func() {
			if len(batch) == 0 {
				return
			}
			// The writer reports actual persistence and errors through observeDBFlush.
			_ = pool.saveChatMessageBatch(batch)
			clear(batch)
			batch = batch[:0]
		}
		for {
			select {
			case msg, ok := <-pool.centralMessageChan:
				if !ok {
					flush()
					_ = pool.ingestWriter.Close()
					metrics.UpdateDBBufferMetrics(0, 0)
					metrics.UpdateMessageBufferMetrics(0, cap(pool.centralMessageChan))
					return
				}
				if msg == nil {
					continue
				}
				batch = append(batch, msg)
				// Drain already available messages without adding another timer delay.
				// The ingest writer owns the row and age thresholds for each table.
			drain:
				for len(batch) < maxBatchSize {
					select {
					case next, open := <-pool.centralMessageChan:
						if !open {
							break drain
						}
						if next != nil {
							batch = append(batch, next)
						}
					default:
						break drain
					}
				}
				flush()
			case <-ticker.C:
				_ = pool.ingestWriter.FlushDue()
				pending := pool.ingestWriter.Stats()
				metrics.UpdateDBBufferMetrics(pending.Rows, pending.Bytes)
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

	return pool.ingestWriter.SaveChatMessageBatch(dbMessages)
}

func observeDBFlush(s db.FlushStats) {
	metrics.RecordDBTableFlush(s.Table, s.Reason, s.Rows, s.Duration.Seconds(), s.OldestAge.Seconds(), s.Err == nil)
	if s.Err != nil {
		slog.Error("DB batch could not be fully saved", "table", s.Table, "rows", s.Rows, "error", s.Err)
	}
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

	slog.Info("Starting connection pool", "channels_per_connection", pool.channelsPerConnection, "channel_limit", pool.channelLimitLabel())

	// Discover all channels first
	slog.Info("Discovering channels")
	allChannels, err := pool.discoverAllChannels(3) // Only get channels with 5+ viewers
	if err != nil {
		return fmt.Errorf("failed to discover channels: %v", err)
	}

	if len(allChannels) == 0 {
		return fmt.Errorf("no channels discovered")
	}
	pool.setLiveChannels(allChannels)

	slog.Info("Discovered channels, shuffling for balanced load", "count", len(allChannels))

	// Shuffle channels to distribute high-traffic and low-traffic channels evenly across connections
	// This prevents Connection 0 from getting all the popular channels
	rand.Shuffle(len(allChannels), func(i, j int) {
		allChannels[i], allChannels[j] = allChannels[j], allChannels[i]
	})

	slog.Info("Channels shuffled - load will be distributed evenly across connections")

	// Size the pool from what discovery actually returned; rediscovery grows it
	// later when Twitch brings more channels online.
	numConnections := 1 + (len(allChannels)-1)/pool.channelsPerConnection

	// Spread the discovered channels evenly rather than filling each connection
	// to capacity and leaving the last one nearly empty.
	actualChannelsPerConnection := (len(allChannels) + numConnections - 1) / numConnections
	if actualChannelsPerConnection == 0 {
		actualChannelsPerConnection = 1
	}

	slog.Info("Distributing channels across connections", "total_channels", len(allChannels), "connections", numConnections, "channels_per_connection", actualChannelsPerConnection)

	// Create and start each connection with proper spacing
	slog.Info("Starting connections", "delay", connectionStartDelay)

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

		if !pool.launchConnection(connectionChannels) {
			return pool.ctx.Err()
		}

		// Wait before starting next connection (except for last one)
		if i < numConnections-1 {
			slog.Info("Waiting before starting next connection", "delay", connectionStartDelay)
			if !waitContext(pool.ctx, connectionStartDelay) {
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
	conn.joinLimiter = pool.joinLimiter
	conn.isLive = pool.isLive

	// Pre-assign channels to this IRC connection
	conn.cancel()
	conn.ctx, conn.cancel = context.WithCancel(pool.ctx)
	conn.assignChannels(channels)

	return conn
}

// launchConnection registers a connection for the given channels and starts its
// owner goroutine. It reports false when the pool is shutting down.
func (pool *ConnectionPool) launchConnection(channels []string) bool {
	pool.mu.Lock()
	if pool.stopping {
		pool.mu.Unlock()
		return false
	}
	connectionID := len(pool.connections)
	conn := pool.createIRCConnection(connectionID, channels)
	pool.connections = append(pool.connections, conn)
	pool.workerWg.Add(1)
	pool.mu.Unlock()

	slog.Info("Starting connection", "connection_id", connectionID, "channels", len(channels))

	go func() {
		defer pool.workerWg.Done()
		pool.runConnection(connectionID, conn, channels)
	}()
	return true
}

// growForChannels adds connections for channels no existing socket had room for,
// so an uncapped pool grows with Twitch instead of dropping the overflow. Starts
// are spaced exactly like the initial build.
func (pool *ConnectionPool) growForChannels(channels []string) {
	if len(channels) == 0 {
		return
	}

	slog.Info("Growing connection pool for unassigned channels",
		"channels", len(channels),
		"new_connections", 1+(len(channels)-1)/pool.channelsPerConnection)

	for start := 0; start < len(channels); start += pool.channelsPerConnection {
		end := min(start+pool.channelsPerConnection, len(channels))
		if !pool.launchConnection(channels[start:end]) {
			return
		}
		if end < len(channels) && !waitContext(pool.ctx, connectionStartDelay) {
			return
		}
	}
}

// setLiveChannels records what the latest discovery saw streaming, so rejoins
// after a reconnect restore those chats first.
func (pool *ConnectionPool) setLiveChannels(channels []string) {
	live := make(map[string]bool, len(channels))
	for _, channel := range channels {
		live[channel] = true
	}
	pool.liveChannels.Store(&live)
}

func (pool *ConnectionPool) isLive(channel string) bool {
	live := pool.liveChannels.Load()
	return live != nil && (*live)[channel]
}

// channelLimitLabel renders the configured cap for logs; zero means no cap.
func (pool *ConnectionPool) channelLimitLabel() string {
	if pool.totalChannels <= 0 {
		return "unlimited"
	}
	return strconv.Itoa(pool.totalChannels)
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
func (pool *ConnectionPool) discoverAllChannels(minViewer int) (discovered []string, err error) {
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime).Seconds()
		metrics.ChannelDiscoveryDuration.Observe(duration)
		// Discovery usually ends at the viewer floor, so record the count here
		// rather than on the (now rarely taken) capped exit.
		if err == nil {
			metrics.ChannelsDiscovered.Set(float64(len(discovered)))
		}
	}()

	var channels []string
	seen := make(map[string]bool)
	seenCursors := make(map[string]bool)
	cursor := ""
	pageSize := 100

	// A zero limit means "no cap": page until Twitch runs out of live streams or
	// the viewer floor is reached.
	for pool.totalChannels <= 0 || len(channels) < pool.totalChannels {
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
				slog.Info("Reached channels below the viewer floor, stopping discovery", "viewers", stream.ViewerCount, "min_viewers", minViewer)
				slog.Info("Final channel count", "count", len(channels))
				return channels, nil
			}

			channelName := "#" + stream.UserLogin
			if stream.UserLogin != "" && !seen[channelName] {
				channels = append(channels, channelName)
				seen[channelName] = true
			}

			if pool.totalChannels > 0 && len(channels) >= pool.totalChannels {
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
			pool.setLiveChannels(newChannels)

			// Shuffle for even distribution
			rand.Shuffle(len(newChannels), func(i, j int) {
				newChannels[i], newChannels[j] = newChannels[j], newChannels[i]
			})

			pool.growForChannels(pool.assignDiscoveredChannels(newChannels))

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
// Channels that no existing socket had room for are returned so the caller can
// grow the pool; only a configured channel cap drops them.
func (pool *ConnectionPool) assignDiscoveredChannels(channels []string) []string {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.stopping {
		return nil
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
	var unassigned []string
	for _, channel := range channels {
		if assigned[channel] {
			continue
		}
		if pool.totalChannels > 0 && len(assigned) >= pool.totalChannels {
			skipped++
			continue
		}
		best := -1
		for i, load := range loads {
			if load < pool.channelsPerConnection && (best == -1 || load < loads[best]) {
				best = i
			}
		}
		assigned[channel] = true
		if best == -1 {
			// Every socket is full; the caller starts more connections for these.
			unassigned = append(unassigned, channel)
			continue
		}
		pool.connections[best].assignChannels([]string{channel})
		loads[best]++
	}
	if skipped > 0 {
		slog.Warn("MAX_CHANNELS reached; retaining existing assignments", "skipped", skipped, "limit", pool.totalChannels)
	}
	return unassigned
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
