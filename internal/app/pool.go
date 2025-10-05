package app

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"twitch-chat-scrapper/internal/config"
	"twitch-chat-scrapper/internal/db"
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

	twitchClient := twitch.NewClient(clientID, clientSecret)
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
					if err := pool.sharedDB.SaveChatMessageBatch(batch); err != nil {
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
					if err := pool.sharedDB.SaveChatMessageBatch(batch); err != nil {
						fmt.Printf("[DB Writer] Error saving batch: %v\n", err)
					}
					batch = batch[:0] // Clear batch
				}

			case <-ticker.C:
				if len(batch) > 0 {
					if err := pool.sharedDB.SaveChatMessageBatch(batch); err != nil {
						fmt.Printf("[DB Writer] Error saving batch: %v\n", err)
					}
					batch = batch[:0] // Clear batch
				}

				// Report if central channel is getting full
				channelLen := len(pool.centralMessageChan)
				if channelLen > 50000 {
					fmt.Printf("[DB Writer] WARNING: Central message channel is %d%% full (%d/100000)\n",
						channelLen*100/100000, channelLen)
				}
			}
		}
	}()
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

	fmt.Printf("Discovered %d channels, distributing across connections...\n", len(allChannels))

	// Create and start each connection with proper spacing
	connectionDelay := 2 * time.Second // Wait 2 seconds between each connection

	fmt.Printf("Starting connections with %v delay between each...\n", connectionDelay)

	for i := 0; i < numConnections; i++ {
		// Calculate channel slice for this connection
		startIdx := i * pool.channelsPerConnection
		endIdx := startIdx + pool.channelsPerConnection

		if endIdx > len(allChannels) {
			endIdx = len(allChannels)
		}

		if startIdx >= len(allChannels) {
			break // No more channels to assign
		}

		connectionChannels := allChannels[startIdx:endIdx]

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
				subscriber.infiniteReadChat()
				chatDone <- fmt.Errorf("infiniteReadChat exited")
			}()

			// Give the chat reader a moment to start and authenticate
			time.Sleep(500 * time.Millisecond)

			// Join assigned channels
			fmt.Printf("[Connection %d] Joining %d channels\n", connectionID, len(channels))
			subscriber.joinNewChannels(channels)
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

				// Individual connection stats (less frequent)
				if totalMessages%10 == 0 {
					fmt.Printf("[Conn %d] Messages: %d | Channels: %d | Connected: %v\n",
						i, conn.NMessages, len(conn.connectedChannels), conn.IsConnected)
				}
			}

			messagesPerSecond := totalMessages - lastTotalMessages
			lastTotalMessages = totalMessages

			fmt.Printf("[POOL STATS] Connections: %d/%d active | Messages/sec: %d | Total channels: %d | Total messages: %d\n",
				activeConnections, len(pool.connections), messagesPerSecond, totalChannels, totalMessages)

			pool.mu.RUnlock()
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
