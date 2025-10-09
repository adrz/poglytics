package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"poglytics-scraper/internal/config"
	"poglytics-scraper/internal/db"
	"poglytics-scraper/internal/twitch"
	"poglytics-scraper/internal/util"
)

// NewIRCConnection creates a new IRC connection with its own database
func NewIRCConnection() *IRCConnection {
	// Random nickname
	id := util.GenerateRandomString(5, "letters")
	nickname := "justinfan" + util.GenerateRandomString(10, "digits")

	ctx, cancel := context.WithCancel(context.Background())

	conn := &IRCConnection{
		ID:                id,
		Server:            "irc.chat.twitch.tv",
		Token:             "oauth",
		Nickname:          nickname,
		Port:              6667,
		Channels:          []string{},
		IsConnected:       false,
		NMessages:         0,
		NChannels:         0,
		ListChannels:      make([]string, 0),
		messageChan:       make(chan *ChatMessage, 1000000),
		ctx:               ctx,
		cancel:            cancel,
		connectedChannels: make(map[string]bool),
		scanInterval:      10 * time.Minute, // Rescan interval for channel discovery
	}

	// Load environment configuration
	env, err := config.LoadEnv()
	if err != nil {
		slog.Warn("Failed to load environment", "error", err)
	}

	// Get database configuration
	dbConfig := config.GetDBConfig(env)

	// Create database configuration
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

	// Initialize database
	database, err := db.NewDatabase(dbCfg)
	if err != nil {
		slog.Error("Failed to initialize database", "error", err)
		os.Exit(1)
	}

	slog.Info("Database initialized successfully", "type", dbCfg.Type)

	conn.DB = database

	go conn.dbWorkerBatch()

	return conn
}

// NewIRCConnectionWithDB creates a new IRC connection with a shared database and Twitch client
// Used by ConnectionPool to create multiple connections sharing the same resources
func NewIRCConnectionWithDB(database DatabaseInterface, twitchClient *twitch.Client) *IRCConnection {
	// Random nickname for each connection
	id := util.GenerateRandomString(5, "letters")
	nickname := "justinfan" + util.GenerateRandomString(10, "digits")

	ctx, cancel := context.WithCancel(context.Background())

	conn := &IRCConnection{
		ID:                id,
		Server:            "irc.chat.twitch.tv",
		Token:             "oauth",
		Nickname:          nickname,
		Port:              6667,
		Channels:          []string{},
		IsConnected:       false,
		NMessages:         0,
		NChannels:         0,
		ListChannels:      make([]string, 0),
		messageChan:       make(chan *ChatMessage, 50000), // Larger buffer for pooled connections
		ctx:               ctx,
		cancel:            cancel,
		connectedChannels: make(map[string]bool),
		scanInterval:      10 * time.Minute,
		DB:                database,     // Use shared database
		TwitchClient:      twitchClient, // Use shared Twitch client
	}

	// Start the database worker for this connection
	go conn.dbWorkerBatch()

	return conn
}

// NewIRCConnectionWithSharedChannel creates an IRC connection that uses a shared message channel (for connection pool)
func NewIRCConnectionWithSharedChannel(database DatabaseInterface, twitchClient *twitch.Client, sharedChan chan *ChatMessage) *IRCConnection {
	// Random nickname for each connection
	id := util.GenerateRandomString(5, "letters")
	nickname := "justinfan" + util.GenerateRandomString(10, "digits")

	ctx, cancel := context.WithCancel(context.Background())

	conn := &IRCConnection{
		ID:                id,
		Server:            "irc.chat.twitch.tv",
		Token:             "oauth",
		Nickname:          nickname,
		Port:              6667,
		Channels:          []string{},
		IsConnected:       false,
		NMessages:         0,
		NChannels:         0,
		ListChannels:      make([]string, 0),
		messageChan:       sharedChan, // Use shared channel - NO local dbWorker!
		ctx:               ctx,
		cancel:            cancel,
		connectedChannels: make(map[string]bool),
		scanInterval:      10 * time.Minute,
		DB:                database,
		TwitchClient:      twitchClient,
	}

	// Do NOT start dbWorkerBatch - pool handles DB writes centrally
	return conn
}

// logDisconnectionEvent logs disconnection events to a file for debugging
func (c *IRCConnection) logDisconnectionEvent(eventType, channel, message string) {
	logFile := "disconnections.log"

	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		slog.Error("Error opening log file", "error", err)
		return
	}
	defer f.Close()

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	logEntry := fmt.Sprintf("[%s] %s | Channel: %s | Message: %s\n",
		timestamp, eventType, channel, message)

	if _, err := f.WriteString(logEntry); err != nil {
		slog.Error("Error writing to log file", "error", err)
		return
	}

	// Also print to console for immediate visibility
	slog.Info("Disconnection event", "type", eventType, "channel", channel, "message", message)
}

// Shutdown gracefully shuts down the IRC connection
func (c *IRCConnection) Shutdown() {
	slog.Info("Shutting down gracefully", "id", c.ID)
	c.cancel()                  // Cancel context to stop all goroutines
	time.Sleep(2 * time.Second) // Give time for goroutines to finish
	if c.DB != nil {
		// Wait a bit for any pending database operations
		time.Sleep(100 * time.Millisecond)
		c.DB.Close()
	}
}
