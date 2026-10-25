package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"poglytics-scraper/internal/twitch"
	"poglytics-scraper/internal/util"
)

// NewIRCConnection creates an IRC connection that uses a shared message channel (for connection pool)
func NewIRCConnection(database DatabaseInterface, twitchClient *twitch.Client, sharedChan chan *ChatMessage) *IRCConnection {
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
		ListChannels:      make([]string, 0),
		messageChan:       sharedChan, // Use shared channel - NO local dbWorker!
		ctx:               ctx,
		cancel:            cancel,
		connectedChannels: make(map[string]bool),
		joinAttempts:      make(map[string]joinAttempt),
		joinUpdates:       make(chan struct{}, 1),
		DB:                database,
		TwitchClient:      twitchClient,
	}

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
	defer func() {
		if err := f.Close(); err != nil {
			slog.Error("Error closing log file", "error", err)
		}
	}()

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
	if c.cancel != nil {
		c.cancel()
	}
	c.closeSocket()
}

// Closing the socket interrupts blocked reads and writes. The pool owns the DB.
func (c *IRCConnection) closeSocket() {
	c.connectionMu.RLock()
	if c.Connection != nil {
		_ = c.Connection.Close()
	}
	c.connectionMu.RUnlock()
	c.IsConnected.Store(false)
}
