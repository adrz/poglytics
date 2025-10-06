package app

import (
	"bufio"
	"context"
	"database/sql"
	"net"
	"sync"
	"time"

	"twitch-chat-scrapper/internal/twitch"
)

// DatabaseInterface defines methods that database implementations must support
type DatabaseInterface interface {
	// InitDB initializes the database connection and creates necessary tables
	InitDB() error

	// SaveChatMessageBatch saves multiple messages in a single transaction
	SaveChatMessageBatch(messages interface{}) error

	// Close closes the database connection gracefully
	Close()

	// GetDB returns the underlying *sql.DB for raw queries
	GetDB() *sql.DB
}

// Subscriber represents the main chat scraper instance
type Subscriber struct {
	ID           string
	Server       string
	Token        string
	Nickname     string
	Port         int
	Channels     []string
	IsConnected  bool
	NMessages    int
	NChannels    int
	ListChannels []string
	Connection   net.Conn
	Reader       *bufio.Reader
	DB           DatabaseInterface
	dbMutex      sync.Mutex
	messageChan  chan *ChatMessage
	ctx          context.Context
	cancel       context.CancelFunc

	// New fields for channel discovery
	TwitchClient      *twitch.Client
	channelsMutex     sync.RWMutex
	connectedChannels map[string]bool
	scanInterval      time.Duration
}

// ChatMessage represents a single chat message
type ChatMessage struct {
	Nickname  string
	Message   string
	Channel   string
	Timestamp time.Time
}
