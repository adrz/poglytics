package app

import (
	"bufio"
	"context"
	"database/sql"
	"net"
	"sync"
	"time"

	"poglytics-scraper/internal/twitch"
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

// IRCConnection represents an IRC connection to Twitch chat
type IRCConnection struct {
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
	messageChan  chan *ChatMessage
	ctx          context.Context
	cancel       context.CancelFunc

	// New fields for channel discovery
	TwitchClient      *twitch.Client
	channelsMutex     sync.RWMutex
	connectedChannels map[string]bool
}

// ChatMessage represents a single chat message or IRC event
type ChatMessage struct {
	// Basic fields (all message types)
	Nickname    string
	Message     string
	Channel     string
	Timestamp   time.Time
	MessageType string // "text_message", "ban", "timeout", "delete_message", "subscription", "notice", etc.

	// IRC Tags (parsed from IRC messages with tags capability)
	Tags map[string]string

	// User info
	UserID      string
	DisplayName string
	Color       string
	Badges      []string

	// Subscription-specific fields
	SubPlan          string // "Prime", "1000", "2000", "3000"
	SubPlanName      string
	Months           int
	CumulativeMonths int
	StreakMonths     int
	IsGift           bool
	GifterName       string
	GifterID         string

	// Ban/Timeout specific
	BanDuration int // seconds, 0 for permanent ban
	BanReason   string
	TargetUser  string

	// Deleted message specific
	TargetMessageID string

	// Raid specific
	RaiderName  string
	ViewerCount int

	// Bits/Cheer specific
	BitsAmount int

	// Notice specific
	NoticeMessageID string // msg-id tag for NOTICE messages
	SystemMessage   string // system-msg tag for USERNOTICE messages

	// Raw message for debugging
	RawMessage string
}
