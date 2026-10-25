package app

import (
	"bufio"
	"context"
	"database/sql"
	"net"
	"sync"
	"sync/atomic"
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
	IsConnected  atomic.Bool
	NMessages    atomic.Int64
	NChannels    atomic.Int64
	ListChannels []string
	Connection   net.Conn
	Reader       *bufio.Reader
	DB           DatabaseInterface
	messageChan  chan *ChatMessage
	ctx          context.Context
	cancel       context.CancelFunc
	writeMu      sync.Mutex   // serializes writes to Connection (JOIN, PONG, auth)
	connectionMu sync.RWMutex // protects socket/reader publication and closure
	readPartial  []byte       // owned by the single socket reader

	// New fields for channel discovery
	TwitchClient  *twitch.Client
	channelsMutex sync.RWMutex
	// connectedChannels holds channels Twitch confirmed on the current socket.
	connectedChannels map[string]bool
	// joinAttempts tracks JOINs sent on the current socket but not yet confirmed.
	joinAttempts map[string]joinAttempt

	// One join worker per socket generation; assignments survive reconnects.
	joinMu      sync.Mutex
	joinUpdates chan struct{}
	nextJoinAt  time.Time
	joinLimiter *joinLimiter      // shared by the pool; nil paces this connection only
	isLive      func(string) bool // nil treats every channel alike
}

type joinAttempt struct {
	count   int
	retryAt time.Time
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
