package db

import (
	"database/sql"
	"time"
)

// ChatMessage represents a single chat message (duplicate to avoid import cycle)
type ChatMessage struct {
	Nickname    string
	Message     string
	Channel     string
	Timestamp   time.Time
	MessageType string

	// IRC Tags
	Tags map[string]string

	// User info
	UserID      string
	DisplayName string
	Color       string
	Badges      []string

	// Subscription-specific fields
	SubPlan          string
	SubPlanName      string
	Months           int
	CumulativeMonths int
	StreakMonths     int
	IsGift           bool
	GifterName       string
	GifterID         string

	// Ban/Timeout specific
	BanDuration int
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
	NoticeMessageID string
	SystemMessage   string

	// Raw message for debugging
	RawMessage string
}

// StreamSnapshot represents a snapshot of a Twitch stream at a point in time
type StreamSnapshot struct {
	ID           string
	UserID       string
	UserLogin    string
	UserName     string
	GameID       string
	GameName     string
	Type         string
	Title        string
	ViewerCount  int
	StartedAt    time.Time
	Language     string
	ThumbnailURL string
	Tags         []string
	TagIDs       []string
	IsMature     bool
	SnapshotTime time.Time // When this snapshot was captured
}

// Database interface defines methods that all database implementations must support
type Database interface {
	// InitDB initializes the database connection and creates necessary tables
	InitDB() error

	// SaveChatMessageBatch saves multiple messages in a single transaction
	// Routes messages to appropriate tables based on MessageType
	SaveChatMessageBatch(messages interface{}) error

	// Individual save methods for specific message types
	SaveTextMessages(messages []*ChatMessage) error
	SaveSubscriptions(messages []*ChatMessage) error
	SaveBans(messages []*ChatMessage) error
	SaveDeletedMessages(messages []*ChatMessage) error
	SaveRaids(messages []*ChatMessage) error
	SaveBits(messages []*ChatMessage) error
	SaveNotices(messages []*ChatMessage) error
	SaveHosts(messages []*ChatMessage) error
	SaveOther(messages []*ChatMessage) error

	// SaveStreamSnapshots saves multiple stream snapshots in a batch
	SaveStreamSnapshots(snapshots []*StreamSnapshot) error

	// Close closes the database connection gracefully
	Close()

	// GetDB returns the underlying *sql.DB for raw queries
	GetDB() *sql.DB
}

// DBConfig holds configuration for database connection
type DBConfig struct {
	Type     string // "sqlite", "postgres", "clickhouse"
	Host     string
	Port     string
	User     string
	Password string
	Database string
	SSLMode  string // For PostgreSQL
	Path     string // For SQLite
}
