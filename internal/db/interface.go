package db

import (
	"database/sql"
)

// ChatMessage represents a single chat message (duplicate to avoid import cycle)
type ChatMessage struct {
	Nickname  string
	Message   string
	Channel   string
	Timestamp string
}

// Database interface defines methods that all database implementations must support
type Database interface {
	// InitDB initializes the database connection and creates necessary tables
	InitDB() error

	// SaveChatMessageBatch saves multiple messages in a single transaction
	SaveChatMessageBatch(messages interface{}) error

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
