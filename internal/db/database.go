package db

import (
	"fmt"
	"time"
)

// NewDatabase creates a new database instance based on the provided configuration
func NewDatabase(config *DBConfig) (Database, error) {
	switch config.Type {
	case "sqlite":
		db := NewSQLiteDB(config.Path)
		if err := db.InitDB(); err != nil {
			return nil, fmt.Errorf("failed to initialize SQLite database: %w", err)
		}
		return db, nil

	case "postgres", "postgresql":
		db := NewPostgresDB(config)
		if err := db.InitDB(); err != nil {
			return nil, fmt.Errorf("failed to initialize PostgreSQL database: %w", err)
		}
		return db, nil

	case "clickhouse":
		db := NewClickHouseDB(config)
		if err := db.InitDB(); err != nil {
			return nil, fmt.Errorf("failed to initialize ClickHouse database: %w", err)
		}
		return db, nil

	default:
		return nil, fmt.Errorf("unsupported database type: %s", config.Type)
	}
}

// Legacy functions for backward compatibility

// InitDB initializes a database with the given path (defaults to SQLite)
// Deprecated: Use NewDatabase with DBConfig instead
func InitDB(dbPath string) (Database, error) {
	config := &DBConfig{
		Type: "sqlite",
		Path: dbPath,
	}
	return NewDatabase(config)
}

// SaveChatMessageBatch saves multiple messages in a single transaction for better performance
// Deprecated: Use database.SaveChatMessageBatch() method instead
func SaveChatMessageBatch(db Database, messages interface{}) error {
	return db.SaveChatMessageBatch(messages)
}

// CloseDB closes the database connection gracefully
// Deprecated: Use database.Close() method instead
func CloseDB(db Database) {
	if db != nil {
		// Wait a bit for any pending database operations
		time.Sleep(100 * time.Millisecond)
		db.Close()
	}
}
