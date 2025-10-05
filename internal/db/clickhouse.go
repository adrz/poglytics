package db

import (
	"database/sql"
	"fmt"
	"reflect"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

// ClickHouseDB implements the Database interface for ClickHouse
type ClickHouseDB struct {
	db     *sql.DB
	config *DBConfig
}

// NewClickHouseDB creates a new ClickHouse database instance
func NewClickHouseDB(config *DBConfig) *ClickHouseDB {
	return &ClickHouseDB{
		config: config,
	}
}

// InitDB initializes the ClickHouse database and creates the necessary tables
func (c *ClickHouseDB) InitDB() error {
	// ClickHouse Go driver v2 DSN format
	// Format: clickhouse://username:password@host:port/database?param1=value1&param2=value2
	connStr := fmt.Sprintf("clickhouse://%s:%s@%s:%s/%s?dial_timeout=10s&compress=true",
		c.config.User,
		c.config.Password,
		c.config.Host,
		c.config.Port,
		c.config.Database,
	)

	db, err := sql.Open("clickhouse", connStr)
	if err != nil {
		return fmt.Errorf("failed to open clickhouse database: %w", err)
	}

	// Set connection pool settings
	db.SetMaxIdleConns(5)
	db.SetMaxOpenConns(10)
	db.SetConnMaxLifetime(time.Hour)

	// Test the connection
	if err = db.Ping(); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping clickhouse database: %w", err)
	}

	c.db = db

	// Create table if it doesn't exist
	// Using MergeTree engine for better performance
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS chat_messages (
		id UInt64,
		nickname String,
		message String,
		channel String,
		timestamp DateTime
	) ENGINE = MergeTree()
	ORDER BY (timestamp, channel)
	PARTITION BY toYYYYMM(timestamp)`

	_, err = c.db.Exec(createTableSQL)
	if err != nil {
		c.db.Close()
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

// SaveChatMessageBatch saves multiple messages in a single transaction for better performance
func (c *ClickHouseDB) SaveChatMessageBatch(messages interface{}) error {
	// Use reflection to handle any message slice type
	val := reflect.ValueOf(messages)
	if val.Kind() != reflect.Slice {
		return fmt.Errorf("messages must be a slice")
	}

	if val.Len() == 0 {
		return nil
	}

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// ClickHouse uses $1, $2, etc. placeholders
	stmt, err := tx.Prepare(`INSERT INTO chat_messages (id, nickname, message, channel, timestamp) VALUES ($1, $2, $3, $4, $5)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for i := 0; i < val.Len(); i++ {
		msg := val.Index(i)

		// Handle pointer to struct
		if msg.Kind() == reflect.Ptr {
			msg = msg.Elem()
		}

		// Extract fields
		nickname := msg.FieldByName("Nickname").String()
		message := msg.FieldByName("Message").String()
		channel := msg.FieldByName("Channel").String()
		timestamp := msg.FieldByName("Timestamp").Interface()

		// Generate a unique ID based on timestamp and index
		var id uint64
		if ts, ok := timestamp.(time.Time); ok {
			id = uint64(ts.Unix())*1000000 + uint64(i)
		}

		_, err = stmt.Exec(id, nickname, message, channel, timestamp)
		if err != nil {
			return fmt.Errorf("failed to insert message: %w", err)
		}
	}

	return tx.Commit()
}

// Close closes the database connection gracefully
func (c *ClickHouseDB) Close() {
	if c.db != nil {
		// Wait a bit for any pending database operations
		time.Sleep(100 * time.Millisecond)
		c.db.Close()
	}
}

// GetDB returns the underlying *sql.DB for raw queries
func (c *ClickHouseDB) GetDB() *sql.DB {
	return c.db
}
