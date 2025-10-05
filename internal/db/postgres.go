package db

import (
	"database/sql"
	"fmt"
	"reflect"
	"time"

	_ "github.com/lib/pq"
)

// PostgresDB implements the Database interface for PostgreSQL
type PostgresDB struct {
	db     *sql.DB
	config *DBConfig
}

// NewPostgresDB creates a new PostgreSQL database instance
func NewPostgresDB(config *DBConfig) *PostgresDB {
	return &PostgresDB{
		config: config,
	}
}

// InitDB initializes the PostgreSQL database and creates the necessary tables
func (p *PostgresDB) InitDB() error {
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		p.config.Host,
		p.config.Port,
		p.config.User,
		p.config.Password,
		p.config.Database,
		p.config.SSLMode,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to open postgres database: %w", err)
	}

	// Test the connection
	if err = db.Ping(); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping postgres database: %w", err)
	}

	p.db = db

	// Create table if it doesn't exist
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS chat_messages (
		id SERIAL PRIMARY KEY,
		nickname TEXT NOT NULL,
		message TEXT NOT NULL,
		channel TEXT NOT NULL,
		timestamp TIMESTAMP NOT NULL
	);

	CREATE INDEX IF NOT EXISTS idx_chat_messages_timestamp ON chat_messages(timestamp);
	CREATE INDEX IF NOT EXISTS idx_chat_messages_channel ON chat_messages(channel);`

	_, err = p.db.Exec(createTableSQL)
	if err != nil {
		p.db.Close()
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

// SaveChatMessageBatch saves multiple messages in a single transaction for better performance
func (p *PostgresDB) SaveChatMessageBatch(messages interface{}) error {
	// Use reflection to handle any message slice type
	val := reflect.ValueOf(messages)
	if val.Kind() != reflect.Slice {
		return fmt.Errorf("messages must be a slice")
	}

	if val.Len() == 0 {
		return nil
	}

	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO chat_messages (nickname, message, channel, timestamp) VALUES ($1, $2, $3, $4)`)
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

		_, err = stmt.Exec(nickname, message, channel, timestamp)
		if err != nil {
			return fmt.Errorf("failed to insert message: %w", err)
		}
	}

	return tx.Commit()
}

// Close closes the database connection gracefully
func (p *PostgresDB) Close() {
	if p.db != nil {
		// Wait a bit for any pending database operations
		time.Sleep(100 * time.Millisecond)
		p.db.Close()
	}
}

// GetDB returns the underlying *sql.DB for raw queries
func (p *PostgresDB) GetDB() *sql.DB {
	return p.db
}
