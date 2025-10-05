package db

import (
	"database/sql"
	"fmt"
	"reflect"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// SQLiteDB implements the Database interface for SQLite
type SQLiteDB struct {
	db   *sql.DB
	path string
}

// NewSQLiteDB creates a new SQLite database instance
func NewSQLiteDB(path string) *SQLiteDB {
	return &SQLiteDB{
		path: path,
	}
}

// InitDB initializes the SQLite database and creates the necessary tables
func (s *SQLiteDB) InitDB() error {
	db, err := sql.Open("sqlite3", s.path)
	if err != nil {
		return fmt.Errorf("failed to open sqlite database: %w", err)
	}

	s.db = db

	// Create table if it doesn't exist
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS chat_messages (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		nickname TEXT NOT NULL,
		message TEXT NOT NULL,
		channel TEXT NOT NULL,
		timestamp DATETIME NOT NULL
	);`

	_, err = s.db.Exec(createTableSQL)
	if err != nil {
		s.db.Close()
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

// SaveChatMessageBatch saves multiple messages in a single transaction for better performance
func (s *SQLiteDB) SaveChatMessageBatch(messages interface{}) error {
	// Use reflection to handle any message slice type
	val := reflect.ValueOf(messages)
	if val.Kind() != reflect.Slice {
		return fmt.Errorf("messages must be a slice")
	}

	if val.Len() == 0 {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO chat_messages (nickname, message, channel, timestamp) VALUES (?, ?, ?, ?)`)
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
func (s *SQLiteDB) Close() {
	if s.db != nil {
		// Wait a bit for any pending database operations
		time.Sleep(100 * time.Millisecond)
		s.db.Close()
	}
}

// GetDB returns the underlying *sql.DB for raw queries
func (s *SQLiteDB) GetDB() *sql.DB {
	return s.db
}
