package db

import (
	"database/sql"
	"fmt"
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

	// Create all tables
	tables := []string{
		// Legacy table (kept for compatibility)
		`CREATE TABLE IF NOT EXISTS chat_messages (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			nickname TEXT NOT NULL,
			message TEXT NOT NULL,
			channel TEXT NOT NULL,
			timestamp DATETIME NOT NULL
		)`,

		// Text messages table
		`CREATE TABLE IF NOT EXISTS text_messages (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			nickname TEXT NOT NULL,
			display_name TEXT,
			user_id TEXT,
			message TEXT NOT NULL,
			channel TEXT NOT NULL,
			timestamp DATETIME NOT NULL,
			color TEXT,
			badges TEXT,
			bits_amount INTEGER DEFAULT 0,
			raw_message TEXT
		)`,

		// Subscriptions table
		`CREATE TABLE IF NOT EXISTS subscriptions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			nickname TEXT,
			display_name TEXT,
			user_id TEXT,
			channel TEXT NOT NULL,
			timestamp DATETIME NOT NULL,
			message_type TEXT NOT NULL,
			sub_plan TEXT,
			sub_plan_name TEXT,
			months INTEGER DEFAULT 0,
			cumulative_months INTEGER DEFAULT 0,
			streak_months INTEGER DEFAULT 0,
			is_gift INTEGER DEFAULT 0,
			gifter_name TEXT,
			gifter_id TEXT,
			target_user TEXT,
			system_message TEXT,
			user_message TEXT,
			raw_message TEXT
		)`,

		// Bans and timeouts table
		`CREATE TABLE IF NOT EXISTS bans (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			channel TEXT NOT NULL,
			timestamp DATETIME NOT NULL,
			message_type TEXT NOT NULL,
			target_user TEXT NOT NULL,
			ban_duration INTEGER DEFAULT 0,
			ban_reason TEXT,
			raw_message TEXT
		)`,

		// Deleted messages table
		`CREATE TABLE IF NOT EXISTS deleted_messages (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			nickname TEXT,
			channel TEXT NOT NULL,
			timestamp DATETIME NOT NULL,
			message TEXT,
			target_message_id TEXT,
			raw_message TEXT
		)`,

		// Raids table
		`CREATE TABLE IF NOT EXISTS raids (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			channel TEXT NOT NULL,
			timestamp DATETIME NOT NULL,
			raider_name TEXT NOT NULL,
			viewer_count INTEGER DEFAULT 0,
			system_message TEXT,
			raw_message TEXT
		)`,

		// Bits/cheers table
		`CREATE TABLE IF NOT EXISTS bits (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			nickname TEXT,
			display_name TEXT,
			user_id TEXT,
			channel TEXT NOT NULL,
			timestamp DATETIME NOT NULL,
			message_type TEXT NOT NULL,
			bits_amount INTEGER DEFAULT 0,
			message TEXT,
			raw_message TEXT
		)`,

		// Notices table
		`CREATE TABLE IF NOT EXISTS notices (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			channel TEXT NOT NULL,
			timestamp DATETIME NOT NULL,
			message_type TEXT NOT NULL,
			notice_message_id TEXT,
			message TEXT,
			system_message TEXT,
			raw_message TEXT
		)`,

		// Hosts table
		`CREATE TABLE IF NOT EXISTS hosts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			channel TEXT NOT NULL,
			timestamp DATETIME NOT NULL,
			target_user TEXT,
			viewer_count INTEGER DEFAULT 0,
			raw_message TEXT
		)`,

		// Other messages table
		`CREATE TABLE IF NOT EXISTS other_messages (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			channel TEXT NOT NULL,
			timestamp DATETIME NOT NULL,
			message_type TEXT,
			nickname TEXT,
			message TEXT,
			raw_message TEXT
		)`,
	}

	// Create indexes
	indexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_text_messages_channel ON text_messages(channel)`,
		`CREATE INDEX IF NOT EXISTS idx_text_messages_timestamp ON text_messages(timestamp)`,
		`CREATE INDEX IF NOT EXISTS idx_subscriptions_channel ON subscriptions(channel)`,
		`CREATE INDEX IF NOT EXISTS idx_subscriptions_timestamp ON subscriptions(timestamp)`,
		`CREATE INDEX IF NOT EXISTS idx_bans_channel ON bans(channel)`,
		`CREATE INDEX IF NOT EXISTS idx_bans_timestamp ON bans(timestamp)`,
		`CREATE INDEX IF NOT EXISTS idx_raids_channel ON raids(channel)`,
		`CREATE INDEX IF NOT EXISTS idx_raids_timestamp ON raids(timestamp)`,
	}

	// Execute table creation
	for _, tableSQL := range tables {
		if _, err := s.db.Exec(tableSQL); err != nil {
			s.db.Close()
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	// Execute index creation
	for _, indexSQL := range indexes {
		if _, err := s.db.Exec(indexSQL); err != nil {
			// Continue even if index creation fails (it might already exist)
			fmt.Printf("Warning: failed to create index: %v\n", err)
		}
	}

	return nil
}

// SaveChatMessageBatch saves multiple messages in a single transaction for better performance
// Routes messages to appropriate tables based on MessageType
func (s *SQLiteDB) SaveChatMessageBatch(messages interface{}) error {
	// Convert interface{} to []*ChatMessage
	chatMessages, ok := messages.([]*ChatMessage)
	if !ok {
		return fmt.Errorf("messages must be []*ChatMessage, got %T", messages)
	}

	if len(chatMessages) == 0 {
		return nil
	}

	// Group messages by type
	textMessages := []*ChatMessage{}
	subscriptions := []*ChatMessage{}
	bans := []*ChatMessage{}
	deletedMessages := []*ChatMessage{}
	raids := []*ChatMessage{}
	bits := []*ChatMessage{}
	notices := []*ChatMessage{}
	hosts := []*ChatMessage{}
	other := []*ChatMessage{}

	for _, msg := range chatMessages {
		switch msg.MessageType {
		case "text_message":
			textMessages = append(textMessages, msg)
		case "subscription", "resub", "subscription_gift", "mystery_subscription_gift":
			subscriptions = append(subscriptions, msg)
		case "ban", "timeout":
			bans = append(bans, msg)
		case "delete_message", "clear_chat":
			deletedMessages = append(deletedMessages, msg)
		case "raid":
			raids = append(raids, msg)
		case "bits", "bits_badge_tier":
			bits = append(bits, msg)
		case "notice", "user_notice":
			notices = append(notices, msg)
		case "host_target":
			hosts = append(hosts, msg)
		default:
			other = append(other, msg)
		}
	}

	// Save to appropriate tables
	if len(textMessages) > 0 {
		if err := s.SaveTextMessages(textMessages); err != nil {
			return fmt.Errorf("failed to save text messages: %w", err)
		}
	}
	if len(subscriptions) > 0 {
		if err := s.SaveSubscriptions(subscriptions); err != nil {
			return fmt.Errorf("failed to save subscriptions: %w", err)
		}
	}
	if len(bans) > 0 {
		if err := s.SaveBans(bans); err != nil {
			return fmt.Errorf("failed to save bans: %w", err)
		}
	}
	if len(deletedMessages) > 0 {
		if err := s.SaveDeletedMessages(deletedMessages); err != nil {
			return fmt.Errorf("failed to save deleted messages: %w", err)
		}
	}
	if len(raids) > 0 {
		if err := s.SaveRaids(raids); err != nil {
			return fmt.Errorf("failed to save raids: %w", err)
		}
	}
	if len(bits) > 0 {
		if err := s.SaveBits(bits); err != nil {
			return fmt.Errorf("failed to save bits: %w", err)
		}
	}
	if len(notices) > 0 {
		if err := s.SaveNotices(notices); err != nil {
			return fmt.Errorf("failed to save notices: %w", err)
		}
	}
	if len(hosts) > 0 {
		if err := s.SaveHosts(hosts); err != nil {
			return fmt.Errorf("failed to save hosts: %w", err)
		}
	}
	if len(other) > 0 {
		if err := s.SaveOther(other); err != nil {
			return fmt.Errorf("failed to save other messages: %w", err)
		}
	}

	return nil
}

// SaveTextMessages saves text messages to the text_messages table
func (s *SQLiteDB) SaveTextMessages(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO text_messages
		(nickname, display_name, user_id, message, channel, timestamp, color, badges, bits_amount, raw_message)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, msg := range messages {
		badges := joinStrings(msg.Badges, ",")

		_, err = stmt.Exec(
			msg.Nickname,
			msg.DisplayName,
			msg.UserID,
			msg.Message,
			msg.Channel,
			msg.Timestamp,
			msg.Color,
			badges,
			msg.BitsAmount,
			msg.RawMessage,
		)
		if err != nil {
			return fmt.Errorf("failed to insert text message: %w", err)
		}
	}

	return tx.Commit()
}

// SaveSubscriptions saves subscription events to the subscriptions table
func (s *SQLiteDB) SaveSubscriptions(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO subscriptions
		(nickname, display_name, user_id, channel, timestamp, message_type, sub_plan, sub_plan_name,
		 months, cumulative_months, streak_months, is_gift, gifter_name, gifter_id, target_user,
		 system_message, user_message, raw_message)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, msg := range messages {
		isGift := 0
		if msg.IsGift {
			isGift = 1
		}

		_, err = stmt.Exec(
			msg.Nickname,
			msg.DisplayName,
			msg.UserID,
			msg.Channel,
			msg.Timestamp,
			msg.MessageType,
			msg.SubPlan,
			msg.SubPlanName,
			msg.Months,
			msg.CumulativeMonths,
			msg.StreakMonths,
			isGift,
			msg.GifterName,
			msg.GifterID,
			msg.TargetUser,
			msg.SystemMessage,
			msg.Message,
			msg.RawMessage,
		)
		if err != nil {
			return fmt.Errorf("failed to insert subscription: %w", err)
		}
	}

	return tx.Commit()
}

// SaveBans saves ban and timeout events to the bans table
func (s *SQLiteDB) SaveBans(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO bans
		(channel, timestamp, message_type, target_user, ban_duration, ban_reason, raw_message)
		VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, msg := range messages {
		_, err = stmt.Exec(
			msg.Channel,
			msg.Timestamp,
			msg.MessageType,
			msg.TargetUser,
			msg.BanDuration,
			msg.BanReason,
			msg.RawMessage,
		)
		if err != nil {
			return fmt.Errorf("failed to insert ban: %w", err)
		}
	}

	return tx.Commit()
}

// SaveDeletedMessages saves deleted message events to the deleted_messages table
func (s *SQLiteDB) SaveDeletedMessages(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO deleted_messages
		(nickname, channel, timestamp, message, target_message_id, raw_message)
		VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, msg := range messages {
		_, err = stmt.Exec(
			msg.Nickname,
			msg.Channel,
			msg.Timestamp,
			msg.Message,
			msg.TargetMessageID,
			msg.RawMessage,
		)
		if err != nil {
			return fmt.Errorf("failed to insert deleted message: %w", err)
		}
	}

	return tx.Commit()
}

// SaveRaids saves raid events to the raids table
func (s *SQLiteDB) SaveRaids(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO raids
		(channel, timestamp, raider_name, viewer_count, system_message, raw_message)
		VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, msg := range messages {
		_, err = stmt.Exec(
			msg.Channel,
			msg.Timestamp,
			msg.RaiderName,
			msg.ViewerCount,
			msg.SystemMessage,
			msg.RawMessage,
		)
		if err != nil {
			return fmt.Errorf("failed to insert raid: %w", err)
		}
	}

	return tx.Commit()
}

// SaveBits saves bits and cheer events to the bits table
func (s *SQLiteDB) SaveBits(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO bits
		(nickname, display_name, user_id, channel, timestamp, message_type, bits_amount, message, raw_message)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, msg := range messages {
		_, err = stmt.Exec(
			msg.Nickname,
			msg.DisplayName,
			msg.UserID,
			msg.Channel,
			msg.Timestamp,
			msg.MessageType,
			msg.BitsAmount,
			msg.Message,
			msg.RawMessage,
		)
		if err != nil {
			return fmt.Errorf("failed to insert bits: %w", err)
		}
	}

	return tx.Commit()
}

// SaveNotices saves notice events to the notices table
func (s *SQLiteDB) SaveNotices(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO notices
		(channel, timestamp, message_type, notice_message_id, message, system_message, raw_message)
		VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, msg := range messages {
		_, err = stmt.Exec(
			msg.Channel,
			msg.Timestamp,
			msg.MessageType,
			msg.NoticeMessageID,
			msg.Message,
			msg.SystemMessage,
			msg.RawMessage,
		)
		if err != nil {
			return fmt.Errorf("failed to insert notice: %w", err)
		}
	}

	return tx.Commit()
}

// SaveHosts saves host events to the hosts table
func (s *SQLiteDB) SaveHosts(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO hosts
		(channel, timestamp, target_user, viewer_count, raw_message)
		VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, msg := range messages {
		_, err = stmt.Exec(
			msg.Channel,
			msg.Timestamp,
			msg.TargetUser,
			msg.ViewerCount,
			msg.RawMessage,
		)
		if err != nil {
			return fmt.Errorf("failed to insert host: %w", err)
		}
	}

	return tx.Commit()
}

// SaveOther saves other/unknown message types to the other_messages table
func (s *SQLiteDB) SaveOther(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO other_messages
		(channel, timestamp, message_type, nickname, message, raw_message)
		VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, msg := range messages {
		_, err = stmt.Exec(
			msg.Channel,
			msg.Timestamp,
			msg.MessageType,
			msg.Nickname,
			msg.Message,
			msg.RawMessage,
		)
		if err != nil {
			return fmt.Errorf("failed to insert other message: %w", err)
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
