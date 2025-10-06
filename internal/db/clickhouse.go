package db

import (
	"database/sql"
	"fmt"
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

	// Create all tables for different message types
	tables := []string{
		// Text messages table
		`CREATE TABLE IF NOT EXISTS text_messages (
			id UInt64,
			nickname String,
			display_name String,
			user_id String,
			message String,
			channel String,
			timestamp DateTime,
			color String,
			badges String,
			bits_amount UInt32,
			raw_message String
		) ENGINE = MergeTree()
		ORDER BY (timestamp, channel)
		PARTITION BY toYYYYMM(timestamp)`,

		// Subscriptions table
		`CREATE TABLE IF NOT EXISTS subscriptions (
			id UInt64,
			nickname String,
			display_name String,
			user_id String,
			channel String,
			timestamp DateTime,
			message_type String,
			sub_plan String,
			sub_plan_name String,
			months UInt32,
			cumulative_months UInt32,
			streak_months UInt32,
			is_gift UInt8,
			gifter_name String,
			gifter_id String,
			target_user String,
			system_message String,
			user_message String,
			raw_message String
		) ENGINE = MergeTree()
		ORDER BY (timestamp, channel)
		PARTITION BY toYYYYMM(timestamp)`,

		// Bans table (includes timeouts)
		`CREATE TABLE IF NOT EXISTS bans (
			id UInt64,
			channel String,
			timestamp DateTime,
			message_type String,
			target_user String,
			ban_duration UInt32,
			ban_reason String,
			raw_message String
		) ENGINE = MergeTree()
		ORDER BY (timestamp, channel)
		PARTITION BY toYYYYMM(timestamp)`,

		// Deleted messages table
		`CREATE TABLE IF NOT EXISTS deleted_messages (
			id UInt64,
			nickname String,
			channel String,
			timestamp DateTime,
			message String,
			target_message_id String,
			raw_message String
		) ENGINE = MergeTree()
		ORDER BY (timestamp, channel)
		PARTITION BY toYYYYMM(timestamp)`,

		// Raids table
		`CREATE TABLE IF NOT EXISTS raids (
			id UInt64,
			channel String,
			timestamp DateTime,
			raider_name String,
			viewer_count UInt32,
			system_message String,
			raw_message String
		) ENGINE = MergeTree()
		ORDER BY (timestamp, channel)
		PARTITION BY toYYYYMM(timestamp)`,

		// Bits table
		`CREATE TABLE IF NOT EXISTS bits (
			id UInt64,
			nickname String,
			display_name String,
			user_id String,
			channel String,
			timestamp DateTime,
			message_type String,
			bits_amount UInt32,
			message String,
			raw_message String
		) ENGINE = MergeTree()
		ORDER BY (timestamp, channel)
		PARTITION BY toYYYYMM(timestamp)`,

		// Notices table
		`CREATE TABLE IF NOT EXISTS notices (
			id UInt64,
			channel String,
			timestamp DateTime,
			message_type String,
			notice_message_id String,
			message String,
			system_message String,
			raw_message String
		) ENGINE = MergeTree()
		ORDER BY (timestamp, channel)
		PARTITION BY toYYYYMM(timestamp)`,

		// Hosts table
		`CREATE TABLE IF NOT EXISTS hosts (
			id UInt64,
			channel String,
			timestamp DateTime,
			target_user String,
			viewer_count UInt32,
			raw_message String
		) ENGINE = MergeTree()
		ORDER BY (timestamp, channel)
		PARTITION BY toYYYYMM(timestamp)`,

		// Other messages table
		`CREATE TABLE IF NOT EXISTS other_messages (
			id UInt64,
			channel String,
			timestamp DateTime,
			message_type String,
			nickname String,
			message String,
			raw_message String
		) ENGINE = MergeTree()
		ORDER BY (timestamp, channel)
		PARTITION BY toYYYYMM(timestamp)`,

		// Keep legacy table for backward compatibility
		`CREATE TABLE IF NOT EXISTS chat_messages (
			id UInt64,
			nickname String,
			message String,
			channel String,
			timestamp DateTime
		) ENGINE = MergeTree()
		ORDER BY (timestamp, channel)
		PARTITION BY toYYYYMM(timestamp)`,
	}

	for _, createTableSQL := range tables {
		_, err = c.db.Exec(createTableSQL)
		if err != nil {
			c.db.Close()
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	return nil
}

// SaveChatMessageBatch saves multiple messages in a single transaction for better performance
// Routes messages to appropriate tables based on MessageType
func (c *ClickHouseDB) SaveChatMessageBatch(messages interface{}) error {
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
		if err := c.SaveTextMessages(textMessages); err != nil {
			return fmt.Errorf("failed to save text messages: %w", err)
		}
	}
	if len(subscriptions) > 0 {
		if err := c.SaveSubscriptions(subscriptions); err != nil {
			return fmt.Errorf("failed to save subscriptions: %w", err)
		}
	}
	if len(bans) > 0 {
		if err := c.SaveBans(bans); err != nil {
			return fmt.Errorf("failed to save bans: %w", err)
		}
	}
	if len(deletedMessages) > 0 {
		if err := c.SaveDeletedMessages(deletedMessages); err != nil {
			return fmt.Errorf("failed to save deleted messages: %w", err)
		}
	}
	if len(raids) > 0 {
		if err := c.SaveRaids(raids); err != nil {
			return fmt.Errorf("failed to save raids: %w", err)
		}
	}
	if len(bits) > 0 {
		if err := c.SaveBits(bits); err != nil {
			return fmt.Errorf("failed to save bits: %w", err)
		}
	}
	if len(notices) > 0 {
		if err := c.SaveNotices(notices); err != nil {
			return fmt.Errorf("failed to save notices: %w", err)
		}
	}
	if len(hosts) > 0 {
		if err := c.SaveHosts(hosts); err != nil {
			return fmt.Errorf("failed to save hosts: %w", err)
		}
	}
	if len(other) > 0 {
		if err := c.SaveOther(other); err != nil {
			return fmt.Errorf("failed to save other messages: %w", err)
		}
	}

	return nil
}

// generateID generates a unique ID based on timestamp and index
func generateID(timestamp time.Time, index int) uint64 {
	return uint64(timestamp.UnixNano())/1000 + uint64(index)
}

// SaveTextMessages saves text messages to the text_messages table
func (c *ClickHouseDB) SaveTextMessages(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO text_messages
		(id, nickname, display_name, user_id, message, channel, timestamp, color, badges, bits_amount, raw_message)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for i, msg := range messages {
		badges := joinStrings(msg.Badges, ",")

		_, err = stmt.Exec(
			generateID(msg.Timestamp, i),
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
func (c *ClickHouseDB) SaveSubscriptions(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO subscriptions
		(id, nickname, display_name, user_id, channel, timestamp, message_type, sub_plan, sub_plan_name,
		 months, cumulative_months, streak_months, is_gift, gifter_name, gifter_id, target_user,
		 system_message, user_message, raw_message)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for i, msg := range messages {
		isGift := uint8(0)
		if msg.IsGift {
			isGift = 1
		}

		_, err = stmt.Exec(
			generateID(msg.Timestamp, i),
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
func (c *ClickHouseDB) SaveBans(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO bans
		(id, channel, timestamp, message_type, target_user, ban_duration, ban_reason, raw_message)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for i, msg := range messages {
		_, err = stmt.Exec(
			generateID(msg.Timestamp, i),
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
func (c *ClickHouseDB) SaveDeletedMessages(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO deleted_messages
		(id, nickname, channel, timestamp, message, target_message_id, raw_message)
		VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for i, msg := range messages {
		_, err = stmt.Exec(
			generateID(msg.Timestamp, i),
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
func (c *ClickHouseDB) SaveRaids(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO raids
		(id, channel, timestamp, raider_name, viewer_count, system_message, raw_message)
		VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for i, msg := range messages {
		_, err = stmt.Exec(
			generateID(msg.Timestamp, i),
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
func (c *ClickHouseDB) SaveBits(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO bits
		(id, nickname, display_name, user_id, channel, timestamp, message_type, bits_amount, message, raw_message)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for i, msg := range messages {
		_, err = stmt.Exec(
			generateID(msg.Timestamp, i),
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
func (c *ClickHouseDB) SaveNotices(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO notices
		(id, channel, timestamp, message_type, notice_message_id, message, system_message, raw_message)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for i, msg := range messages {
		_, err = stmt.Exec(
			generateID(msg.Timestamp, i),
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
func (c *ClickHouseDB) SaveHosts(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO hosts
		(id, channel, timestamp, target_user, viewer_count, raw_message)
		VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for i, msg := range messages {
		_, err = stmt.Exec(
			generateID(msg.Timestamp, i),
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
func (c *ClickHouseDB) SaveOther(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO other_messages
		(id, channel, timestamp, message_type, nickname, message, raw_message)
		VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for i, msg := range messages {
		_, err = stmt.Exec(
			generateID(msg.Timestamp, i),
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
