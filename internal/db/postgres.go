package db

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/lib/pq"
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

	// Create tables for different message types
	createTablesSQL := `
	-- Text messages table
	CREATE TABLE IF NOT EXISTS text_messages (
		id SERIAL PRIMARY KEY,
		nickname TEXT NOT NULL,
		display_name TEXT,
		user_id TEXT,
		message TEXT NOT NULL,
		channel TEXT NOT NULL,
		timestamp TIMESTAMP NOT NULL,
		color TEXT,
		badges TEXT,
		bits_amount INTEGER DEFAULT 0,
		raw_message TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_text_messages_timestamp ON text_messages(timestamp);
	CREATE INDEX IF NOT EXISTS idx_text_messages_channel ON text_messages(channel);

	-- Subscriptions table
	CREATE TABLE IF NOT EXISTS subscriptions (
		id SERIAL PRIMARY KEY,
		nickname TEXT NOT NULL,
		display_name TEXT,
		user_id TEXT,
		channel TEXT NOT NULL,
		timestamp TIMESTAMP NOT NULL,
		message_type TEXT NOT NULL,
		sub_plan TEXT,
		sub_plan_name TEXT,
		months INTEGER DEFAULT 0,
		cumulative_months INTEGER DEFAULT 0,
		streak_months INTEGER DEFAULT 0,
		is_gift BOOLEAN DEFAULT FALSE,
		gifter_name TEXT,
		gifter_id TEXT,
		target_user TEXT,
		system_message TEXT,
		user_message TEXT,
		raw_message TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_subscriptions_timestamp ON subscriptions(timestamp);
	CREATE INDEX IF NOT EXISTS idx_subscriptions_channel ON subscriptions(channel);

	-- Bans and timeouts table
	CREATE TABLE IF NOT EXISTS bans (
		id SERIAL PRIMARY KEY,
		channel TEXT NOT NULL,
		timestamp TIMESTAMP NOT NULL,
		message_type TEXT NOT NULL,
		target_user TEXT NOT NULL,
		ban_duration INTEGER DEFAULT 0,
		ban_reason TEXT,
		raw_message TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_bans_timestamp ON bans(timestamp);
	CREATE INDEX IF NOT EXISTS idx_bans_channel ON bans(channel);

	-- Deleted messages table
	CREATE TABLE IF NOT EXISTS deleted_messages (
		id SERIAL PRIMARY KEY,
		nickname TEXT,
		channel TEXT NOT NULL,
		timestamp TIMESTAMP NOT NULL,
		message TEXT,
		target_message_id TEXT,
		raw_message TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_deleted_messages_timestamp ON deleted_messages(timestamp);
	CREATE INDEX IF NOT EXISTS idx_deleted_messages_channel ON deleted_messages(channel);

	-- Raids table
	CREATE TABLE IF NOT EXISTS raids (
		id SERIAL PRIMARY KEY,
		channel TEXT NOT NULL,
		timestamp TIMESTAMP NOT NULL,
		raider_name TEXT NOT NULL,
		viewer_count INTEGER DEFAULT 0,
		system_message TEXT,
		raw_message TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_raids_timestamp ON raids(timestamp);
	CREATE INDEX IF NOT EXISTS idx_raids_channel ON raids(channel);

	-- Bits/cheers table
	CREATE TABLE IF NOT EXISTS bits (
		id SERIAL PRIMARY KEY,
		nickname TEXT NOT NULL,
		display_name TEXT,
		user_id TEXT,
		channel TEXT NOT NULL,
		timestamp TIMESTAMP NOT NULL,
		message_type TEXT NOT NULL,
		bits_amount INTEGER DEFAULT 0,
		message TEXT,
		raw_message TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_bits_timestamp ON bits(timestamp);
	CREATE INDEX IF NOT EXISTS idx_bits_channel ON bits(channel);

	-- Notices table
	CREATE TABLE IF NOT EXISTS notices (
		id SERIAL PRIMARY KEY,
		channel TEXT NOT NULL,
		timestamp TIMESTAMP NOT NULL,
		message_type TEXT NOT NULL,
		notice_message_id TEXT,
		message TEXT,
		system_message TEXT,
		raw_message TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_notices_timestamp ON notices(timestamp);
	CREATE INDEX IF NOT EXISTS idx_notices_channel ON notices(channel);

	-- Hosts table
	CREATE TABLE IF NOT EXISTS hosts (
		id SERIAL PRIMARY KEY,
		channel TEXT NOT NULL,
		timestamp TIMESTAMP NOT NULL,
		target_user TEXT,
		viewer_count INTEGER DEFAULT 0,
		raw_message TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_hosts_timestamp ON hosts(timestamp);
	CREATE INDEX IF NOT EXISTS idx_hosts_channel ON hosts(channel);

	-- Other messages table (for message types we don't specifically handle)
	CREATE TABLE IF NOT EXISTS other_messages (
		id SERIAL PRIMARY KEY,
		channel TEXT NOT NULL,
		timestamp TIMESTAMP NOT NULL,
		message_type TEXT NOT NULL,
		nickname TEXT,
		message TEXT,
		raw_message TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_other_messages_timestamp ON other_messages(timestamp);
	CREATE INDEX IF NOT EXISTS idx_other_messages_channel ON other_messages(channel);

	-- Stream snapshots table
	CREATE TABLE IF NOT EXISTS stream_snapshots (
		id SERIAL PRIMARY KEY,
		stream_id TEXT NOT NULL,
		user_id TEXT NOT NULL,
		user_login TEXT NOT NULL,
		user_name TEXT NOT NULL,
		game_id TEXT,
		game_name TEXT,
		type TEXT,
		title TEXT,
		viewer_count INTEGER DEFAULT 0,
		started_at TIMESTAMP,
		language TEXT,
		thumbnail_url TEXT,
		tags TEXT[],
		tag_ids TEXT[],
		is_mature BOOLEAN DEFAULT FALSE,
		snapshot_time TIMESTAMP NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_stream_snapshots_user_login ON stream_snapshots(user_login);
	CREATE INDEX IF NOT EXISTS idx_stream_snapshots_snapshot_time ON stream_snapshots(snapshot_time);
	CREATE INDEX IF NOT EXISTS idx_stream_snapshots_game_id ON stream_snapshots(game_id);

	-- Legacy table (keep for backward compatibility)
	CREATE TABLE IF NOT EXISTS chat_messages (
		id SERIAL PRIMARY KEY,
		nickname TEXT NOT NULL,
		message TEXT NOT NULL,
		channel TEXT NOT NULL,
		timestamp TIMESTAMP NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_chat_messages_timestamp ON chat_messages(timestamp);
	CREATE INDEX IF NOT EXISTS idx_chat_messages_channel ON chat_messages(channel);`

	_, err = p.db.Exec(createTablesSQL)
	if err != nil {
		p.db.Close()
		return fmt.Errorf("failed to create tables: %w", err)
	}

	return nil
}

// SaveChatMessageBatch saves multiple messages in a single transaction for better performance
// Routes messages to appropriate tables based on MessageType
func (p *PostgresDB) SaveChatMessageBatch(messages interface{}) error {
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
		if err := p.SaveTextMessages(textMessages); err != nil {
			return fmt.Errorf("failed to save text messages: %w", err)
		}
	}
	if len(subscriptions) > 0 {
		if err := p.SaveSubscriptions(subscriptions); err != nil {
			return fmt.Errorf("failed to save subscriptions: %w", err)
		}
	}
	if len(bans) > 0 {
		if err := p.SaveBans(bans); err != nil {
			return fmt.Errorf("failed to save bans: %w", err)
		}
	}
	if len(deletedMessages) > 0 {
		if err := p.SaveDeletedMessages(deletedMessages); err != nil {
			return fmt.Errorf("failed to save deleted messages: %w", err)
		}
	}
	if len(raids) > 0 {
		if err := p.SaveRaids(raids); err != nil {
			return fmt.Errorf("failed to save raids: %w", err)
		}
	}
	if len(bits) > 0 {
		if err := p.SaveBits(bits); err != nil {
			return fmt.Errorf("failed to save bits: %w", err)
		}
	}
	if len(notices) > 0 {
		if err := p.SaveNotices(notices); err != nil {
			return fmt.Errorf("failed to save notices: %w", err)
		}
	}
	if len(hosts) > 0 {
		if err := p.SaveHosts(hosts); err != nil {
			return fmt.Errorf("failed to save hosts: %w", err)
		}
	}
	if len(other) > 0 {
		if err := p.SaveOther(other); err != nil {
			return fmt.Errorf("failed to save other messages: %w", err)
		}
	}

	return nil
}

// SaveTextMessages saves text messages to the text_messages table
func (p *PostgresDB) SaveTextMessages(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO text_messages
		(nickname, display_name, user_id, message, channel, timestamp, color, badges, bits_amount, raw_message)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`)
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
func (p *PostgresDB) SaveSubscriptions(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO subscriptions
		(nickname, display_name, user_id, channel, timestamp, message_type, sub_plan, sub_plan_name,
		 months, cumulative_months, streak_months, is_gift, gifter_name, gifter_id, target_user,
		 system_message, user_message, raw_message)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`)
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
			msg.SubPlan,
			msg.SubPlanName,
			msg.Months,
			msg.CumulativeMonths,
			msg.StreakMonths,
			msg.IsGift,
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
func (p *PostgresDB) SaveBans(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO bans
		(channel, timestamp, message_type, target_user, ban_duration, ban_reason, raw_message)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`)
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
func (p *PostgresDB) SaveDeletedMessages(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO deleted_messages
		(nickname, channel, timestamp, message, target_message_id, raw_message)
		VALUES ($1, $2, $3, $4, $5, $6)`)
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
func (p *PostgresDB) SaveRaids(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO raids
		(channel, timestamp, raider_name, viewer_count, system_message, raw_message)
		VALUES ($1, $2, $3, $4, $5, $6)`)
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
func (p *PostgresDB) SaveBits(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO bits
		(nickname, display_name, user_id, channel, timestamp, message_type, bits_amount, message, raw_message)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`)
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
func (p *PostgresDB) SaveNotices(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO notices
		(channel, timestamp, message_type, notice_message_id, message, system_message, raw_message)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`)
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
func (p *PostgresDB) SaveHosts(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO hosts
		(channel, timestamp, target_user, viewer_count, raw_message)
		VALUES ($1, $2, $3, $4, $5)`)
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
func (p *PostgresDB) SaveOther(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO other_messages
		(channel, timestamp, message_type, nickname, message, raw_message)
		VALUES ($1, $2, $3, $4, $5, $6)`)
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

// SaveStreamSnapshots saves stream snapshots to the stream_snapshots table
func (p *PostgresDB) SaveStreamSnapshots(snapshots []*StreamSnapshot) error {
	if len(snapshots) == 0 {
		return nil
	}

	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO stream_snapshots
		(stream_id, user_id, user_login, user_name, game_id, game_name, type, title,
		viewer_count, started_at, language, thumbnail_url, tags, tag_ids, is_mature, snapshot_time)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, snapshot := range snapshots {
		_, err = stmt.Exec(
			snapshot.ID,
			snapshot.UserID,
			snapshot.UserLogin,
			snapshot.UserName,
			snapshot.GameID,
			snapshot.GameName,
			snapshot.Type,
			snapshot.Title,
			snapshot.ViewerCount,
			snapshot.StartedAt,
			snapshot.Language,
			snapshot.ThumbnailURL,
			pq.Array(snapshot.Tags),    // Postgres supports array types directly
			pq.Array(snapshot.TagIDs),  // Postgres supports array types directly
			snapshot.IsMature,
			snapshot.SnapshotTime,
		)
		if err != nil {
			return fmt.Errorf("failed to insert stream snapshot: %w", err)
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
