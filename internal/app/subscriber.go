package app

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"poglytics-scraper/internal/config"
	"poglytics-scraper/internal/db"
	"poglytics-scraper/internal/metrics"
	"poglytics-scraper/internal/twitch"
	"poglytics-scraper/internal/util"
)

func NewSubscriber() *Subscriber {
	// Random nickname
	id := util.GenerateRandomString(5, "letters")
	nickname := "justinfan" + util.GenerateRandomString(10, "digits")

	ctx, cancel := context.WithCancel(context.Background())

	subscriber := &Subscriber{
		ID:                id,
		Server:            "irc.chat.twitch.tv",
		Token:             "oauth",
		Nickname:          nickname,
		Port:              6667,
		Channels:          []string{},
		IsConnected:       false,
		NMessages:         0,
		NChannels:         0,
		ListChannels:      make([]string, 0),
		messageChan:       make(chan *ChatMessage, 1000000),
		ctx:               ctx,
		cancel:            cancel,
		connectedChannels: make(map[string]bool),
		scanInterval:      10 * time.Minute, // Rescan interval for channel discovery
	}

	// Load environment configuration
	env, err := config.LoadEnv()
	if err != nil {
		slog.Warn("Failed to load environment", "error", err)
	}

	// Get database configuration
	dbConfig := config.GetDBConfig(env)

	// Create database configuration
	dbCfg := &db.DBConfig{
		Type:     dbConfig["type"],
		Host:     dbConfig["host"],
		Port:     dbConfig["port"],
		User:     dbConfig["user"],
		Password: dbConfig["password"],
		Database: dbConfig["database"],
		SSLMode:  dbConfig["sslmode"],
		Path:     dbConfig["path"],
	}

	// Initialize database
	database, err := db.NewDatabase(dbCfg)
	if err != nil {
		slog.Error("Failed to initialize database", "error", err)
		os.Exit(1)
	}

	slog.Info("Database initialized successfully", "type", dbCfg.Type)

	subscriber.DB = database

	go subscriber.dbWorkerBatch()

	return subscriber
}

// NewSubscriberWithDB creates a new subscriber with a shared database and Twitch client
// Used by ConnectionPool to create multiple subscribers sharing the same resources
func NewSubscriberWithDB(database DatabaseInterface, twitchClient *twitch.Client) *Subscriber {
	// Random nickname for each connection
	id := util.GenerateRandomString(5, "letters")
	nickname := "justinfan" + util.GenerateRandomString(10, "digits")

	ctx, cancel := context.WithCancel(context.Background())

	subscriber := &Subscriber{
		ID:                id,
		Server:            "irc.chat.twitch.tv",
		Token:             "oauth",
		Nickname:          nickname,
		Port:              6667,
		Channels:          []string{},
		IsConnected:       false,
		NMessages:         0,
		NChannels:         0,
		ListChannels:      make([]string, 0),
		messageChan:       make(chan *ChatMessage, 50000), // Larger buffer for pooled connections
		ctx:               ctx,
		cancel:            cancel,
		connectedChannels: make(map[string]bool),
		scanInterval:      10 * time.Minute,
		DB:                database,     // Use shared database
		TwitchClient:      twitchClient, // Use shared Twitch client
	}

	// Start the database worker for this connection
	go subscriber.dbWorkerBatch()

	return subscriber
}

// NewSubscriberWithSharedChannel creates a subscriber that uses a shared message channel (for connection pool)
func NewSubscriberWithSharedChannel(database DatabaseInterface, twitchClient *twitch.Client, sharedChan chan *ChatMessage) *Subscriber {
	// Random nickname for each connection
	id := util.GenerateRandomString(5, "letters")
	nickname := "justinfan" + util.GenerateRandomString(10, "digits")

	ctx, cancel := context.WithCancel(context.Background())

	subscriber := &Subscriber{
		ID:                id,
		Server:            "irc.chat.twitch.tv",
		Token:             "oauth",
		Nickname:          nickname,
		Port:              6667,
		Channels:          []string{},
		IsConnected:       false,
		NMessages:         0,
		NChannels:         0,
		ListChannels:      make([]string, 0),
		messageChan:       sharedChan, // Use shared channel - NO local dbWorker!
		ctx:               ctx,
		cancel:            cancel,
		connectedChannels: make(map[string]bool),
		scanInterval:      10 * time.Minute,
		DB:                database,
		TwitchClient:      twitchClient,
	}

	// Do NOT start dbWorkerBatch - pool handles DB writes centrally
	return subscriber
}

func (s *Subscriber) send(message string) error {
	if s.Connection == nil {
		return fmt.Errorf("connection is nil")
	}

	_, err := s.Connection.Write([]byte(message + "\r\n"))
	return err
}

func (s *Subscriber) connect() error {
	exp := 0
	connected := false

	for !connected {
		conn, err := net.Dial("tcp", s.Server+":"+strconv.Itoa(s.Port))
		if err != nil {
			slog.Info("Connection failed, retrying", "id", s.ID, "retry_in_seconds", 1<<exp)
			time.Sleep(time.Duration(1<<exp) * time.Second)
			exp++
			continue
		}

		s.Connection = conn
		s.Reader = bufio.NewReaderSize(conn, 2*1024*1024) // 2MB buffer instead of default 4KB
		s.IsConnected = true
		metrics.UpdateConnectionStatus(s.ID, true)
		slog.Info("Connected to Twitch IRC", "id", s.ID)
		connected = true
	}
	// Request IRC capabilities for tags, commands, and membership
	// This enables us to receive:
	// - tags: IRC v3 message tags with metadata
	// - commands: CLEARCHAT, CLEARMSG, HOSTTARGET, NOTICE, RECONNECT, ROOMSTATE, USERNOTICE, USERSTATE
	// - membership: JOIN, PART, MODE messages
	if err := s.send("CAP REQ :twitch.tv/tags twitch.tv/commands"); err != nil {
		return err
	}

	// Send authentication
	if err := s.send(fmt.Sprintf("PASS %s", s.Token)); err != nil {
		return err
	}

	if err := s.send(fmt.Sprintf("NICK %s", s.Nickname)); err != nil {
		return err
	}

	return nil
}

// parseIRCMessage is the main parser that routes to specific message type parsers
func (s *Subscriber) parseIRCMessage(rawMessage string) *ChatMessage {
	trimmed := strings.TrimSpace(rawMessage)
	if trimmed == "" {
		return nil
	}

	// Parse IRC message format: [@tags] [:prefix] COMMAND [params] [:trailing]
	var tags map[string]string
	remainder := trimmed

	// Extract tags if present
	if strings.HasPrefix(remainder, "@") {
		spaceIdx := strings.Index(remainder, " ")
		if spaceIdx > 0 {
			tags = parseTags(remainder[0:spaceIdx])
			remainder = strings.TrimSpace(remainder[spaceIdx+1:])
		}
	}

	// Now parse the command
	parts := strings.Fields(remainder)
	if len(parts) < 2 {
		return nil
	}

	// Find command (skip prefix if present)
	commandIdx := 0
	if strings.HasPrefix(parts[0], ":") {
		commandIdx = 1
	}

	if commandIdx >= len(parts) {
		return nil
	}

	command := parts[commandIdx]

	// Route to appropriate parser based on command
	switch command {
	case "PRIVMSG":
		return parsePRIVMSG(rawMessage, tags, parts, commandIdx)
	case "CLEARCHAT":
		return parseCLEARCHAT(rawMessage, tags, parts, commandIdx)
	case "CLEARMSG":
		return parseCLEARMSG(rawMessage, tags, parts, commandIdx)
	case "USERNOTICE":
		return parseUSERNOTICE(rawMessage, tags, parts, commandIdx)
	case "NOTICE":
		return parseNOTICE(rawMessage, tags, parts, commandIdx)
	case "HOSTTARGET":
		return parseHOSTTARGET(rawMessage, tags, parts, commandIdx)
	default:
		// Log other message types for debugging
		return &ChatMessage{
			MessageType: "other",
			Timestamp:   time.Now(),
			RawMessage:  rawMessage,
		}
	}
}

// logDisconnectionEvent logs disconnection events to a file for debugging
func (s *Subscriber) logDisconnectionEvent(eventType, channel, message string) {
	logFile := "disconnections.log"

	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		slog.Error("Error opening log file", "error", err)
		return
	}
	defer f.Close()

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	logEntry := fmt.Sprintf("[%s] %s | Channel: %s | Message: %s\n",
		timestamp, eventType, channel, message)

	if _, err := f.WriteString(logEntry); err != nil {
		slog.Error("Error writing to log file", "error", err)
		return
	}

	// Also print to console for immediate visibility
	slog.Info("Disconnection event", "type", eventType, "channel", channel, "message", message)
}

// shutdown gracefully shuts down the subscriber
func (s *Subscriber) Shutdown() {
	slog.Info("Shutting down gracefully", "id", s.ID)
	s.cancel()                  // Cancel context to stop all goroutines
	time.Sleep(2 * time.Second) // Give time for goroutines to finish
	if s.DB != nil {
		// Wait a bit for any pending database operations
		time.Sleep(100 * time.Millisecond)
		s.DB.Close()
	}
}

// saveChatMessageBatch saves multiple messages using the database interface
func (s *Subscriber) saveChatMessageBatch(messages []*ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	// Convert app.ChatMessage to db.ChatMessage
	dbMessages := make([]*db.ChatMessage, len(messages))
	for i, msg := range messages {
		dbMessages[i] = &db.ChatMessage{
			Nickname:         msg.Nickname,
			Message:          msg.Message,
			Channel:          msg.Channel,
			Timestamp:        msg.Timestamp,
			MessageType:      msg.MessageType,
			Tags:             msg.Tags,
			UserID:           msg.UserID,
			DisplayName:      msg.DisplayName,
			Color:            msg.Color,
			Badges:           msg.Badges,
			SubPlan:          msg.SubPlan,
			SubPlanName:      msg.SubPlanName,
			Months:           msg.Months,
			CumulativeMonths: msg.CumulativeMonths,
			StreakMonths:     msg.StreakMonths,
			IsGift:           msg.IsGift,
			GifterName:       msg.GifterName,
			GifterID:         msg.GifterID,
			BanDuration:      msg.BanDuration,
			BanReason:        msg.BanReason,
			TargetUser:       msg.TargetUser,
			TargetMessageID:  msg.TargetMessageID,
			RaiderName:       msg.RaiderName,
			ViewerCount:      msg.ViewerCount,
			BitsAmount:       msg.BitsAmount,
			NoticeMessageID:  msg.NoticeMessageID,
			SystemMessage:    msg.SystemMessage,
			RawMessage:       msg.RawMessage,
		}
	}

	return s.DB.SaveChatMessageBatch(dbMessages)
}

// dbWorkerBatch processes chat messages in batches for better database performance
func (s *Subscriber) dbWorkerBatch() {
	ticker := time.NewTicker(1 * time.Second) // Batch every second
	defer ticker.Stop()

	var batch []*ChatMessage
	const maxBatchSize = 100

	for {
		select {
		case <-s.ctx.Done():
			// Process remaining messages before exiting
			if len(batch) > 0 {
				s.saveChatMessageBatch(batch)
			}
			return

		case msg := <-s.messageChan:
			batch = append(batch, msg)
			if len(batch) >= maxBatchSize {
				s.dbMutex.Lock()
				if err := s.saveChatMessageBatch(batch); err != nil {
					slog.Error("Error saving batch to DB", "error", err, "id", s.ID)
				}
				s.dbMutex.Unlock()
				batch = batch[:0] // Clear batch
			}

		case <-ticker.C:
			if len(batch) > 0 {
				s.dbMutex.Lock()
				if err := s.saveChatMessageBatch(batch); err != nil {
					slog.Error("Error saving batch to DB", "error", err, "id", s.ID)
				}
				s.dbMutex.Unlock()
				batch = batch[:0] // Clear batch
			}
		}
	}
}

// readChat reads and processes chat messages from the IRC connection
func (s *Subscriber) readChat() error {
	// Wait for connection
	for !s.IsConnected {
		time.Sleep(100 * time.Millisecond)
	}

	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		default:
			// Use robust reading method to handle buffer overflows
			// REDUCED timeout from 120s to 5s to prevent blocking and message bursts
			// Long timeouts cause messages to accumulate during JOIN operations
			data, err := s.readLineWithTimeoutRobust(20 * time.Second)
			if err != nil {
				// If we get a buffer error that we can't handle, try to continue
				if strings.Contains(err.Error(), "buffer") || strings.Contains(err.Error(), "slice bounds") {
					slog.Error("Unrecoverable buffer error in readChat, skipping", "id", s.ID, "error", err)
					metrics.RecordParseError(s.ID, "buffer_error")
					// Reset reader and continue
					s.Reader = bufio.NewReaderSize(s.Connection, 4*1024*1024)
					continue
				}
				if strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "i/o timeout") {
					metrics.RecordReadTimeout(s.ID)
					// Timeout is OK during low activity - just continue
					continue
				}
				return fmt.Errorf("error reading from connection: %v", err)
			}

			s.NMessages++

			// Parse chat message and send to async processing
			if chatMsg := s.parseIRCMessage(data); chatMsg != nil {
				// Record message metric
				metrics.RecordMessage(s.ID)

				// Record parsed message type metric
				if chatMsg.MessageType != "" {
					metrics.RecordParsedMessage(s.ID, chatMsg.MessageType)
				}

				// Try to send to message channel (non-blocking)
				select {
				case s.messageChan <- chatMsg:
					// Message sent successfully
				default:
					// Channel full - log periodically
					if s.NMessages%1000 == 0 {
						slog.Warn("Message channel full, dropping messages", "id", s.ID, "len", len(s.messageChan), "cap", cap(s.messageChan))
					}
					metrics.RecordParseError(s.ID, "channel_full")
				}
			}

			// PING PONG
			if strings.HasPrefix(data, "PING") {
				pingStart := time.Now()
				parts := strings.Split(strings.TrimSpace(data), " ")
				if len(parts) > 1 {
					pongMsg := fmt.Sprintf("PONG %s", parts[len(parts)-1])
					if err := s.send(pongMsg); err != nil {
						slog.Error("Error sending PONG", "id", s.ID, "error", err)
					} else {
						// Record ping/pong latency
						latency := time.Since(pingStart).Seconds()
						metrics.RecordPingPong(s.ID, latency)
						metrics.RecordIRCMessage(s.ID, "PING")
					}
				}
			}
		}
	}
}

// infiniteReadChat continuously reads chat messages with error recovery
func (s *Subscriber) infiniteReadChat() {
	nFailure := 0

	for !s.IsConnected {
		time.Sleep(500 * time.Millisecond)
	}

	slog.Info("Chat reader started", "id", s.ID)

	for {
		err := s.readChat()
		if err != nil {
			nFailure++
			slog.Error("Error reading chat", "id", s.ID, "error", err)

			// Log the disconnection to file
			s.logDisconnectionEvent("CONNECTION_ERROR", "N/A", fmt.Sprintf("Connection error: %v", err))

			// Check if it's an EOF or connection closed error
			isEOF := strings.Contains(err.Error(), "EOF") ||
				strings.Contains(err.Error(), "connection reset") ||
				strings.Contains(err.Error(), "broken pipe") ||
				strings.Contains(err.Error(), "use of closed")

			// If we have EOF or connection errors, return to let pool handle reconnection
			// (If ID starts with "conn-", we're in a pool)
			if isEOF {
				slog.Info("Connection closed by server (EOF), returning for pool to handle reconnection", "id", s.ID)
				// Need to clear connected channels so they'll be rejoined after pool reconnects
				s.channelsMutex.Lock()
				for channel := range s.connectedChannels {
					delete(s.connectedChannels, channel)
				}
				s.channelsMutex.Unlock()
				return // Let pool handle reconnection
			}

			time.Sleep(5 * time.Second)
			continue
		}
	}
}

// joinNewChannels joins channels that are not currently connected
func (s *Subscriber) joinNewChannels(newChannels []string) {
	s.channelsMutex.Lock()
	defer s.channelsMutex.Unlock()

	var channelsToJoin []string

	for _, channel := range newChannels {
		if !s.connectedChannels[channel] {
			channelsToJoin = append(channelsToJoin, channel)
		}
	}

	if len(channelsToJoin) == 0 {
		return
	}

	slog.Info("Joining new channels", "id", s.ID, "count", len(channelsToJoin))

	// Join channels with rate limiting to avoid Twitch IRC limits
	// Twitch allows ~50 JOIN commands per 15 seconds (conservative estimate)
	batchSize := 50                // Join 50 channels at a time
	batchDelay := 10 * time.Second // Wait 15 seconds between batches

	for i := 0; i < len(channelsToJoin); i += batchSize {
		end := i + batchSize
		if end > len(channelsToJoin) {
			end = len(channelsToJoin)
		}

		batchStart := time.Now()
		successCount := 0

		currentBatch := channelsToJoin[i:end]
		innerBatchSize := 5
		for j := 0; j < len(currentBatch); j += innerBatchSize {
			innerEnd := j + innerBatchSize
			if innerEnd > len(currentBatch) {
				innerEnd = len(currentBatch)
			}
			batch := currentBatch[j:innerEnd]

			// Join the inner batch of channels
			// join #channel1, #channel2, ...
			if err := s.joinChannel(batch); err != nil {
				slog.Error("Error joining batch", "id", s.ID, "batch", batch, "error", err)
				continue
			}

			for _, channel := range batch {
				s.connectedChannels[channel] = true
				successCount++
			}
			time.Sleep(1000 * time.Millisecond) // Minimal delay between batches
		}

		slog.Info("Joined channels in batch", "id", s.ID, "joined", successCount, "total_in_batch", end-i, "batch", (i/batchSize)+1, "total_batches", (len(channelsToJoin)+batchSize-1)/batchSize, "duration", time.Since(batchStart).Seconds())

		// Wait for the rest of the batch period if we finished early
		if end < len(channelsToJoin) {
			elapsed := time.Since(batchStart)
			if elapsed < batchDelay {
				remaining := batchDelay - elapsed
				slog.Info("Rate limiting: waiting before next batch", "id", s.ID, "wait_seconds", remaining.Seconds())
				time.Sleep(remaining)
			}
		}
	}

	slog.Info("Finished joining all channels", "id", s.ID)
}

// joinChannelBatch joins multiple channels in a single JOIN command
func (s *Subscriber) joinChannel(channels []string) error {
	if len(channels) == 0 {
		return nil
	}

	// Create comma-separated channel list
	channelList := strings.Join(channels, ",")

	if err := s.send(fmt.Sprintf("JOIN %s", channelList)); err != nil {
		// Record failure for all channels in batch
		for range channels {
			metrics.RecordChannelJoin(s.ID, false)
		}
		return err
	}

	// Record success for all channels
	for _, channel := range channels {
		s.NChannels++
		s.ListChannels = append(s.ListChannels, channel)
		metrics.RecordChannelJoin(s.ID, true)
	}

	return nil
}
