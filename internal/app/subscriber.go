package app

import (
	"bufio"
	"context"
	"fmt"
	"log"
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
		log.Printf("Warning: Failed to load environment: %v", err)
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
		log.Fatalf("Failed to initialize database: %v", err)
	}

	fmt.Printf("Database initialized successfully (type: %s)\n", dbCfg.Type)

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
			fmt.Printf("Connection failed, retrying in %d second(s)...\n", 1<<exp)
			time.Sleep(time.Duration(1<<exp) * time.Second)
			exp++
			continue
		}

		s.Connection = conn
		s.Reader = bufio.NewReaderSize(conn, 2*1024*1024) // 2MB buffer instead of default 4KB
		s.IsConnected = true
		metrics.UpdateConnectionStatus(s.ID, true)
		fmt.Println("Connected to Twitch IRC")
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

	// Read authentication response with error handling for large responses
	// response, err := s.readLineWithTimeout(5 * time.Second)
	// if err != nil {
	// 	return err
	// }

	// fmt.Print("Auth response:", response)

	// if !strings.HasPrefix(response, ":tmi.twitch.tv 001") {
	// 	return fmt.Errorf("twitch did not accept the username-oauth combination")
	// }

	return nil
}

// joinChannel joins a specific channel
func (s *Subscriber) joinChannel(channel string) error {
	if err := s.send(fmt.Sprintf("JOIN %s", channel)); err != nil {
		metrics.RecordChannelJoin(s.ID, false)
		return err
	}

	s.NChannels++
	s.ListChannels = append(s.ListChannels, channel)
	metrics.RecordChannelJoin(s.ID, true)
	return nil
}

// joinChannelBatch joins multiple channels in a single JOIN command
func (s *Subscriber) joinChannelBatch(channels []string) error {
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

// parseTags extracts IRC tags from a message
// Tags format: @key1=value1;key2=value2;key3=value3
func parseTags(tagString string) map[string]string {
	tags := make(map[string]string)
	if tagString == "" {
		return tags
	}

	// Remove @ prefix if present
	tagString = strings.TrimPrefix(tagString, "@")

	pairs := strings.Split(tagString, ";")
	for _, pair := range pairs {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) == 2 {
			// Unescape IRC tag values according to IRCv3 spec
			value := parts[1]
			value = strings.ReplaceAll(value, "\\s", " ")
			value = strings.ReplaceAll(value, "\\:", ";")
			value = strings.ReplaceAll(value, "\\n", "\n")
			value = strings.ReplaceAll(value, "\\r", "\r")
			value = strings.ReplaceAll(value, "\\\\", "\\")
			tags[parts[0]] = value
		}
	}
	return tags
}

// parseIRCMessage is the main parser that routes to specific message type parsers
func parseIRCMessage(rawMessage string) *ChatMessage {
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

// parsePRIVMSG parses regular chat messages
func parsePRIVMSG(rawMessage string, tags map[string]string, parts []string, commandIdx int) *ChatMessage {
	if commandIdx+2 >= len(parts) {
		return nil
	}

	channel := parts[commandIdx+1]

	// Extract message (everything after " :" that follows the channel name)
	// Format: :user!user@user.tmi.twitch.tv PRIVMSG #channel :message
	// We need to find " :" that appears AFTER the channel name
	channelEnd := strings.Index(rawMessage, channel)
	if channelEnd == -1 {
		return nil
	}
	// Look for " :" after the channel name
	colonIdx := strings.Index(rawMessage[channelEnd:], " :")
	if colonIdx == -1 {
		return nil
	}
	message := rawMessage[channelEnd+colonIdx+2:]

	msg := &ChatMessage{
		MessageType: "text_message",
		Channel:     channel,
		Message:     message,
		Timestamp:   time.Now(),
		Tags:        tags,
		RawMessage:  rawMessage,
	}

	// Extract common fields from tags
	if tags != nil {
		msg.UserID = tags["user-id"]
		msg.DisplayName = tags["display-name"]
		msg.Color = tags["color"]

		// Parse badges
		if badgeStr := tags["badges"]; badgeStr != "" {
			badges := strings.Split(badgeStr, ",")
			msg.Badges = badges
		}

		// Bits amount
		if bitsStr := tags["bits"]; bitsStr != "" {
			if bits, err := strconv.Atoi(bitsStr); err == nil {
				msg.BitsAmount = bits
			}
		}
	}

	// Extract nickname from prefix
	if commandIdx > 0 && strings.HasPrefix(parts[0], ":") {
		prefix := parts[0][1:]
		if bangIdx := strings.Index(prefix, "!"); bangIdx > 0 {
			msg.Nickname = prefix[:bangIdx]
		}
	}

	return msg
}

// parseCLEARCHAT parses ban and timeout messages
func parseCLEARCHAT(rawMessage string, tags map[string]string, parts []string, commandIdx int) *ChatMessage {
	if commandIdx+1 >= len(parts) {
		return nil
	}

	channel := parts[commandIdx+1]

	msg := &ChatMessage{
		MessageType: "ban",
		Channel:     channel,
		Timestamp:   time.Now(),
		Tags:        tags,
		RawMessage:  rawMessage,
	}

	// Check if there's a username (ban/timeout) or if it's a full chat clear
	if commandIdx+2 < len(parts) {
		// Extract target user (everything after the second colon or just the next part)
		if strings.Contains(rawMessage, " :") {
			colonIdx := strings.Index(rawMessage, " :")
			msg.TargetUser = strings.TrimSpace(rawMessage[colonIdx+2:])
		} else if commandIdx+2 < len(parts) {
			msg.TargetUser = parts[commandIdx+2]
		}
	}

	// Extract ban duration from tags (0 = permanent ban)
	if tags != nil {
		if durationStr := tags["ban-duration"]; durationStr != "" {
			if duration, err := strconv.Atoi(durationStr); err == nil {
				msg.BanDuration = duration
				if duration > 0 {
					msg.MessageType = "timeout"
				}
			}
		}
		msg.BanReason = tags["ban-reason"]
	}

	return msg
}

// parseCLEARMSG parses individual message deletion
func parseCLEARMSG(rawMessage string, tags map[string]string, parts []string, commandIdx int) *ChatMessage {
	if commandIdx+1 >= len(parts) {
		return nil
	}

	channel := parts[commandIdx+1]

	// Extract the deleted message text
	var message string
	if strings.Contains(rawMessage, " :") {
		colonIdx := strings.Index(rawMessage, " :")
		message = rawMessage[colonIdx+2:]
	}

	msg := &ChatMessage{
		MessageType: "delete_message",
		Channel:     channel,
		Message:     message,
		Timestamp:   time.Now(),
		Tags:        tags,
		RawMessage:  rawMessage,
	}

	if tags != nil {
		msg.TargetMessageID = tags["target-msg-id"]
		msg.Nickname = tags["login"]
	}

	return msg
}

// parseUSERNOTICE parses subscription, resub, subgift, raid, and ritual messages
func parseUSERNOTICE(rawMessage string, tags map[string]string, parts []string, commandIdx int) *ChatMessage {
	if commandIdx+1 >= len(parts) {
		return nil
	}

	channel := parts[commandIdx+1]

	// Extract user message if present
	var userMessage string
	if strings.Contains(rawMessage, " :") {
		colonIdx := strings.Index(rawMessage, " :")
		userMessage = rawMessage[colonIdx+2:]
	}

	msg := &ChatMessage{
		MessageType: "subscription", // Default, will be refined based on msg-id
		Channel:     channel,
		Message:     userMessage,
		Timestamp:   time.Now(),
		Tags:        tags,
		RawMessage:  rawMessage,
	}

	if tags != nil {
		msg.UserID = tags["user-id"]
		msg.DisplayName = tags["display-name"]
		msg.SystemMessage = tags["system-msg"]

		// Determine specific type based on msg-id
		msgID := tags["msg-id"]
		switch msgID {
		case "sub", "resub":
			msg.MessageType = "subscription"
			msg.SubPlan = tags["msg-param-sub-plan"]
			msg.SubPlanName = tags["msg-param-sub-plan-name"]
			if months := tags["msg-param-cumulative-months"]; months != "" {
				msg.CumulativeMonths, _ = strconv.Atoi(months)
			}
			if months := tags["msg-param-months"]; months != "" {
				msg.Months, _ = strconv.Atoi(months)
			}
			if streak := tags["msg-param-streak-months"]; streak != "" {
				msg.StreakMonths, _ = strconv.Atoi(streak)
			}

		case "subgift", "anonsubgift":
			msg.MessageType = "subscription_gift"
			msg.IsGift = true
			msg.SubPlan = tags["msg-param-sub-plan"]
			msg.SubPlanName = tags["msg-param-sub-plan-name"]
			msg.TargetUser = tags["msg-param-recipient-user-name"]
			msg.GifterName = tags["login"]
			msg.GifterID = tags["user-id"]
			if months := tags["msg-param-gift-months"]; months != "" {
				msg.Months, _ = strconv.Atoi(months)
			}

		case "submysterygift", "anonsubmysterygift":
			msg.MessageType = "mystery_subscription_gift"
			msg.IsGift = true
			msg.SubPlan = tags["msg-param-sub-plan"]
			msg.GifterName = tags["login"]
			msg.GifterID = tags["user-id"]

		case "raid":
			msg.MessageType = "raid"
			msg.RaiderName = tags["msg-param-login"]
			if viewers := tags["msg-param-viewerCount"]; viewers != "" {
				msg.ViewerCount, _ = strconv.Atoi(viewers)
			}

		case "ritual":
			msg.MessageType = "other"
			msg.SystemMessage = tags["system-msg"]

		case "bitsbadgetier":
			msg.MessageType = "bits_badge_tier"
			if threshold := tags["msg-param-threshold"]; threshold != "" {
				msg.BitsAmount, _ = strconv.Atoi(threshold)
			}

		default:
			msg.MessageType = "user_notice"
			msg.NoticeMessageID = msgID
		}
	}

	// Extract nickname from login tag or prefix
	if tags != nil && tags["login"] != "" {
		msg.Nickname = tags["login"]
	} else if commandIdx > 0 && strings.HasPrefix(parts[0], ":") {
		prefix := parts[0][1:]
		if bangIdx := strings.Index(prefix, "!"); bangIdx > 0 {
			msg.Nickname = prefix[:bangIdx]
		}
	}

	return msg
}

// parseNOTICE parses system notices
func parseNOTICE(rawMessage string, tags map[string]string, parts []string, commandIdx int) *ChatMessage {
	if commandIdx+1 >= len(parts) {
		return nil
	}

	channel := parts[commandIdx+1]

	// Extract notice message
	var message string
	if strings.Contains(rawMessage, " :") {
		colonIdx := strings.Index(rawMessage, " :")
		message = rawMessage[colonIdx+2:]
	}

	msg := &ChatMessage{
		MessageType:   "notice",
		Channel:       channel,
		Message:       message,
		SystemMessage: message,
		Timestamp:     time.Now(),
		Tags:          tags,
		RawMessage:    rawMessage,
	}

	if tags != nil {
		msg.NoticeMessageID = tags["msg-id"]
	}

	return msg
}

// parseHOSTTARGET parses host notifications
func parseHOSTTARGET(rawMessage string, tags map[string]string, parts []string, commandIdx int) *ChatMessage {
	if commandIdx+2 >= len(parts) {
		return nil
	}

	channel := parts[commandIdx+1]
	targetInfo := parts[commandIdx+2]

	// Format: "HOSTTARGET #channel :target_channel viewer_count"
	// or "HOSTTARGET #channel :- 0" when hosting ends

	var targetUser string
	var viewerCount int

	if strings.Contains(rawMessage, " :") {
		colonIdx := strings.Index(rawMessage, " :")
		trailing := strings.TrimSpace(rawMessage[colonIdx+2:])
		trailingParts := strings.Fields(trailing)

		if len(trailingParts) > 0 {
			targetUser = trailingParts[0]
			if len(trailingParts) > 1 {
				viewerCount, _ = strconv.Atoi(trailingParts[1])
			}
		}
	} else {
		targetUser = targetInfo
	}

	msg := &ChatMessage{
		MessageType: "host_target",
		Channel:     channel,
		TargetUser:  targetUser,
		ViewerCount: viewerCount,
		Timestamp:   time.Now(),
		Tags:        tags,
		RawMessage:  rawMessage,
	}

	return msg
}

// logRawIRCMessage logs raw IRC messages to type-specific files
func logRawIRCMessage(messageType, rawMessage string) {
	var logFile string

	switch messageType {
	case "text_message":
		logFile = "logs/messages.log"
	case "subscription", "subscription_gift", "mystery_subscription_gift", "resub":
		logFile = "logs/subscriptions.log"
	case "ban", "timeout":
		logFile = "logs/bans.log"
	case "delete_message":
		logFile = "logs/deleted_messages.log"
	case "raid":
		logFile = "logs/raids.log"
	case "bits", "bits_badge_tier":
		logFile = "logs/bits.log"
	case "notice", "user_notice":
		logFile = "logs/notices.log"
	default:
		logFile = "logs/other.log"
	}

	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	f.WriteString(fmt.Sprintf("[%s] %s\n", timestamp, rawMessage))
}

// logParsedMessage logs parsed message details to type-specific files
func logParsedMessage(msg *ChatMessage) {
	if msg == nil {
		return
	}

	var logFile string

	switch msg.MessageType {
	case "text_message":
		return // Don't log parsed text messages to reduce file size
	case "subscription", "subscription_gift", "mystery_subscription_gift", "resub":
		logFile = "logs/subscriptions_parsed.log"
	case "ban", "timeout":
		logFile = "logs/bans_parsed.log"
	case "delete_message":
		logFile = "logs/deleted_messages_parsed.log"
	case "raid":
		logFile = "logs/raids.log" // Reuse same file
	case "bits", "bits_badge_tier":
		logFile = "logs/bits_parsed.log"
	case "notice", "user_notice":
		logFile = "logs/notices_parsed.log"
	default:
		logFile = "logs/other_parsed.log"
	}

	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()

	timestamp := msg.Timestamp.Format("2006-01-02 15:04:05")

	var details string
	switch msg.MessageType {
	case "subscription", "subscription_gift", "mystery_subscription_gift":
		details = fmt.Sprintf("[%s] Type=%s Channel=%s User=%s(%s) Plan=%s Months=%d CumulativeMonths=%d IsGift=%v Gifter=%s Target=%s System=%s Message=%s\n",
			timestamp, msg.MessageType, msg.Channel, msg.Nickname, msg.DisplayName,
			msg.SubPlan, msg.Months, msg.CumulativeMonths, msg.IsGift,
			msg.GifterName, msg.TargetUser, msg.SystemMessage, msg.Message)

	case "ban", "timeout":
		details = fmt.Sprintf("[%s] Type=%s Channel=%s TargetUser=%s Duration=%ds Reason=%s\n",
			timestamp, msg.MessageType, msg.Channel, msg.TargetUser, msg.BanDuration, msg.BanReason)

	case "delete_message":
		details = fmt.Sprintf("[%s] Type=%s Channel=%s User=%s MessageID=%s Message=%s\n",
			timestamp, msg.MessageType, msg.Channel, msg.Nickname, msg.TargetMessageID, msg.Message)

	case "raid":
		details = fmt.Sprintf("[%s] Type=%s Channel=%s Raider=%s Viewers=%d System=%s\n",
			timestamp, msg.MessageType, msg.Channel, msg.RaiderName, msg.ViewerCount, msg.SystemMessage)

	case "bits", "bits_badge_tier":
		details = fmt.Sprintf("[%s] Type=%s Channel=%s User=%s(%s) Bits=%d Message=%s\n",
			timestamp, msg.MessageType, msg.Channel, msg.Nickname, msg.DisplayName, msg.BitsAmount, msg.Message)

	case "notice", "user_notice":
		details = fmt.Sprintf("[%s] Type=%s Channel=%s NoticeID=%s Message=%s\n",
			timestamp, msg.MessageType, msg.Channel, msg.NoticeMessageID, msg.Message)

	default:
		details = fmt.Sprintf("[%s] Type=%s Channel=%s Raw=%s\n",
			timestamp, msg.MessageType, msg.Channel, msg.RawMessage)
	}

	f.WriteString(details)
}

// parseMessage parses IRC PRIVMSG format into a ChatMessage struct (legacy compatibility)
func (s *Subscriber) parseMessage(rawMessage string) *ChatMessage {
	// Use the new comprehensive IRC parser
	msg := parseIRCMessage(rawMessage)

	// Messages are automatically saved to the database via messageChan and dbWorkerBatch
	// No need to log to files - all data is in the database tables

	return msg
}

// logDisconnectionEvent logs disconnection events to a file for debugging
func (s *Subscriber) logDisconnectionEvent(eventType, channel, message string) {
	logFile := "disconnections.log"

	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Error opening log file: %v\n", err)
		return
	}
	defer f.Close()

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	logEntry := fmt.Sprintf("[%s] %s | Channel: %s | Message: %s\n",
		timestamp, eventType, channel, message)

	if _, err := f.WriteString(logEntry); err != nil {
		fmt.Printf("Error writing to log file: %v\n", err)
		return
	}

	// Also print to console for immediate visibility
	fmt.Printf("[DISCONNECT] %s | Channel: %s | Message: %s\n", eventType, channel, message)
}

// initializeTwitchClient initializes the Twitch API client with credentials
func (s *Subscriber) initializeTwitchClient() error {
	env, err := config.LoadEnv()
	if err != nil {
		return fmt.Errorf("failed to load environment: %v", err)
	}

	clientID := env["CLIENT_ID"]
	clientSecret := env["CLIENT_SECRET"]

	if clientID == "" || clientSecret == "" {
		return fmt.Errorf("CLIENT_ID and CLIENT_SECRET must be set in .env file or environment variables")
	}

	s.TwitchClient = twitch.NewClient(clientID, clientSecret)

	// Get OAuth token
	if err := s.TwitchClient.GetOAuth(); err != nil {
		return fmt.Errorf("failed to get OAuth token: %v", err)
	}

	fmt.Println("Twitch API authenticated successfully")
	return nil
}

// shutdown gracefully shuts down the subscriber
func (s *Subscriber) Shutdown() {
	fmt.Println("Shutting down gracefully...")
	s.cancel()                  // Cancel context to stop all goroutines
	time.Sleep(2 * time.Second) // Give time for goroutines to finish
	if s.DB != nil {
		// Wait a bit for any pending database operations
		time.Sleep(100 * time.Millisecond)
		s.DB.Close()
	}
}

// reconnect handles connection recovery when buffer overflows become unrecoverable
func (s *Subscriber) reconnect() error {
	fmt.Println("Attempting to reconnect due to buffer issues...")

	// Record reconnect metric
	metrics.RecordReconnect(s.ID)

	// Close existing connection
	if s.Connection != nil {
		s.Connection.Close()
	}

	// Reset connection state
	s.IsConnected = false
	metrics.UpdateConnectionStatus(s.ID, false)
	s.Connection = nil
	s.Reader = nil

	// Generate new nickname to avoid conflicts
	s.Nickname = "justinfan" + util.GenerateRandomString(10, "digits")

	// Reconnect
	return s.connect()
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
					fmt.Printf("Error saving batch to DB: %v\n", err)
				}
				s.dbMutex.Unlock()
				batch = batch[:0] // Clear batch
			}

		case <-ticker.C:
			if len(batch) > 0 {
				s.dbMutex.Lock()
				if err := s.saveChatMessageBatch(batch); err != nil {
					fmt.Printf("Error saving batch to DB: %v\n", err)
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
					fmt.Printf("[%s] Unrecoverable buffer error in readChat, skipping: %v\n", s.ID, err)
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
			if chatMsg := s.parseMessage(data); chatMsg != nil {
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
						fmt.Printf("[%s] Message channel full (%d/%d), dropping messages! This causes bursts when buffer clears.\n",
							s.ID, len(s.messageChan), cap(s.messageChan))
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
						fmt.Printf("Error sending PONG: %v\n", err)
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

	fmt.Printf("[%s] Chat reader started\n", s.ID)

	for {
		err := s.readChat()
		if err != nil {
			nFailure++
			fmt.Printf("[%s] Error reading chat: %v\n", s.ID, err)

			// Log the disconnection to file
			s.logDisconnectionEvent("CONNECTION_ERROR", "N/A", fmt.Sprintf("Connection error: %v", err))

			// Check if it's an EOF or connection closed error
			isEOF := strings.Contains(err.Error(), "EOF") ||
				strings.Contains(err.Error(), "connection reset") ||
				strings.Contains(err.Error(), "broken pipe") ||
				strings.Contains(err.Error(), "use of closed")

			// If we have EOF or connection errors, return to let pool handle reconnection
			// (If ID starts with "conn-", we're in a pool)
			if isEOF && strings.HasPrefix(s.ID, "conn-") {
				fmt.Printf("[%s] Connection closed by server (EOF), returning for pool to handle reconnection\n", s.ID)
				return // Let pool handle reconnection
			}

			// If we have EOF or connection errors and we're NOT in a pool, reconnect ourselves
			if isEOF {
				fmt.Println("Connection closed by server (EOF), attempting to reconnect...")
				if reconnErr := s.reconnect(); reconnErr != nil {
					fmt.Printf("Reconnection failed: %v\n", reconnErr)
				} else {
					fmt.Println("Reconnected successfully after EOF")
					nFailure = 0
					// Need to rejoin all channels
					s.channelsMutex.Lock()
					for channel := range s.connectedChannels {
						delete(s.connectedChannels, channel)
					}
					s.channelsMutex.Unlock()
					continue
				}
			}

			// If we have buffer-related errors and multiple failures, try reconnecting
			if nFailure > 2 && (strings.Contains(err.Error(), "buffer") || strings.Contains(err.Error(), "slice bounds")) {
				fmt.Println("Multiple buffer errors detected, attempting to reconnect...")
				if reconnErr := s.reconnect(); reconnErr != nil {
					fmt.Printf("Reconnection failed: %v\n", reconnErr)
				} else {
					fmt.Println("Reconnected successfully")
					nFailure = 0
					continue
				}
			}

			if nFailure > 5 {
				if strings.HasPrefix(s.ID, "conn-") {
					fmt.Printf("[%s] Too many failures, returning for pool to handle\n", s.ID)
					return // Let pool handle it
				}
				log.Fatalf("Too many failures: %v", err)
			}

			time.Sleep(5 * time.Second)
			continue
		}
	}
}

// discoverChannels fetches current live streams and returns channel names
func (s *Subscriber) discoverChannels(maxChannels int) ([]string, error) {
	if s.TwitchClient == nil {
		return nil, fmt.Errorf("twitch client not initialized")
	}

	var channels []string
	cursor := ""
	pageSize := 100

	for len(channels) < maxChannels {
		fmt.Printf("Discovering channels - fetched %d so far...\n", len(channels))

		streamsResp, err := s.TwitchClient.GetStreams(pageSize, cursor)
		if err != nil {
			return nil, fmt.Errorf("failed to get streams: %v", err)
		}

		if len(streamsResp.Data) == 0 {
			fmt.Println("No more live streams available")
			break
		}

		// Add channels from this page
		for _, stream := range streamsResp.Data {
			// Stop if we hit channels with fewer than 5 viewers (streams are sorted by viewer count)
			if stream.ViewerCount < 5 {
				fmt.Printf("Reached channels with < 5 viewers (current: %d viewers), stopping discovery\n", stream.ViewerCount)
				return channels, nil
			}

			channelName := "#" + stream.UserLogin
			channels = append(channels, channelName)

			fmt.Printf("Added channel: %s (viewers: %d)\n", channelName, stream.ViewerCount)

			if len(channels) >= maxChannels {
				break
			}
		}

		// Check if there's a next page
		if streamsResp.Pagination.Cursor == "" {
			fmt.Println("Reached end of available streams")
			break
		}

		cursor = streamsResp.Pagination.Cursor

		// Add a small delay to be respectful to the API
		time.Sleep(1 * time.Millisecond)
	}

	return channels, nil
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

	fmt.Printf("[%s] Joining %d new channels\n", s.ID, len(channelsToJoin))

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

			if err := s.joinChannelBatch(batch); err != nil {
				fmt.Printf("[%s] Error joining batch %v: %v\n", s.ID, batch, err)
				continue
			}

			for _, channel := range batch {
				s.connectedChannels[channel] = true
				successCount++
			}
			time.Sleep(1000 * time.Millisecond) // Minimal delay between batches
		}

		fmt.Printf("[%s] Joined %d/%d channels in batch %d/%d (%.1fs)\n",
			s.ID, successCount, end-i, (i/batchSize)+1, (len(channelsToJoin)+batchSize-1)/batchSize, time.Since(batchStart).Seconds())

		// Wait for the rest of the batch period if we finished early
		if end < len(channelsToJoin) {
			elapsed := time.Since(batchStart)
			if elapsed < batchDelay {
				remaining := batchDelay - elapsed
				fmt.Printf("[%s] Rate limiting: waiting %.1fs before next batch...\n", s.ID, remaining.Seconds())
				time.Sleep(remaining)
			}
		}
	}

	fmt.Printf("[%s] Finished joining all channels\n", s.ID)
}

// periodicChannelDiscovery runs in the background to discover and join new channels
func (s *Subscriber) periodicChannelDiscovery(maxChannels int) {
	ticker := time.NewTicker(s.scanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			channels, err := s.discoverChannels(maxChannels)
			if err != nil {
				fmt.Printf("Error discovering channels: %v\n", err)
				continue
			}

			// Only join new channels if we're connected
			if s.IsConnected {
				s.joinNewChannels(channels)
			} else {
				// Store channels for when we connect
				s.channelsMutex.Lock()
				s.Channels = channels
				s.channelsMutex.Unlock()
			}
		}
	}
}

// startStatisticsReporting starts a goroutine that reports statistics every second
func (s *Subscriber) startStatisticsReporting() {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		lastMessageCount := 0

		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				currentMessages := s.NMessages
				messagesPerSecond := currentMessages - lastMessageCount
				lastMessageCount = currentMessages

				connectedCount := len(s.connectedChannels)

				fmt.Printf("[STATS] Messages/sec: %d | Connected channels: %d | Total messages: %d\n",
					messagesPerSecond, connectedCount, currentMessages)
			}
		}
	}()
}

// Run is the main execution loop for the subscriber with dynamic channel discovery
func (s *Subscriber) Run(maxChannels int) {
	defer s.cancel() // Cancel context when run exits
	defer func() {
		if s.DB != nil {
			// Wait a bit for any pending database operations
			time.Sleep(100 * time.Millisecond)
			s.DB.Close()
		}
	}()

	// Initialize Twitch client for API access
	if err := s.initializeTwitchClient(); err != nil {
		log.Fatalf("Failed to initialize Twitch client: %v", err)
	}

	// Start statistics reporting
	s.startStatisticsReporting()

	// Start periodic channel discovery in background
	go s.periodicChannelDiscovery(maxChannels)

	// Initial channel discovery
	fmt.Println("Performing initial channel discovery...")
	channels, err := s.discoverChannels(maxChannels)
	if err != nil {
		log.Printf("Error in initial channel discovery: %v. Starting with no channels.", err)
	} else {
		s.channelsMutex.Lock()
		s.Channels = channels
		s.channelsMutex.Unlock()
		fmt.Printf("Initial discovery found %d channels\n", len(channels))
	}

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			// Connect to IRC
			if err := s.connect(); err != nil {
				fmt.Printf("Error connecting: %v\n", err)
				time.Sleep(1 * time.Second)
				// Generate new nickname for retry
				s.Nickname = "justinfan" + util.GenerateRandomString(10, "digits")
				continue
			}

			// Start reading chat messages in a separate goroutine before joining channels
			// This allows us to handle connection messages and early chat traffic immediately
			go s.infiniteReadChat()

			// Give the chat reader a moment to start
			time.Sleep(100 * time.Millisecond)

			// Join initial channels discovered
			s.channelsMutex.RLock()
			initialChannels := make([]string, len(s.Channels))
			copy(initialChannels, s.Channels)
			s.channelsMutex.RUnlock()

			if len(initialChannels) > 0 {
				fmt.Printf("Joining %d initial channels\n", len(initialChannels))
				s.joinNewChannels(initialChannels)
			} else {
				fmt.Println("No initial channels found - waiting for periodic discovery")
			}

			// Wait for the chat reader goroutine to finish (it runs indefinitely)
			// This will block here until context is cancelled or an error occurs
			<-s.ctx.Done()
			return
		}
	}
}
