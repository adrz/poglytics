package app

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"twitch-chat-scrapper/internal/config"
	"twitch-chat-scrapper/internal/db"
	"twitch-chat-scrapper/internal/twitch"
	"twitch-chat-scrapper/internal/util"
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
		messageChan:       make(chan *ChatMessage, 10000),
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
		fmt.Println("Connected to Twitch IRC")
		connected = true
	}

	// Send authentication
	if err := s.send(fmt.Sprintf("PASS %s", s.Token)); err != nil {
		return err
	}

	if err := s.send(fmt.Sprintf("NICK %s", s.Nickname)); err != nil {
		return err
	}

	// Read authentication response with error handling for large responses
	response, err := s.readLineWithTimeout(5 * time.Second)
	if err != nil {
		return err
	}

	fmt.Print("Auth response:", response)

	if !strings.HasPrefix(response, ":tmi.twitch.tv 001") {
		return fmt.Errorf("twitch did not accept the username-oauth combination")
	}

	return nil
}

// joinChannel joins a specific channel
func (s *Subscriber) joinChannel(channel string) error {
	if err := s.send(fmt.Sprintf("JOIN %s", channel)); err != nil {
		return err
	}

	s.NChannels++
	s.ListChannels = append(s.ListChannels, channel)
	return nil
}

// parseMessage parses IRC PRIVMSG format into a ChatMessage struct
func (s *Subscriber) parseMessage(rawMessage string) *ChatMessage {
	// Regular expression to parse IRC PRIVMSG format
	// Example: :nickname!nickname@nickname.tmi.twitch.tv PRIVMSG #channel :message
	re := regexp.MustCompile(`^:([^!]+)![^@]+@[^ ]+ PRIVMSG (#[^ ]+) :(.*)$`)
	matches := re.FindStringSubmatch(strings.TrimSpace(rawMessage))

	if len(matches) != 4 {
		return nil // Not a chat message
	}

	return &ChatMessage{
		Nickname:  matches[1],
		Channel:   matches[2],
		Message:   matches[3],
		Timestamp: time.Now(),
	}
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

// detectDisconnectionMessage detects PART, NOTICE, or KICK messages from IRC
func (s *Subscriber) detectDisconnectionMessage(rawMessage string) {
	trimmed := strings.TrimSpace(rawMessage)

	// Detect PART messages
	// Format: :username!username@username.tmi.twitch.tv PART #channel
	if strings.Contains(trimmed, " PART ") {
		parts := strings.Fields(trimmed)
		for i, part := range parts {
			if part == "PART" && i+1 < len(parts) {
				channel := parts[i+1]
				s.logDisconnectionEvent("PART", channel, trimmed)

				// Remove from connected channels
				s.channelsMutex.Lock()
				delete(s.connectedChannels, channel)
				s.channelsMutex.Unlock()
				break
			}
		}
		return
	}

	// Detect KICK messages
	// Format: :username!username@username.tmi.twitch.tv KICK #channel username :reason
	if strings.Contains(trimmed, " KICK ") {
		parts := strings.Fields(trimmed)
		for i, part := range parts {
			if part == "KICK" && i+1 < len(parts) {
				channel := parts[i+1]
				reason := ""
				// Find reason after the second parameter
				if i+3 < len(parts) {
					reasonStart := strings.Index(trimmed, parts[i+2]) + len(parts[i+2])
					if colonIdx := strings.Index(trimmed[reasonStart:], ":"); colonIdx != -1 {
						reason = trimmed[reasonStart+colonIdx+1:]
					}
				}
				logMsg := fmt.Sprintf("%s | Reason: %s", trimmed, reason)
				s.logDisconnectionEvent("KICK", channel, logMsg)

				// Remove from connected channels
				s.channelsMutex.Lock()
				delete(s.connectedChannels, channel)
				s.channelsMutex.Unlock()
				break
			}
		}
		return
	}

	// Detect NOTICE messages
	// Format: @msg-id=... :tmi.twitch.tv NOTICE #channel :message
	if strings.Contains(trimmed, " NOTICE ") {
		// Extract channel
		noticeRe := regexp.MustCompile(`NOTICE (#\w+)`)
		matches := noticeRe.FindStringSubmatch(trimmed)
		if len(matches) >= 2 {
			channel := matches[1]

			// Extract the notice message
			noticeMsg := ""
			if colonIdx := strings.LastIndex(trimmed, ":"); colonIdx != -1 {
				noticeMsg = trimmed[colonIdx+1:]
			}

			logMsg := fmt.Sprintf("%s | Notice: %s", trimmed, noticeMsg)
			s.logDisconnectionEvent("NOTICE", channel, logMsg)

			// Check if it's a serious notice that indicates disconnection
			lowerNotice := strings.ToLower(noticeMsg)
			if strings.Contains(lowerNotice, "suspend") ||
				strings.Contains(lowerNotice, "banned") ||
				strings.Contains(lowerNotice, "timeout") {
				// Remove from connected channels
				s.channelsMutex.Lock()
				delete(s.connectedChannels, channel)
				s.channelsMutex.Unlock()
			}
		}
		return
	}
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

	// Close existing connection
	if s.Connection != nil {
		s.Connection.Close()
	}

	// Reset connection state
	s.IsConnected = false
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

	return s.DB.SaveChatMessageBatch(messages)
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
			data, err := s.readLineWithTimeoutRobust(30 * time.Second)
			if err != nil {
				// If we get a buffer error that we can't handle, try to continue
				if strings.Contains(err.Error(), "buffer") || strings.Contains(err.Error(), "slice bounds") {
					fmt.Printf("Unrecoverable buffer error in readChat, skipping: %v\n", err)
					// Reset reader and continue
					s.Reader = bufio.NewReaderSize(s.Connection, 4*1024*1024)
					continue
				}
				return fmt.Errorf("error reading from connection: %v", err)
			}

			s.NMessages++

			// Detect disconnection messages (PART, NOTICE, KICK)
			s.detectDisconnectionMessage(data)

			// Parse chat message and send to async processing
			if chatMsg := s.parseMessage(data); chatMsg != nil {
				select {
				case s.messageChan <- chatMsg:
					// Message sent successfully
				default:
					if s.NMessages%1000 == 0 {
						fmt.Println("Message channel full, dropping messages")
					}
				}
			}

			// PING PONG
			if strings.HasPrefix(data, "PING") {
				parts := strings.Split(strings.TrimSpace(data), " ")
				if len(parts) > 1 {
					pongMsg := fmt.Sprintf("PONG %s", parts[len(parts)-1])
					if err := s.send(pongMsg); err != nil {
						fmt.Printf("Error sending PONG: %v\n", err)
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

	fmt.Println("Chat reader started")

	for {
		err := s.readChat()
		if err != nil {
			nFailure++
			fmt.Printf("Error reading chat: %v\n", err)

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
			channelName := "#" + stream.UserLogin
			channels = append(channels, channelName)

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

	fmt.Printf("Joining %d new channels\n", len(channelsToJoin))

	// Join channels with rate limiting
	batchSize := 100
	for i := 0; i < len(channelsToJoin); i += batchSize {
		end := i + batchSize
		if end > len(channelsToJoin) {
			end = len(channelsToJoin)
		}

		for _, channel := range channelsToJoin[i:end] {
			if err := s.joinChannel(channel); err != nil {
				fmt.Printf("Error joining channel %s: %v\n", channel, err)
				continue
			}
			s.connectedChannels[channel] = true
			time.Sleep(10 * time.Millisecond)
		}

		if end < len(channelsToJoin) {
			time.Sleep(50 * time.Millisecond)
		}
	}
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
