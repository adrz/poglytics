package app

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

	"poglytics-scraper/internal/metrics"
)

// readChat reads and processes chat messages from the IRC connection
func (c *IRCConnection) readChat() error {
	// Wait for connection
	for !c.IsConnected {
		time.Sleep(100 * time.Millisecond)
	}

	for {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		default:
			// Use robust reading method to handle buffer overflows
			// REDUCED timeout from 120s to 20s to prevent blocking and message bursts
			// Long timeouts cause messages to accumulate during JOIN operations
			data, err := c.readLineWithTimeoutRobust(20 * time.Second)
			if err != nil {
				// If we get a buffer error that we can't handle, try to continue
				if strings.Contains(err.Error(), "buffer") || strings.Contains(err.Error(), "slice bounds") {
					slog.Error("Unrecoverable buffer error in readChat, skipping", "id", c.ID, "error", err)
					metrics.RecordParseError(c.ID, "buffer_error")
					// Reset reader and continue
					c.Reader = bufio.NewReaderSize(c.Connection, 4*1024*1024)
					continue
				}
				if strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "i/o timeout") {
					metrics.RecordReadTimeout(c.ID)
					// Timeout is OK during low activity - just continue
					continue
				}
				return fmt.Errorf("error reading from connection: %v", err)
			}

			c.NMessages++

			// Parse chat message and send to async processing
			if chatMsg := c.parseIRCMessage(data); chatMsg != nil {
				// Record message metric
				metrics.RecordMessage(c.ID)

				// Record parsed message type metric
				if chatMsg.MessageType != "" {
					metrics.RecordParsedMessage(c.ID, chatMsg.MessageType)
				}

				// Try to send to message channel (non-blocking)
				select {
				case c.messageChan <- chatMsg:
					// Message sent successfully
				default:
					// Channel full - log periodically
					if c.NMessages%1000 == 0 {
						slog.Warn("Message channel full, dropping messages", "id", c.ID, "len", len(c.messageChan), "cap", cap(c.messageChan))
					}
					metrics.RecordParseError(c.ID, "channel_full")
				}
			}

			// PING PONG
			if strings.HasPrefix(data, "PING") {
				pingStart := time.Now()
				parts := strings.Split(strings.TrimSpace(data), " ")
				if len(parts) > 1 {
					pongMsg := fmt.Sprintf("PONG %s", parts[len(parts)-1])
					if err := c.send(pongMsg); err != nil {
						slog.Error("Error sending PONG", "id", c.ID, "error", err)
					} else {
						// Record ping/pong latency
						latency := time.Since(pingStart).Seconds()
						metrics.RecordPingPong(c.ID, latency)
						metrics.RecordIRCMessage(c.ID, "PING")
					}
				}
			}
		}
	}
}

// infiniteReadChat continuously reads chat messages with error recovery
func (c *IRCConnection) infiniteReadChat() {
	nFailure := 0

	for !c.IsConnected {
		time.Sleep(500 * time.Millisecond)
	}

	slog.Info("Chat reader started", "id", c.ID)

	for {
		err := c.readChat()
		if err != nil {
			nFailure++
			slog.Error("Error reading chat", "id", c.ID, "error", err)

			// Log the disconnection to file
			c.logDisconnectionEvent("CONNECTION_ERROR", "N/A", fmt.Sprintf("Connection error: %v", err))

			// Check if it's an EOF or connection closed error
			isEOF := strings.Contains(err.Error(), "EOF") ||
				strings.Contains(err.Error(), "connection reset") ||
				strings.Contains(err.Error(), "broken pipe") ||
				strings.Contains(err.Error(), "use of closed")

			// If we have EOF or connection errors, return to let pool handle reconnection
			// (If ID starts with "conn-", we're in a pool)
			if isEOF {
				slog.Info("Connection closed by server (EOF), returning for pool to handle reconnection", "id", c.ID)
				// Need to clear connected channels so they'll be rejoined after pool reconnects
				c.channelsMutex.Lock()
				for channel := range c.connectedChannels {
					delete(c.connectedChannels, channel)
				}
				c.channelsMutex.Unlock()
				return // Let pool handle reconnection
			}

			time.Sleep(5 * time.Second)
			continue
		}
	}
}

// readLineWithTimeoutRobust reads a line with timeout and handles buffer overflows more gracefully
func (c *IRCConnection) readLineWithTimeoutRobust(timeout time.Duration) (string, error) {
	if c.Connection == nil {
		return "", fmt.Errorf("connection is nil")
	}

	// Set read deadline
	c.Connection.SetReadDeadline(time.Now().Add(timeout))
	defer c.Connection.SetReadDeadline(time.Time{}) // Reset deadline

	// First try with the current reader
	line, err := c.Reader.ReadBytes('\n')
	if err == nil {
		return string(line), nil
	}
	return "", err
}

// send sends a message to the IRC server
func (c *IRCConnection) send(message string) error {
	if c.Connection == nil {
		return fmt.Errorf("connection is nil")
	}

	_, err := c.Connection.Write([]byte(message + "\r\n"))
	return err
}

// connect establishes a connection to the Twitch IRC server with exponential backoff retry
func (c *IRCConnection) connect() error {
	exp := 0
	connected := false

	for !connected {
		conn, err := net.Dial("tcp", c.Server+":"+strconv.Itoa(c.Port))
		if err != nil {
			slog.Info("Connection failed, retrying", "id", c.ID, "retry_in_seconds", 1<<exp)
			time.Sleep(time.Duration(1<<exp) * time.Second)
			exp++
			continue
		}

		c.Connection = conn
		c.Reader = bufio.NewReaderSize(conn, 2*1024*1024) // 2MB buffer instead of default 4KB
		c.IsConnected = true
		metrics.UpdateConnectionStatus(c.ID, true)
		slog.Info("Connected to Twitch IRC", "id", c.ID)
		connected = true
	}

	// Request IRC capabilities for tags, commands, and membership
	// This enables us to receive:
	// - tags: IRC v3 message tags with metadata
	// - commands: CLEARCHAT, CLEARMSG, HOSTTARGET, NOTICE, RECONNECT, ROOMSTATE, USERNOTICE, USERSTATE
	// - membership: JOIN, PART, MODE messages
	if err := c.send("CAP REQ :twitch.tv/tags twitch.tv/commands"); err != nil {
		return err
	}

	// Send authentication
	if err := c.send(fmt.Sprintf("PASS %s", c.Token)); err != nil {
		return err
	}

	if err := c.send(fmt.Sprintf("NICK %s", c.Nickname)); err != nil {
		return err
	}

	return nil
}
