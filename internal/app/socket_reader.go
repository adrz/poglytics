package app

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

	"poglytics-scraper/internal/metrics"
)

// readChat reads and processes chat messages from the IRC connection
func (c *IRCConnection) readChat() error {
	if !c.IsConnected.Load() {
		return fmt.Errorf("connection is not ready")
	}

	for {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		default:
			data, err := c.readLineWithTimeoutRobust(20 * time.Second)
			if err != nil {
				var networkError net.Error
				if errors.As(err, &networkError) && networkError.Timeout() {
					metrics.RecordReadTimeout(c.ID)
					continue
				}
				return fmt.Errorf("read IRC message: %w", err)
			}

			c.NMessages.Add(1)

			// Twitch asks us to reconnect (server maintenance / rebalancing).
			// Cycle the connection promptly instead of waiting for the EOF that
			// otherwise follows; the pool reconnects with backoff.
			if strings.HasPrefix(data, ":tmi.twitch.tv RECONNECT") {
				slog.Info("Twitch requested RECONNECT, cycling connection", "id", c.ID)
				metrics.RecordIRCMessage(c.ID, "RECONNECT")
				return fmt.Errorf("server requested RECONNECT")
			}

			// Handle PING/PONG BEFORE parsing chat, so a burst of chat traffic
			// can never delay our PONG. A late PONG gets the connection killed.
			if strings.HasPrefix(data, "PING ") {
				pingStart := time.Now()
				payload := strings.TrimRight(strings.TrimPrefix(data, "PING "), "\r\n")
				if payload != "" {
					pongMsg := "PONG " + payload
					if err := c.send(pongMsg); err != nil {
						return fmt.Errorf("send PONG: %w", err)
					} else {
						// Record ping/pong latency
						latency := time.Since(pingStart).Seconds()
						metrics.RecordPingPong(c.ID, latency)
						metrics.RecordIRCMessage(c.ID, "PING")
					}
				}
				continue
			}

			c.trackMembership(data)

			// Parse chat message and send to async processing
			if chatMsg := c.parseIRCMessage(data); chatMsg != nil {
				// Record message metric
				metrics.RecordMessage(c.ID)

				// Record parsed message type metric
				if chatMsg.MessageType != "" {
					metrics.RecordParsedMessage(c.ID, chatMsg.MessageType)
				}

				// Record NOTICEs by msg-id: this is Twitch telling us *why* it is
				// unhappy (rate limits, join failures, suspensions). Ground truth
				// for debugging disconnects.
				if chatMsg.MessageType == "notice" {
					metrics.RecordNotice(c.ID, chatMsg.NoticeMessageID)
					if throttleNotice(chatMsg.NoticeMessageID) {
						slog.Warn("Twitch NOTICE (possible throttling)", "id", c.ID, "msg_id", chatMsg.NoticeMessageID, "message", chatMsg.Message)
					}
				}

				// Try to send to message channel (non-blocking)
				select {
				case c.messageChan <- chatMsg:
					// Message sent successfully
				default:
					metrics.MessagesDropped.Inc()
					// Channel full - log periodically
					if c.NMessages.Load()%1000 == 0 {
						slog.Warn("Message channel full, dropping messages", "id", c.ID, "len", len(c.messageChan), "cap", cap(c.messageChan))
					}
					metrics.RecordParseError(c.ID, "channel_full")
				}
			}
		}
	}
}

// infiniteReadChat is retained for callers that only need to run a reader.
// Every terminal error is handled by the connection owner with a fresh socket.
func (c *IRCConnection) infiniteReadChat() {
	if err := c.readChat(); err != nil && c.ctx.Err() == nil {
		slog.Error("Chat reader stopped", "id", c.ID, "error", err)
	}
}

// readLineWithTimeoutRobust preserves incomplete lines across read timeouts and
// bounds memory even when a peer never sends a newline.
func (c *IRCConnection) readLineWithTimeoutRobust(timeout time.Duration) (string, error) {
	c.connectionMu.RLock()
	socket, reader := c.Connection, c.Reader
	c.connectionMu.RUnlock()
	if socket == nil || reader == nil {
		return "", fmt.Errorf("connection is nil")
	}
	if err := socket.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return "", err
	}
	const maxIRCLineBytes = 64 * 1024
	for {
		fragment, err := reader.ReadSlice('\n')
		if len(c.readPartial)+len(fragment) > maxIRCLineBytes {
			c.readPartial = nil
			return "", fmt.Errorf("IRC line exceeds %d bytes", maxIRCLineBytes)
		}
		c.readPartial = append(c.readPartial, fragment...)
		if err == nil {
			line := string(c.readPartial)
			c.readPartial = c.readPartial[:0]
			return line, nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return "", err
	}
}

// throttleNotice reports whether a Twitch NOTICE msg-id indicates we are being
// rate-limited / throttled - the ground-truth signal behind join/traffic-driven
// disconnects.
func throttleNotice(msgID string) bool {
	switch msgID {
	case "msg_ratelimit", "msg_rejected", "msg_rejected_mandatory", "unrecognized_cmd":
		return true
	}
	return strings.Contains(msgID, "ratelimit") || strings.Contains(msgID, "rate_limit")
}

// send sends a message to the IRC server. Writes are serialized (JOIN, PONG and
// auth all share one socket) and bounded by a deadline so a stalled socket can't
// block a goroutine forever.
func (c *IRCConnection) send(message string) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.connectionMu.RLock()
	socket := c.Connection
	c.connectionMu.RUnlock()
	if socket == nil {
		return fmt.Errorf("connection is nil")
	}
	if err := socket.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	payload := []byte(message + "\r\n")
	n, err := socket.Write(payload)
	if err == nil && n != len(payload) {
		err = io.ErrShortWrite
	}
	return err
}

// connect makes one bounded attempt. The pool owns cancellation and backoff.
func (c *IRCConnection) connect() error {
	dialer := net.Dialer{Timeout: 10 * time.Second}
	socket, err := dialer.DialContext(c.ctx, "tcp", net.JoinHostPort(c.Server, strconv.Itoa(c.Port)))
	if err != nil {
		return err
	}
	c.connectionMu.Lock()
	if err := c.ctx.Err(); err != nil {
		c.connectionMu.Unlock()
		_ = socket.Close()
		return err
	}
	c.Connection = socket
	c.Reader = bufio.NewReaderSize(socket, 64*1024)
	c.readPartial = nil
	c.connectionMu.Unlock()

	c.channelsMutex.Lock()
	clear(c.connectedChannels)
	clear(c.joinAttempts)
	c.ListChannels = c.ListChannels[:0]
	c.NChannels.Store(0)
	c.channelsMutex.Unlock()

	for _, command := range []string{
		"CAP REQ :twitch.tv/tags twitch.tv/commands",
		fmt.Sprintf("PASS %s", c.Token),
		fmt.Sprintf("NICK %s", c.Nickname),
	} {
		if err := c.send(command); err != nil {
			c.closeSocket()
			return err
		}
	}
	c.IsConnected.Store(true)
	metrics.UpdateConnectionStatus(c.ID, true)
	return nil
}
