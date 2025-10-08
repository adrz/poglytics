package app

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func (c *IRCConnection) parseIRCMessage(rawMessage string) *ChatMessage {
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
// Unused for debug purposes only
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
