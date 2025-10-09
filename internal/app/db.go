package app

import (
	"log/slog"
	"time"

	"poglytics-scraper/internal/db"
)

// saveChatMessageBatch saves multiple messages using the database interface
func (conn *IRCConnection) saveChatMessageBatch(messages []*ChatMessage) error {
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

	return conn.DB.SaveChatMessageBatch(dbMessages)
}

// dbWorkerBatch processes chat messages in batches for better database performance
func (conn *IRCConnection) dbWorkerBatch() {
	ticker := time.NewTicker(1 * time.Second) // Batch every second
	defer ticker.Stop()

	var batch []*ChatMessage
	const maxBatchSize = 100

	for {
		select {
		case <-conn.ctx.Done():
			// Process remaining messages before exiting
			if len(batch) > 0 {
				conn.saveChatMessageBatch(batch)
			}
			return

		case msg := <-conn.messageChan:
			batch = append(batch, msg)
			if len(batch) >= maxBatchSize {
				conn.dbMutex.Lock()
				if err := conn.saveChatMessageBatch(batch); err != nil {
					slog.Error("Error saving batch to DB", "error", err, "id", conn.ID)
				}
				conn.dbMutex.Unlock()
				batch = batch[:0] // Clear batch
			}

		case <-ticker.C:
			if len(batch) > 0 {
				conn.dbMutex.Lock()
				if err := conn.saveChatMessageBatch(batch); err != nil {
					slog.Error("Error saving batch to DB", "error", err, "id", conn.ID)
				}
				conn.dbMutex.Unlock()
				batch = batch[:0] // Clear batch
			}
		}
	}
}
