package app

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"poglytics-scraper/internal/metrics"
)

// joinNewChannels joins channels that are not currently connected
func (conn *IRCConnection) joinNewChannels(newChannels []string) {
	conn.channelsMutex.Lock()
	defer conn.channelsMutex.Unlock()

	var channelsToJoin []string

	for _, channel := range newChannels {
		if !conn.connectedChannels[channel] {
			channelsToJoin = append(channelsToJoin, channel)
		}
	}

	if len(channelsToJoin) == 0 {
		return
	}

	slog.Info("Joining new channels", "id", conn.ID, "count", len(channelsToJoin))

	// Join channels with rate limiting to avoid Twitch IRC limits
	// Twitch allows ~50 JOIN commands per 15 seconds (conservative estimate)
	batchSize := 50                // Join 50 channels at a time
	batchDelay := 10 * time.Second // Wait 10 seconds between batches

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
			if err := conn.joinChannel(batch); err != nil {
				slog.Error("Error joining batch", "id", conn.ID, "batch", batch, "error", err)
				continue
			}

			for _, channel := range batch {
				conn.connectedChannels[channel] = true
				successCount++
			}
			time.Sleep(1000 * time.Millisecond) // Minimal delay between batches
		}

		slog.Info("Joined channels in batch", "id", conn.ID, "joined", successCount, "total_in_batch", end-i, "batch", (i/batchSize)+1, "total_batches", (len(channelsToJoin)+batchSize-1)/batchSize, "duration", time.Since(batchStart).Seconds())

		// Wait for the rest of the batch period if we finished early
		if end < len(channelsToJoin) {
			elapsed := time.Since(batchStart)
			if elapsed < batchDelay {
				remaining := batchDelay - elapsed
				slog.Info("Rate limiting: waiting before next batch", "id", conn.ID, "wait_seconds", remaining.Seconds())
				time.Sleep(remaining)
			}
		}
	}

	slog.Info("Finished joining all channels", "id", conn.ID)
}

// joinChannel joins multiple channels in a single JOIN command
func (conn *IRCConnection) joinChannel(channels []string) error {
	if len(channels) == 0 {
		return nil
	}

	// Create comma-separated channel list
	channelList := strings.Join(channels, ",")

	if err := conn.send(fmt.Sprintf("JOIN %s", channelList)); err != nil {
		// Record failure for all channels in batch
		for range channels {
			metrics.RecordChannelJoin(conn.ID, false)
		}
		return err
	}

	// Record success for all channels
	for _, channel := range channels {
		conn.NChannels++
		conn.ListChannels = append(conn.ListChannels, channel)
		metrics.RecordChannelJoin(conn.ID, true)
	}

	return nil
}
