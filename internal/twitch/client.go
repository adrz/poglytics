package twitch

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"poglytics-scraper/internal/db"
	"poglytics-scraper/internal/metrics"
)

// GetOAuth obtains an OAuth token for API access
func (c *Client) GetOAuth() error {
	url := "https://id.twitch.tv/oauth2/token"
	data := fmt.Sprintf("client_id=%s&client_secret=%s&grant_type=client_credentials",
		c.ClientID, c.ClientSecret)

	req, err := http.NewRequest("POST", url, strings.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to make request: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			slog.Error("Error closing response body", "error", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("OAuth request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp TokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	c.AccessToken = tokenResp.AccessToken
	return nil
}

// GetStreams retrieves stream data from the Twitch API
func (c *Client) GetStreams(pageSize int, cursor string) (*StreamsResponse, error) {
	url := fmt.Sprintf("https://api.twitch.tv/helix/streams?first=%d", pageSize)
	if cursor != "" {
		url += "&after=" + cursor
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.AccessToken)
	req.Header.Set("Client-Id", c.ClientID)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			slog.Error("Error closing response body", "error", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse and record rate limit headers
	if rateLimitStr := resp.Header.Get("Ratelimit-Limit"); rateLimitStr != "" {
		if rateLimit, err := strconv.Atoi(rateLimitStr); err == nil {
			if remainingStr := resp.Header.Get("Ratelimit-Remaining"); remainingStr != "" {
				if remaining, err := strconv.Atoi(remainingStr); err == nil {
					if resetStr := resp.Header.Get("Ratelimit-Reset"); resetStr != "" {
						if reset, err := strconv.ParseInt(resetStr, 10, 64); err == nil {
							metrics.UpdateTwitchAPIRateLimit(remaining, rateLimit, reset)
							log.Printf("Twitch API Rate Limit - Remaining: %d/%d, Reset: %d", remaining, rateLimit, reset)
						}
					}
				}
			}
		}
	}

	var streamsResp StreamsResponse
	if err := json.NewDecoder(resp.Body).Decode(&streamsResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	// Save streams to database if database is available
	if c.DB != nil && len(streamsResp.Data) > 0 {
		snapshots := make([]*db.StreamSnapshot, len(streamsResp.Data))
		snapshotTime := time.Now()

		for i, stream := range streamsResp.Data {
			startedAt, _ := time.Parse(time.RFC3339, stream.StartedAt)
			snapshots[i] = &db.StreamSnapshot{
				ID:           stream.ID,
				UserID:       stream.UserID,
				UserLogin:    stream.UserLogin,
				UserName:     stream.UserName,
				GameID:       stream.GameID,
				GameName:     stream.GameName,
				Type:         stream.Type,
				Title:        stream.Title,
				ViewerCount:  stream.ViewerCount,
				StartedAt:    startedAt,
				Language:     stream.Language,
				ThumbnailURL: stream.ThumbnailURL,
				Tags:         stream.Tags,
				TagIDs:       stream.TagIDs,
				IsMature:     stream.IsMature,
				SnapshotTime: snapshotTime,
			}
		}

		if err := c.DB.SaveStreamSnapshots(snapshots); err != nil {
			log.Printf("Warning: failed to save stream snapshots to database: %v", err)
		} else {
			log.Printf("Saved %d stream snapshots to database", len(snapshots))
		}
	}

	return &streamsResp, nil
}
