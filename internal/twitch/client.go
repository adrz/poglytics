package twitch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"poglytics-scraper/internal/db"
	"poglytics-scraper/internal/metrics"
)

// GetOAuth obtains an OAuth token for API access
func (c *Client) GetOAuth() error {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	return c.getOAuth(context.Background())
}

// getOAuth is called with tokenMu held.
func (c *Client) getOAuth(ctx context.Context) error {
	data := url.Values{
		"client_id": {c.ClientID}, "client_secret": {c.ClientSecret},
		"grant_type": {"client_credentials"},
	}
	req, err := http.NewRequestWithContext(ctx, "POST", "https://id.twitch.tv/oauth2/token", strings.NewReader(data.Encode()))
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

	if tokenResp.AccessToken == "" {
		return fmt.Errorf("OAuth response contains no access token")
	}
	c.AccessToken = tokenResp.AccessToken
	c.tokenExpires = time.Time{}
	if tokenResp.ExpiresIn > 0 {
		c.tokenExpires = time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second)
	}
	return nil
}

// GetStreams retrieves stream data from the Twitch API
func (c *Client) GetStreams(pageSize int, cursor string) (*StreamsResponse, error) {
	return c.GetStreamsContext(context.Background(), pageSize, cursor)
}

func (c *Client) GetStreamsContext(ctx context.Context, pageSize int, cursor string) (*StreamsResponse, error) {
	if pageSize < 1 || pageSize > 100 {
		return nil, fmt.Errorf("page size must be between 1 and 100")
	}
	resp, err := c.streamsRequest(ctx, pageSize, cursor)
	if err != nil {
		return nil, err
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

func (c *Client) accessToken(ctx context.Context, rejected string) (string, error) {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	if c.AccessToken == "" || (!c.tokenExpires.IsZero() && !time.Now().Before(c.tokenExpires)) || (rejected != "" && c.AccessToken == rejected) {
		if err := c.getOAuth(ctx); err != nil {
			return "", err
		}
	}
	return c.AccessToken, nil
}

func (c *Client) streamsRequest(ctx context.Context, pageSize int, cursor string) (*http.Response, error) {
	query := url.Values{"first": {strconv.Itoa(pageSize)}}
	if cursor != "" {
		query.Set("after", cursor)
	}
	rejected := ""
	for attempt := 0; attempt < 2; attempt++ {
		token, err := c.accessToken(ctx, rejected)
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, "GET", "https://api.twitch.tv/helix/streams?"+query.Encode(), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Client-Id", c.ClientID)
		resp, err := c.HTTPClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("get streams: %w", err)
		}
		if resp.StatusCode != http.StatusUnauthorized || attempt == 1 {
			return resp, nil
		}
		_ = resp.Body.Close()
		rejected = token
	}
	panic("unreachable")
}
