package twitch

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
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
	defer resp.Body.Close()

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
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var streamsResp StreamsResponse
	if err := json.NewDecoder(resp.Body).Decode(&streamsResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &streamsResp, nil
}
