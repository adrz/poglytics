package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

type TwitchTokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
	TokenType   string `json:"token_type"`
}

type TwitchStream struct {
	ID           string   `json:"id"`
	UserID       string   `json:"user_id"`
	UserLogin    string   `json:"user_login"`
	UserName     string   `json:"user_name"`
	GameID       string   `json:"game_id"`
	GameName     string   `json:"game_name"`
	Type         string   `json:"type"`
	Title        string   `json:"title"`
	ViewerCount  int      `json:"viewer_count"`
	StartedAt    string   `json:"started_at"`
	Language     string   `json:"language"`
	ThumbnailURL string   `json:"thumbnail_url"`
	TagIDs       []string `json:"tag_ids"`
	IsMature     bool     `json:"is_mature"`
}

type TwitchStreamsResponse struct {
	Data       []TwitchStream `json:"data"`
	Pagination struct {
		Cursor string `json:"cursor"`
	} `json:"pagination"`
}

type TwitchClient struct {
	ClientID     string
	ClientSecret string
	AccessToken  string
	HTTPClient   *http.Client
}

func NewTwitchClient(clientID, clientSecret string) *TwitchClient {
	return &TwitchClient{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		HTTPClient:   &http.Client{Timeout: 30 * time.Second},
	}
}

func (tc *TwitchClient) GetOAuth() error {
	url := "https://id.twitch.tv/oauth2/token"
	data := fmt.Sprintf("client_id=%s&client_secret=%s&grant_type=client_credentials",
		tc.ClientID, tc.ClientSecret)

	req, err := http.NewRequest("POST", url, strings.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := tc.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("OAuth request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp TwitchTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	tc.AccessToken = tokenResp.AccessToken
	return nil
}

func (tc *TwitchClient) GetStreams(pageSize int, cursor string) (*TwitchStreamsResponse, error) {
	url := fmt.Sprintf("https://api.twitch.tv/helix/streams?first=%d", pageSize)
	if cursor != "" {
		url += "&after=" + cursor
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+tc.AccessToken)
	req.Header.Set("Client-Id", tc.ClientID)

	resp, err := tc.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var streamsResp TwitchStreamsResponse
	if err := json.NewDecoder(resp.Body).Decode(&streamsResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &streamsResp, nil
}

func loadEnv() (map[string]string, error) {
	env := make(map[string]string)

	file, err := os.Open("./.env")
	if err != nil {
		// Try to get from environment variables if .env file doesn't exist
		if clientID := os.Getenv("CLIENT_ID"); clientID != "" {
			env["CLIENT_ID"] = clientID
		}
		if clientSecret := os.Getenv("CLIENT_SECRET"); clientSecret != "" {
			env["CLIENT_SECRET"] = clientSecret
		}
		return env, nil
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			// Remove quotes if present
			if (strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"")) ||
				(strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'")) {
				value = value[1 : len(value)-1]
			}
			env[key] = value
		}
	}

	return env, scanner.Err()
}

func main() {
	// Load environment variables
	env, err := loadEnv()
	if err != nil {
		log.Fatalf("Failed to load environment: %v", err)
	}

	clientID := env["CLIENT_ID"]
	clientSecret := env["CLIENT_SECRET"]

	if clientID == "" || clientSecret == "" {
		log.Fatal("CLIENT_ID and CLIENT_SECRET must be set in .env file or environment variables")
	}

	// Create Twitch client
	client := NewTwitchClient(clientID, clientSecret)

	// Get OAuth token
	if err := client.GetOAuth(); err != nil {
		log.Fatalf("Failed to get OAuth token: %v", err)
	}

	fmt.Println("Successfully authenticated with Twitch API")

	// Fetch streams
	var channels []string
	cursor := ""
	pageSize := 100
	maxChannels := 500000

	for len(channels) < maxChannels {
		fmt.Printf("Fetching page with %d channels so far...\n", len(channels))

		streamsResp, err := client.GetStreams(pageSize, cursor)
		if err != nil {
			log.Fatalf("Failed to get streams: %v", err)
		}

		if len(streamsResp.Data) == 0 {
			fmt.Println("No more streams available")
			break
		}

		// Add channels from this page
		for i, stream := range streamsResp.Data {
			// Stop if we hit channels with fewer than 5 viewers (streams are sorted by viewer count)
			if stream.ViewerCount < 5 {
				fmt.Printf("Reached channels with < 5 viewers (current: %d viewers), stopping discovery\n", stream.ViewerCount)
				fmt.Printf("Final channel count: %d channels (all with 5+ viewers)\n", len(channels))
				goto writeFile
			}

			fmt.Printf("%d - Channel: %s (viewers: %d)\n", len(channels)+i, stream.UserLogin, stream.ViewerCount)
			channels = append(channels, "#"+stream.UserLogin)

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

writeFile:
	fmt.Printf("Collected %d channels\n", len(channels))

	// Write channels to file
	file, err := os.Create("../../channels.txt")
	if err != nil {
		log.Fatalf("Failed to create channels.txt: %v", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, channel := range channels {
		_, err := writer.WriteString(channel + "\n")
		if err != nil {
			log.Fatalf("Failed to write channel to file: %v", err)
		}
	}

	if err := writer.Flush(); err != nil {
		log.Fatalf("Failed to flush writer: %v", err)
	}

	fmt.Printf("Successfully wrote %d channels to channels.txt\n", len(channels))
}
