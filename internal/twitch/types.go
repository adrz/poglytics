package twitch

import (
	"net/http"
	"time"
)

// Client represents a Twitch API client
type Client struct {
	ClientID     string
	ClientSecret string
	AccessToken  string
	HTTPClient   *http.Client
}

// TokenResponse represents the OAuth token response from Twitch
type TokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
	TokenType   string `json:"token_type"`
}

// Stream represents a Twitch stream
type Stream struct {
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

// StreamsResponse represents the response from the Twitch streams API
type StreamsResponse struct {
	Data       []Stream `json:"data"`
	Pagination struct {
		Cursor string `json:"cursor"`
	} `json:"pagination"`
}

// NewClient creates a new Twitch API client
func NewClient(clientID, clientSecret string) *Client {
	return &Client{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		HTTPClient:   &http.Client{Timeout: 30 * time.Second},
	}
}
