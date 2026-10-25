package twitch

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func response(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}
}

func TestOAuthAndCursorAreEncodedAndUnauthorizedIsRetried(t *testing.T) {
	client := NewClient("id&value", "secret=+&value")
	client.AccessToken = "expired"
	var streamCalls, oauthCalls int
	client.HTTPClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host == "id.twitch.tv" {
			oauthCalls++
			if err := req.ParseForm(); err != nil {
				t.Fatal(err)
			}
			if req.Form.Get("client_id") != client.ClientID || req.Form.Get("client_secret") != client.ClientSecret {
				t.Fatal("OAuth credentials were not form-encoded")
			}
			return response(200, `{"access_token":"renewed","expires_in":3600}`), nil
		}
		streamCalls++
		if req.URL.Query().Get("after") != "cursor+&=" {
			t.Fatal("cursor was not URL-encoded")
		}
		if streamCalls == 1 {
			return response(401, "expired"), nil
		}
		if req.Header.Get("Authorization") != "Bearer renewed" {
			t.Fatal("retry did not use refreshed token")
		}
		return response(200, `{"data":[],"pagination":{}}`), nil
	})
	if _, err := client.GetStreams(100, "cursor+&="); err != nil {
		t.Fatal(err)
	}
	if streamCalls != 2 || oauthCalls != 1 {
		t.Fatalf("streams=%d OAuth=%d", streamCalls, oauthCalls)
	}
}

func TestConcurrentRequestsShareTokenRefresh(t *testing.T) {
	client := NewClient("id", "secret")
	var oauthCalls atomic.Int64
	client.HTTPClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host == "id.twitch.tv" {
			oauthCalls.Add(1)
			return response(200, `{"access_token":"token","expires_in":3600}`), nil
		}
		return response(200, `{"data":[]}`), nil
	})
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := client.GetStreamsContext(context.Background(), 100, ""); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if oauthCalls.Load() != 1 {
		t.Fatalf("refreshed token %d times", oauthCalls.Load())
	}
}
