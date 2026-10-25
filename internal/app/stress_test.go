package app

// Stress/soak tests that exercise the reconnect, keepalive and join-pacing
// logic against a controllable fake Twitch-IRC server. This lets us reproduce
// - deterministically and offline - the failure modes that only showed up
// against real Twitch under load (reconnect storms, keepalive starvation during
// chat floods, JOIN bursts, server-requested RECONNECT).
//
// Run everything (a few of these are timing tests, ~30s total):
//
//	go test -race -run 'Stress' -v ./internal/app/
//
// Skip the slow timing tests (keeps only the fast, deterministic ones):
//
//	go test -race -short -run 'Stress' -v ./internal/app/
//
// The -race flag is the point: it also validates the atomic/mutex fixes.

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeIRC is a minimal, fault-injecting Twitch-IRC server. Each test supplies a
// per-connection handler that decides how the "server" behaves.
type fakeIRC struct {
	ln net.Listener

	mu          sync.Mutex
	acceptTimes []time.Time
	joinTimes   []time.Time // one entry per channel across all JOIN commands
	pingSent    map[string]time.Time
	pongLatency []time.Duration
	pongCount   int
}

func startFakeIRC(t *testing.T, handle func(*fakeIRC, net.Conn)) *fakeIRC {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fakeIRC listen: %v", err)
	}
	s := &fakeIRC{ln: ln, pingSent: map[string]time.Time{}}
	go func() {
		for {
			c, err := s.ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.acceptTimes = append(s.acceptTimes, time.Now())
			s.mu.Unlock()
			go func() {
				defer c.Close()
				handle(s, c)
			}()
		}
	}()
	return s
}

func (s *fakeIRC) hostPort(t *testing.T) (string, int) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(s.ln.Addr().String())
	if err != nil {
		t.Fatalf("split addr: %v", err)
	}
	p, _ := strconv.Atoi(portStr)
	return host, p
}

func (s *fakeIRC) closeServer() { _ = s.ln.Close() }

func (s *fakeIRC) accepts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.acceptTimes)
}

func (s *fakeIRC) acceptGaps() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	var g []time.Duration
	for i := 1; i < len(s.acceptTimes); i++ {
		g = append(g, s.acceptTimes[i].Sub(s.acceptTimes[i-1]))
	}
	return g
}

func (s *fakeIRC) joinCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.joinTimes)
}

func (s *fakeIRC) joinSpan() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.joinTimes) < 2 {
		return 0
	}
	return s.joinTimes[len(s.joinTimes)-1].Sub(s.joinTimes[0])
}

func (s *fakeIRC) pongs() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pongCount
}

func (s *fakeIRC) maxPongLatency() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	var m time.Duration
	for _, d := range s.pongLatency {
		if d > m {
			m = d
		}
	}
	return m
}

// readClient consumes client->server lines, recording JOINs and matching PONGs
// back to the PINGs we sent (to measure keepalive latency). Returns on EOF.
func (s *fakeIRC) readClient(c net.Conn) {
	br := bufio.NewReader(c)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case strings.HasPrefix(line, "JOIN "):
			n := len(strings.Split(strings.TrimPrefix(line, "JOIN "), ","))
			now := time.Now()
			s.mu.Lock()
			for i := 0; i < n; i++ {
				s.joinTimes = append(s.joinTimes, now)
			}
			s.mu.Unlock()
		case strings.HasPrefix(line, "PONG"):
			fields := strings.Fields(line)
			token := ""
			if len(fields) > 0 {
				token = strings.TrimPrefix(fields[len(fields)-1], ":")
			}
			now := time.Now()
			s.mu.Lock()
			s.pongCount++
			if sent, ok := s.pingSent[token]; ok {
				s.pongLatency = append(s.pongLatency, now.Sub(sent))
			}
			s.mu.Unlock()
		}
	}
}

func (s *fakeIRC) sendPing(c net.Conn, token string) error {
	s.mu.Lock()
	s.pingSent[token] = time.Now()
	s.mu.Unlock()
	_, err := c.Write([]byte("PING :" + token + "\r\n"))
	return err
}

// stressConn builds an IRCConnection aimed at the fake server. It has no DB or
// Twitch client - the connect/read/join code paths never touch them.
func stressConn(host string, port int, buf int) (*IRCConnection, chan *ChatMessage) {
	ch := make(chan *ChatMessage, buf)
	conn := NewIRCConnection(nil, nil, ch)
	conn.Server = host
	conn.Port = port
	return conn, ch
}

// TestStressReconnectBackoffCurve verifies the backoff grows exponentially and
// stays capped - the core of the reconnect-storm fix. Fast and deterministic.
func TestStressReconnectBackoffCurve(t *testing.T) {
	const cap = 2 * time.Minute
	prevMin := time.Duration(0)
	for f := 1; f <= 10; f++ {
		min, max := time.Hour, time.Duration(0)
		for i := 0; i < 300; i++ { // sample across the jitter
			d := reconnectBackoff(f)
			if d < min {
				min = d
			}
			if d > max {
				max = d
			}
		}
		if min <= 0 {
			t.Fatalf("failure %d: non-positive backoff %v", f, min)
		}
		if max > cap+cap/4 { // cap + jitter headroom
			t.Fatalf("failure %d: backoff %v exceeds cap", f, max)
		}
		// Below the cap, each step's floor must clearly exceed the previous.
		if f > 1 && f <= 6 && min <= prevMin {
			t.Fatalf("failure %d: backoff not growing (min %v <= prev %v)", f, min, prevMin)
		}
		prevMin = min
	}
}

// TestStressReconnectStormIsBounded proves that when the server keeps killing us
// (as real Twitch does when it throttles rapid reconnects), the backoff keeps
// the number of reconnect attempts small and increasingly spaced - instead of
// the old flat-2s hammering that produced the 48-EOF storm in disconnections.log.
func TestStressReconnectStormIsBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test")
	}
	srv := startFakeIRC(t, func(s *fakeIRC, c net.Conn) {
		// Read the handshake, then drop the connection almost immediately.
		_ = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		br := bufio.NewReader(c)
		_, _ = br.ReadString('\n')
	})
	defer srv.closeServer()

	host, port := srv.hostPort(t)
	conn, ch := stressConn(host, port, 256)
	go func() {
		for range ch {
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	pool := &ConnectionPool{ctx: ctx, cancel: cancel}
	go pool.runConnection(0, conn, nil)

	time.Sleep(14 * time.Second) // long enough to watch the backoff escalate
	cancel()
	time.Sleep(500 * time.Millisecond)

	n := srv.accepts()
	gaps := srv.acceptGaps()
	t.Logf("storm window (14s): %d reconnect attempts, gaps=%v", n, gaps)

	// Old flat-2s behaviour would produce ~6-7 attempts here.
	if n > 5 {
		t.Fatalf("too many reconnect attempts in 14s (%d): backoff is not limiting the storm", n)
	}
	// At least one backoff must have grown well past the old flat ~2s, proving
	// escalation (old code's gaps would all sit around ~2.5s).
	var maxGap time.Duration
	for _, g := range gaps {
		if g > maxGap {
			maxGap = g
		}
	}
	if len(gaps) > 0 && maxGap < 3500*time.Millisecond {
		t.Fatalf("reconnect gaps never escalated past ~2s (max %v): backoff not applied", maxGap)
	}
}

// TestStressPongSurvivesMessageFlood proves the keepalive PONG still goes out
// promptly while the connection is buried under a chat flood - i.e. a high-
// traffic channel can't starve the PING/PONG and get us disconnected.
func TestStressPongSurvivesMessageFlood(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test")
	}
	done := make(chan struct{})
	srv := startFakeIRC(t, func(s *fakeIRC, c net.Conn) {
		go s.readClient(c)

		line := ":user!user@user.tmi.twitch.tv PRIVMSG #chan :flood flood flood\r\n"
		flood := []byte(strings.Repeat(line, 20))
		deadline := time.Now().Add(3 * time.Second)
		nextPing := time.Now()
		seq := 0
		for time.Now().Before(deadline) {
			if _, err := c.Write(flood); err != nil {
				return
			}
			if !time.Now().Before(nextPing) {
				seq++
				if err := s.sendPing(c, fmt.Sprintf("seq-%d", seq)); err != nil {
					return
				}
				nextPing = time.Now().Add(250 * time.Millisecond)
			}
			time.Sleep(time.Millisecond) // ~20k msgs/sec, bounded
		}
		time.Sleep(500 * time.Millisecond) // let the last PONGs come back
		close(done)
	})
	defer srv.closeServer()

	host, port := srv.hostPort(t)
	conn, ch := stressConn(host, port, 8192)
	go func() {
		for range ch { // realistic downstream drain
		}
	}()

	if err := conn.connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	go conn.infiniteReadChat()

	<-done
	time.Sleep(200 * time.Millisecond)

	if srv.pongs() == 0 {
		t.Fatalf("client never PONGed during the flood: keepalive was starved")
	}
	if lat := srv.maxPongLatency(); lat > 2*time.Second {
		t.Fatalf("max PONG latency %v under flood: keepalive delayed by chat backlog", lat)
	}
	t.Logf("flood survived: %d pongs, max PONG latency %v", srv.pongs(), srv.maxPongLatency())
}

// TestStressServerRequestedReconnect proves we act on Twitch's RECONNECT command
// (cycle to a fresh socket) instead of sitting on the old one until it EOFs.
func TestStressServerRequestedReconnect(t *testing.T) {
	srv := startFakeIRC(t, func(s *fakeIRC, c net.Conn) {
		go s.readClient(c)
		time.Sleep(200 * time.Millisecond)
		_, _ = c.Write([]byte(":tmi.twitch.tv RECONNECT\r\n"))
		time.Sleep(6 * time.Second) // hold the socket open; only a RECONNECT-aware client leaves
	})
	defer srv.closeServer()

	host, port := srv.hostPort(t)
	conn, ch := stressConn(host, port, 256)
	go func() {
		for range ch {
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := &ConnectionPool{ctx: ctx, cancel: cancel}
	go pool.runConnection(0, conn, nil)

	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if srv.accepts() >= 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	time.Sleep(300 * time.Millisecond)

	if n := srv.accepts(); n < 2 {
		t.Fatalf("client did not cycle on RECONNECT (accepts=%d); it waited for EOF instead", n)
	}
}

// TestStressJoinsArePaced proves JOINs are spread over time rather than fired in
// one burst - the burst on (re)connect was what tripped Twitch's rate limit.
func TestStressJoinsArePaced(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test")
	}
	srv := startFakeIRC(t, func(s *fakeIRC, c net.Conn) {
		s.readClient(c) // block, draining handshake + JOINs until client leaves
	})
	defer srv.closeServer()

	host, port := srv.hostPort(t)
	conn, _ := stressConn(host, port, 256)

	if err := conn.connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}

	channels := make([]string, 0, 30)
	for i := 0; i < 30; i++ {
		channels = append(channels, fmt.Sprintf("#chan%02d", i))
	}

	start := time.Now()
	conn.joinNewChannels(channels) // paced + blocking
	elapsed := time.Since(start)

	// Give the server a beat to finish recording the last JOIN line.
	time.Sleep(100 * time.Millisecond)

	if got := srv.joinCount(); got != 30 {
		t.Fatalf("expected 30 channels joined, got %d", got)
	}
	// 30 channels at 10 per 2s => spread over ~4s. A burst would be sub-second.
	if span := srv.joinSpan(); span < 3500*time.Millisecond {
		t.Fatalf("JOINs were not paced: 30 channels joined within %v (looks like a burst)", span)
	} else {
		t.Logf("30 JOINs paced over %v (loop took %v)", span, elapsed)
	}
}
