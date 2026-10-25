package app

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"poglytics-scraper/internal/db"
)

type recordingDB struct {
	messages  []*db.ChatMessage
	snapshots []*db.StreamSnapshot
	closes    int
}

func (*recordingDB) InitDB() error { return nil }
func (d *recordingDB) SaveChatMessageBatch(batch interface{}) error {
	d.messages = append(d.messages, batch.([]*db.ChatMessage)...)
	return nil
}
func (d *recordingDB) Close() { d.closes++ }
func (d *recordingDB) SaveStreamSnapshots(rows []*db.StreamSnapshot) error {
	d.snapshots = append(d.snapshots, rows...)
	return nil
}
func (*recordingDB) GetDB() *sql.DB { return nil }

func TestShutdownDrainsQueueAndClosesSharedDBOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	database := &recordingDB{}
	options := db.DefaultBatchOptions()
	options.Legacy = true
	writer, err := db.NewIngestWriter(database, options, nil)
	if err != nil {
		t.Fatal(err)
	}
	pool := &ConnectionPool{ctx: ctx, cancel: cancel, sharedDB: database, ingestWriter: writer, centralMessageChan: make(chan *ChatMessage, 1201)}
	for i := 0; i < cap(pool.centralMessageChan); i++ {
		pool.centralMessageChan <- &ChatMessage{Message: fmt.Sprint(i), Tags: map[string]string{"id": fmt.Sprint(i)}}
	}
	for i := 0; i < 3; i++ {
		pool.connections = append(pool.connections, NewIRCConnection(database, nil, pool.centralMessageChan))
	}
	pool.startCentralizedDBWriter()
	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		wg.Add(2)
		for i := 0; i < 2; i++ {
			go func() { defer wg.Done(); pool.Shutdown() }()
		}
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not finish promptly")
	}
	if len(database.messages) != 1201 || database.closes != 1 {
		t.Fatalf("saved %d/1201 messages; closed database %d times", len(database.messages), database.closes)
	}
	for i, msg := range database.messages {
		if msg.Message != fmt.Sprint(i) || msg.Tags["id"] != fmt.Sprint(i) {
			t.Fatalf("message %d lost its content or tags: %+v", i, msg)
		}
	}
}

func TestTableWriterShutdownDrainsChatAndDiscoverySnapshots(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	database := &recordingDB{}
	writer, err := db.NewIngestWriter(database, db.DefaultBatchOptions(), nil)
	if err != nil {
		t.Fatal(err)
	}
	pool := &ConnectionPool{ctx: ctx, cancel: cancel, sharedDB: database, ingestWriter: writer, centralMessageChan: make(chan *ChatMessage, 1201)}
	for i := 0; i < cap(pool.centralMessageChan); i++ {
		pool.centralMessageChan <- &ChatMessage{MessageType: []string{"text_message", "ban", "room_state"}[i%3], Message: fmt.Sprint(i), Tags: map[string]string{"id": fmt.Sprint(i)}}
	}
	if err := writer.SaveStreamSnapshots([]*db.StreamSnapshot{{ID: "discovery-page"}}); err != nil {
		t.Fatal(err)
	}
	pool.startCentralizedDBWriter()
	pool.Shutdown()
	if len(database.messages) != 1201 || len(database.snapshots) != 1 || database.snapshots[0].ID != "discovery-page" || database.closes != 1 {
		t.Fatalf("shutdown lost buffered rows: chats=%d snapshots=%d closes=%d", len(database.messages), len(database.snapshots), database.closes)
	}
	seen := map[string]bool{}
	for _, message := range database.messages {
		if seen[message.Message] || message.Tags["id"] != message.Message {
			t.Fatal("duplicated or corrupted chat")
		}
		seen[message.Message] = true
	}
	if pending := writer.Stats(); pending.Rows != 0 || pending.Bytes != 0 {
		t.Fatalf("buffer not drained: %+v", pending)
	}
}

func pipeIRC(t *testing.T) (*IRCConnection, net.Conn) {
	t.Helper()
	client, server := net.Pipe()
	conn := NewIRCConnection(nil, nil, make(chan *ChatMessage, 10))
	conn.Connection = client
	conn.Reader = bufio.NewReaderSize(client, 1024)
	conn.IsConnected.Store(true)
	t.Cleanup(conn.Shutdown)
	t.Cleanup(func() { _ = server.Close() })
	return conn, server
}

func TestReadRetainsPartialLineAcrossTimeout(t *testing.T) {
	conn, server := pipeIRC(t)
	go func() { _, _ = io.WriteString(server, "PING :partial") }()
	if _, err := conn.readLineWithTimeoutRobust(20 * time.Millisecond); err == nil {
		t.Fatal("expected timeout on incomplete line")
	}
	go func() { _, _ = io.WriteString(server, "-token\r\n") }()
	line, err := conn.readLineWithTimeoutRobust(time.Second)
	if err != nil || line != "PING :partial-token\r\n" {
		t.Fatalf("partial line lost: %q, %v", line, err)
	}
}

func TestReadRejectsOversizedLine(t *testing.T) {
	conn, server := pipeIRC(t)
	go func() { _, _ = io.WriteString(server, strings.Repeat("x", 65*1024)) }()
	if _, err := conn.readLineWithTimeoutRobust(time.Second); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected bounded line error, got %v", err)
	}
}

func TestShutdownInterruptsIdleReader(t *testing.T) {
	conn, _ := pipeIRC(t)
	done := make(chan error, 1)
	go func() { done <- conn.readChat() }()
	conn.Shutdown()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reader remained blocked after shutdown")
	}
}

func TestJoinFailureDoesNotClaimMembership(t *testing.T) {
	conn, server := pipeIRC(t)
	_ = server.Close()
	if err := conn.joinChannel([]string{"#room"}); err == nil {
		t.Fatal("expected write failure")
	}
	if conn.NChannels.Load() != 0 || len(conn.connectedChannels) != 0 {
		t.Fatal("failed JOIN counted as connected")
	}
}

func TestJoinWorkerStopsDuringPacing(t *testing.T) {
	conn, server := pipeIRC(t)
	channels := make([]string, 11)
	for i := range channels {
		channels[i] = fmt.Sprintf("#room%d", i)
	}
	conn.assignChannels(channels)
	ctx, cancel := context.WithCancel(conn.ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- conn.runChannelJoins(ctx) }()
	_ = server.SetReadDeadline(time.Now().Add(time.Second))
	line, err := bufio.NewReader(server).ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "JOIN #room0,") {
		t.Fatalf("missing first JOIN: %q, %v", line, err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("join error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("join worker ignored cancellation during pacing")
	}
	// Sent but not echoed back by Twitch: pending, not joined.
	if conn.NChannels.Load() != 0 || len(conn.joinAttempts) != 10 {
		t.Fatalf("joined=%d pending=%d, want 0 joined and 10 pending", conn.NChannels.Load(), len(conn.joinAttempts))
	}
}

func joinEcho(nick, channel string) string {
	return fmt.Sprintf(":%s!%s@%s.tmi.twitch.tv JOIN %s\r\n", nick, nick, nick, channel)
}

// Twitch drops JOINs over its per-IP limit without replying. Only the echo
// proves membership, and anything left unconfirmed must be sent again.
func TestUnconfirmedJoinIsRetried(t *testing.T) {
	conn, server := pipeIRC(t)
	go func() { _, _ = io.Copy(io.Discard, server) }()
	conn.assignChannels([]string{"#confirmed", "#dropped"})
	if err := conn.joinChannel([]string{"#confirmed", "#dropped"}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if batch, retryAt := conn.nextJoinBatch(now); len(batch) != 0 || retryAt.Before(now.Add(joinConfirmTimeout-time.Second)) {
		t.Fatalf("pending JOINs resent early: batch=%v retryAt=%v", batch, retryAt)
	}

	conn.trackMembership(joinEcho(conn.Nickname, "#confirmed"))
	conn.trackMembership(joinEcho("justinfan1", "#dropped")) // someone else's echo
	if !conn.connectedChannels["#confirmed"] || conn.connectedChannels["#dropped"] || conn.NChannels.Load() != 1 {
		t.Fatalf("membership = %v", conn.connectedChannels)
	}

	later := now.Add(joinConfirmTimeout + time.Second)
	if batch, _ := conn.nextJoinBatch(later); !reflect.DeepEqual(batch, []string{"#dropped"}) {
		t.Fatalf("retry batch = %v, want only the unconfirmed channel", batch)
	}
	if err := conn.joinChannel([]string{"#dropped"}); err != nil {
		t.Fatal(err)
	}
	if got := time.Until(conn.joinAttempts["#dropped"].retryAt); got < 2*joinConfirmTimeout-time.Second {
		t.Fatalf("second retry due in %v, want backoff to at least %v", got, 2*joinConfirmTimeout)
	}
}

func TestReaderConfirmsJoinsAndPartsFromTwitch(t *testing.T) {
	conn, server := pipeIRC(t)
	conn.assignChannels([]string{"#room"})
	done := make(chan error, 1)
	go func() { done <- conn.readChat() }()
	joined := func() bool {
		conn.channelsMutex.RLock()
		defer conn.channelsMutex.RUnlock()
		return conn.connectedChannels["#room"]
	}

	_, _ = io.WriteString(server, joinEcho(conn.Nickname, "#room"))
	for deadline := time.Now().Add(time.Second); !joined(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("JOIN echo not recorded")
		}
	}
	_, _ = io.WriteString(server, fmt.Sprintf(":%s!%s@%s.tmi.twitch.tv PART #room\r\n", conn.Nickname, conn.Nickname, conn.Nickname))
	for deadline := time.Now().Add(time.Second); joined(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("PART not recorded")
		}
	}
	conn.Shutdown()
	<-done
	if batch, _ := conn.nextJoinBatch(time.Now().Add(joinConfirmTimeout + time.Second)); !reflect.DeepEqual(batch, []string{"#room"}) {
		t.Fatalf("parted channel not rejoined: %v", batch)
	}
}

// After a mass reconnect, channels that are streaming must not queue behind
// hundreds of offline ones.
func TestLiveChannelsJoinFirst(t *testing.T) {
	conn := NewIRCConnection(nil, nil, nil)
	t.Cleanup(conn.Shutdown)
	channels := make([]string, 15)
	for i := range channels {
		channels[i] = fmt.Sprintf("#c%d", i)
	}
	conn.assignChannels(channels)
	conn.isLive = func(channel string) bool { return channel == "#c12" || channel == "#c14" }
	batch, _ := conn.nextJoinBatch(time.Now())
	want := []string{"#c12", "#c14", "#c0", "#c1", "#c2", "#c3", "#c4", "#c5", "#c6", "#c7"}
	if !reflect.DeepEqual(batch, want) {
		t.Fatalf("batch = %v, want %v", batch, want)
	}
}

func TestJoinLimiterIsSharedAcrossConnections(t *testing.T) {
	limiter := newJoinLimiter(1000) // one JOIN per millisecond
	start := time.Now()
	for i := 0; i < 3; i++ {
		if !limiter.wait(context.Background(), joinChannelsPerCommand) {
			t.Fatal("wait failed")
		}
	}
	if elapsed := time.Since(start); elapsed < 2*joinChannelsPerCommand*time.Millisecond {
		t.Fatalf("30 JOINs released after %v, want at least 20ms at 1000/s", elapsed)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if limiter.wait(ctx, 1) {
		t.Fatal("wait ignored cancellation")
	}
}

func TestRediscoveryPreservesAssignmentsAndCapacity(t *testing.T) {
	conn := NewIRCConnection(nil, nil, nil)
	t.Cleanup(conn.Shutdown)
	conn.assignChannels([]string{"#original"})
	pool := &ConnectionPool{connections: []*IRCConnection{conn}, totalChannels: 2, channelsPerConnection: 2}
	if overflow := pool.assignDiscoveredChannels([]string{"#original", "#new", "#new", "#overflow"}); overflow != nil {
		t.Fatalf("MAX_CHANNELS overflow must be dropped, not grown into: %v", overflow)
	}
	if !reflect.DeepEqual(conn.Channels, []string{"#original", "#new"}) {
		t.Fatalf("assignments = %v", conn.Channels)
	}
	// A disconnected socket has no joined state but must retain its ownership.
	pool.assignDiscoveredChannels([]string{"#new", "#original"})
	if len(conn.Channels) != 2 {
		t.Fatalf("rediscovery duplicated disconnected assignments: %v", conn.Channels)
	}
}

// Without MAX_CHANNELS the pool must hand back what it could not place so the
// caller can start more connections for it.
func TestRediscoveryReportsOverflowWhenUncapped(t *testing.T) {
	conn := NewIRCConnection(nil, nil, nil)
	t.Cleanup(conn.Shutdown)
	pool := &ConnectionPool{connections: []*IRCConnection{conn}, channelsPerConnection: 1}
	overflow := pool.assignDiscoveredChannels([]string{"#a", "#b", "#b", "#c"})
	if !reflect.DeepEqual(conn.Channels, []string{"#a"}) {
		t.Fatalf("assignments = %v", conn.Channels)
	}
	if !reflect.DeepEqual(overflow, []string{"#b", "#c"}) {
		t.Fatalf("overflow = %v, want the channels needing new connections", overflow)
	}
}

func TestReconnectRejoinsRediscoveredChannels(t *testing.T) {
	firstJoin := make(chan struct{})
	requestReconnect := make(chan struct{})
	secondJoin := make(chan string, 1)
	var connectionNumber int
	var mu sync.Mutex
	srv := startFakeIRC(t, func(_ *fakeIRC, socket net.Conn) {
		mu.Lock()
		connectionNumber++
		number := connectionNumber
		mu.Unlock()
		reader := bufio.NewReader(socket)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			if !strings.HasPrefix(line, "JOIN ") {
				continue
			}
			if number == 1 {
				close(firstJoin)
				<-requestReconnect
				_, _ = io.WriteString(socket, ":tmi.twitch.tv RECONNECT\r\n")
			} else {
				secondJoin <- line
			}
			_, _ = io.Copy(io.Discard, socket)
			return
		}
	})
	defer srv.closeServer()
	host, port := srv.hostPort(t)
	conn, _ := stressConn(host, port, 10)
	ctx, cancel := context.WithCancel(context.Background())
	pool := &ConnectionPool{ctx: ctx, cancel: cancel, connections: []*IRCConnection{conn}, totalChannels: 2, channelsPerConnection: 2}
	done := make(chan struct{})
	go func() { pool.runConnection(0, conn, []string{"#original"}); close(done) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("connection owner did not stop")
		}
	}()
	select {
	case <-firstJoin:
	case <-time.After(time.Second):
		t.Fatal("initial JOIN not received")
	}
	pool.assignDiscoveredChannels([]string{"#new"})
	close(requestReconnect)
	select {
	case line := <-secondJoin:
		if line != "JOIN #original,#new\r\n" {
			t.Fatalf("reconnect lost assignments: %q", line)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("reconnect did not rejoin assigned channels")
	}
}
