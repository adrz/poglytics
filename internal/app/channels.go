package app

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"poglytics-scraper/internal/metrics"
)

const (
	joinChannelsPerCommand = 10
	// Ten attempts every five seconds stays within the documented normal-account
	// rate of 20 per ten seconds. Anonymous connection limits are undocumented.
	joinInterval = 5 * time.Second
	// Twitch echoes a JOIN back within seconds. JOINs over its per-IP limit get no
	// reply at all, so an unconfirmed JOIN is sent again after this long, backing
	// off for channels that never confirm (suspended, banned or renamed).
	joinConfirmTimeout = time.Minute
	maxJoinRetryDelay  = 16 * time.Minute
)

// assignChannels keeps the desired assignment separate from the current socket's
// joined channels, so reconnects and rediscovery cannot lose pending work.
func (conn *IRCConnection) assignChannels(channels []string) {
	conn.channelsMutex.Lock()
	seen := make(map[string]bool, len(conn.Channels))
	for _, channel := range conn.Channels {
		seen[channel] = true
	}
	for _, channel := range channels {
		if !seen[channel] {
			conn.Channels = append(conn.Channels, channel)
			seen[channel] = true
		}
	}
	conn.channelsMutex.Unlock()
	conn.wakeJoinWorker()
}

func (conn *IRCConnection) wakeJoinWorker() {
	select {
	case conn.joinUpdates <- struct{}{}:
	default:
	}
}

func (conn *IRCConnection) joinNewChannels(channels []string) {
	conn.assignChannels(channels)
	_, _ = conn.joinAssignedChannels(conn.ctx)
}

// runChannelJoins is started and awaited by the socket's owner. Rediscovery
// only updates assignments; it never starts another socket writer.
func (conn *IRCConnection) runChannelJoins(ctx context.Context) error {
	for {
		retryAt, err := conn.joinAssignedChannels(ctx)
		if err != nil {
			return err
		}
		var retry <-chan time.Time
		var timer *time.Timer
		if !retryAt.IsZero() {
			timer = time.NewTimer(time.Until(retryAt))
			retry = timer.C
		}
		select {
		case <-ctx.Done():
		case <-conn.joinUpdates:
		case <-retry:
		}
		if timer != nil {
			timer.Stop()
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

// joinAssignedChannels sends JOINs for every assigned channel that is neither
// confirmed nor waiting on an earlier attempt. It returns when the earliest
// unconfirmed attempt is due again, or the zero time when none is outstanding.
func (conn *IRCConnection) joinAssignedChannels(ctx context.Context) (time.Time, error) {
	conn.joinMu.Lock()
	defer conn.joinMu.Unlock()

	for {
		if err := ctx.Err(); err != nil {
			return time.Time{}, err
		}
		batch, retryAt := conn.nextJoinBatch(time.Now())
		if len(batch) == 0 {
			return retryAt, nil
		}
		if !waitContext(ctx, time.Until(conn.nextJoinAt)) {
			return time.Time{}, ctx.Err()
		}
		if conn.joinLimiter != nil && !conn.joinLimiter.wait(ctx, len(batch)) {
			return time.Time{}, ctx.Err()
		}
		if err := conn.joinChannel(batch); err != nil {
			return time.Time{}, err
		}
		conn.nextJoinAt = time.Now().Add(joinInterval)
	}
}

// nextJoinBatch picks channels due a JOIN, live ones first so a mass rejoin
// restores active chats before idle ones. When nothing is due it reports when
// the earliest unconfirmed attempt will be.
func (conn *IRCConnection) nextJoinBatch(now time.Time) (batch []string, retryAt time.Time) {
	conn.channelsMutex.RLock()
	defer conn.channelsMutex.RUnlock()

	var idle []string
	for _, channel := range conn.Channels {
		if conn.connectedChannels[channel] {
			continue
		}
		if due := conn.joinAttempts[channel].retryAt; due.After(now) {
			if retryAt.IsZero() || due.Before(retryAt) {
				retryAt = due
			}
			continue
		}
		if conn.isLive != nil && conn.isLive(channel) {
			batch = append(batch, channel)
			if len(batch) == joinChannelsPerCommand {
				return batch, retryAt
			}
		} else if len(idle) < joinChannelsPerCommand {
			idle = append(idle, channel)
		}
	}
	batch = append(batch, idle[:min(len(idle), joinChannelsPerCommand-len(batch))]...)
	return batch, retryAt
}

func (conn *IRCConnection) joinChannel(channels []string) error {
	if len(channels) == 0 {
		return nil
	}
	if err := conn.send(fmt.Sprintf("JOIN %s", strings.Join(channels, ","))); err != nil {
		for range channels {
			metrics.RecordChannelJoin(conn.ID, false)
		}
		return err
	}

	// Membership is only claimed once Twitch echoes the JOIN (trackMembership).
	sentAt := time.Now()
	conn.channelsMutex.Lock()
	defer conn.channelsMutex.Unlock()
	for _, channel := range channels {
		attempt := conn.joinAttempts[channel]
		if attempt.count > 0 {
			metrics.ChannelJoinRetries.Inc()
		}
		attempt.count++
		attempt.retryAt = sentAt.Add(joinRetryDelay(attempt.count))
		conn.joinAttempts[channel] = attempt
		metrics.RecordChannelJoin(conn.ID, true)
	}
	return nil
}

func joinRetryDelay(attempts int) time.Duration {
	delay := joinConfirmTimeout
	for i := 1; i < attempts && delay < maxJoinRetryDelay; i++ {
		delay *= 2
	}
	return min(delay, maxJoinRetryDelay)
}

// trackMembership applies Twitch's echo of our own JOIN or PART, the only proof
// that a channel's chat is actually being delivered to this socket.
func (conn *IRCConnection) trackMembership(line string) {
	if !strings.HasPrefix(line, ":") || !strings.HasPrefix(line[1:], conn.Nickname+"!") {
		return
	}
	fields := strings.Fields(line)
	if len(fields) < 3 {
		return
	}
	channel := fields[2]

	conn.channelsMutex.Lock()
	defer conn.channelsMutex.Unlock()
	switch fields[1] {
	case "JOIN":
		delete(conn.joinAttempts, channel)
		if !conn.connectedChannels[channel] {
			conn.connectedChannels[channel] = true
			conn.ListChannels = append(conn.ListChannels, channel)
			conn.NChannels.Add(1)
		}
	case "PART":
		if conn.connectedChannels[channel] {
			delete(conn.connectedChannels, channel)
			conn.ListChannels = slices.DeleteFunc(conn.ListChannels, func(c string) bool { return c == channel })
			conn.NChannels.Add(-1)
		}
		// We never PART, so Twitch removed us; rejoin rather than go quiet.
		conn.joinAttempts[channel] = joinAttempt{count: 1, retryAt: time.Now().Add(joinConfirmTimeout)}
		conn.wakeJoinWorker()
	}
}

// joinLimiter spaces JOINs across every connection in the pool. Twitch limits
// JOINs per IP rather than per connection and silently drops the excess, which
// is how a mass RECONNECT used to lose most channels for good.
type joinLimiter struct {
	mu       sync.Mutex
	interval time.Duration
	next     time.Time
}

func newJoinLimiter(perSecond int) *joinLimiter {
	return &joinLimiter{interval: time.Second / time.Duration(perSecond)}
}

// wait reserves n JOINs in arrival order and blocks until they may be sent.
func (l *joinLimiter) wait(ctx context.Context, n int) bool {
	l.mu.Lock()
	start := time.Now()
	if l.next.After(start) {
		start = l.next
	}
	l.next = start.Add(time.Duration(n) * l.interval)
	l.mu.Unlock()
	return waitContext(ctx, time.Until(start))
}

func waitContext(ctx context.Context, delay time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}
	if delay <= 0 {
		return true
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}
