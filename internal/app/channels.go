package app

import (
	"context"
	"fmt"
	"strings"
	"time"

	"poglytics-scraper/internal/metrics"
)

const (
	joinChannelsPerCommand = 10
	// Ten attempts every five seconds stays within the documented normal-account
	// rate of 20 per ten seconds. Anonymous connection limits are undocumented.
	joinInterval = 5 * time.Second
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
	select {
	case conn.joinUpdates <- struct{}{}:
	default:
	}
}

func (conn *IRCConnection) joinNewChannels(channels []string) {
	conn.assignChannels(channels)
	_ = conn.joinAssignedChannels(conn.ctx)
}

// runChannelJoins is started and awaited by the socket's owner. Rediscovery
// only updates assignments; it never starts another socket writer.
func (conn *IRCConnection) runChannelJoins(ctx context.Context) error {
	for {
		if err := conn.joinAssignedChannels(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-conn.joinUpdates:
		}
	}
}

func (conn *IRCConnection) joinAssignedChannels(ctx context.Context) error {
	conn.joinMu.Lock()
	defer conn.joinMu.Unlock()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		conn.channelsMutex.RLock()
		batch := make([]string, 0, joinChannelsPerCommand)
		for _, channel := range conn.Channels {
			if !conn.connectedChannels[channel] {
				batch = append(batch, channel)
				if len(batch) == joinChannelsPerCommand {
					break
				}
			}
		}
		conn.channelsMutex.RUnlock()
		if len(batch) == 0 {
			return nil
		}
		if !waitContext(ctx, time.Until(conn.nextJoinAt)) {
			return ctx.Err()
		}
		if err := conn.joinChannel(batch); err != nil {
			return err
		}
		conn.nextJoinAt = time.Now().Add(joinInterval)
	}
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

	// This records commands sent, not server-confirmed membership.
	conn.channelsMutex.Lock()
	defer conn.channelsMutex.Unlock()
	for _, channel := range channels {
		if !conn.connectedChannels[channel] {
			conn.connectedChannels[channel] = true
			conn.ListChannels = append(conn.ListChannels, channel)
			conn.NChannels.Add(1)
		}
		metrics.RecordChannelJoin(conn.ID, true)
	}
	return nil
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
