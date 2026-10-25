package db

import (
	"errors"
	"fmt"
	"sync"
	"time"
	"unsafe"
)

// BatchOptions controls the additional, in-memory database buffers. Legacy
// preserves the old 500 mixed messages / one-second tick and immediate snapshots.
type BatchOptions struct {
	Legacy           bool
	MessageRows      int
	MessageInterval  time.Duration
	EventRows        int
	EventInterval    time.Duration
	SnapshotRows     int
	SnapshotInterval time.Duration
	MaxBytes         int
}

func DefaultBatchOptions() BatchOptions {
	return BatchOptions{
		MessageRows: 5000, MessageInterval: 5 * time.Second,
		EventRows: 1000, EventInterval: 30 * time.Second,
		SnapshotRows: 1000, SnapshotInterval: 10 * time.Second,
		MaxBytes: 32 << 20,
	}
}

func (o BatchOptions) Validate() error {
	if o.MessageRows < 1 || o.EventRows < 1 || o.SnapshotRows < 1 || o.MaxBytes < 1 {
		return fmt.Errorf("batch row counts and byte limit must be positive")
	}
	if o.MessageInterval < time.Millisecond || o.EventInterval < time.Millisecond || o.SnapshotInterval < time.Millisecond {
		return fmt.Errorf("batch intervals must be at least 1ms")
	}
	return nil
}

type IngestSink interface {
	SaveChatMessageBatch(interface{}) error
	SaveStreamSnapshots([]*StreamSnapshot) error
}

type FlushStats struct {
	Table, Reason string
	Rows          int
	Duration      time.Duration
	OldestAge     time.Duration
	Err           error
}

type BufferStats struct {
	Rows, Bytes, PeakRows, PeakBytes int
}

type tableBuffer struct {
	messages  []*ChatMessage
	snapshots []*StreamSnapshot
	first     time.Time
	bytes     int
}

func (b *tableBuffer) rows() int { return len(b.messages) + len(b.snapshots) }

var ingestTables = [...]string{
	"text_messages", "subscriptions", "bans", "deleted_messages", "raids",
	"bits", "notices", "hosts", "other_messages", "stream_snapshots", "mixed",
}

// MessageTable is shared with the database routers so batching cannot separate
// messages differently from the table that ultimately stores them.
func MessageTable(messageType string) string {
	switch messageType {
	case "text_message":
		return "text_messages"
	case "subscription", "resub", "subscription_gift", "mystery_subscription_gift":
		return "subscriptions"
	case "ban", "timeout":
		return "bans"
	case "delete_message", "clear_chat":
		return "deleted_messages"
	case "raid":
		return "raids"
	case "bits", "bits_badge_tier":
		return "bits"
	case "notice", "user_notice":
		return "notices"
	case "host_target":
		return "hosts"
	default:
		return "other_messages"
	}
}

// IngestWriter serializes chat and snapshot writes with backpressure. Call
// FlushDue regularly (the scraper uses a 100ms tick), and Close after producers
// stop. Save methods transfer ownership of rows, including maps and slices;
// callers must not mutate them afterward. The input slice itself may be reused.
// A successful Save can mean buffered: only Observe reports persisted rows.
// Observe runs under the writer lock and must not call back into the writer.
type IngestWriter struct {
	mu         sync.Mutex
	sink       IngestSink
	options    BatchOptions
	buffers    map[string]*tableBuffer
	observe    func(FlushStats)
	stats      BufferStats
	legacyTick time.Time
	closed     bool
	closeErr   error
	now        func() time.Time
}

func NewIngestWriter(sink IngestSink, options BatchOptions, observe func(FlushStats)) (*IngestWriter, error) {
	if sink == nil {
		return nil, fmt.Errorf("ingest sink is required")
	}
	if err := options.Validate(); err != nil {
		return nil, err
	}
	w := &IngestWriter{sink: sink, options: options, observe: observe, buffers: make(map[string]*tableBuffer), now: time.Now}
	for _, table := range ingestTables {
		w.buffers[table] = &tableBuffer{}
	}
	w.legacyTick = w.now().Add(time.Second)
	return w, nil
}

func (w *IngestWriter) SaveChatMessageBatch(value interface{}) error {
	messages, ok := value.([]*ChatMessage)
	if !ok {
		return fmt.Errorf("expected []*db.ChatMessage")
	}
	// Validate the entire batch before accepting ownership of any of it.
	for _, message := range messages {
		if message == nil {
			return fmt.Errorf("nil chat message")
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("ingest writer is closed")
	}
	var result error
	for _, message := range messages {
		table := MessageTable(message.MessageType)
		if w.options.Legacy {
			table = "mixed"
		}
		bytes := messageBytes(message)
		result = errors.Join(result, w.makeRoom(bytes))
		b := w.buffers[table]
		w.add(b, bytes)
		b.messages = append(b.messages, message)
		limit, _ := w.policy(table)
		if b.rows() >= limit || bytes > w.options.MaxBytes {
			result = errors.Join(result, w.flush(table, "rows"))
		}
	}
	return result
}

func (w *IngestWriter) SaveStreamSnapshots(snapshots []*StreamSnapshot) error {
	for _, snapshot := range snapshots {
		if snapshot == nil {
			return fmt.Errorf("nil stream snapshot")
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("ingest writer is closed")
	}
	var result error
	for _, snapshot := range snapshots {
		bytes := snapshotBytes(snapshot)
		result = errors.Join(result, w.makeRoom(bytes))
		b := w.buffers["stream_snapshots"]
		w.add(b, bytes)
		b.snapshots = append(b.snapshots, snapshot)
		if (!w.options.Legacy && b.rows() >= w.options.SnapshotRows) || bytes > w.options.MaxBytes {
			result = errors.Join(result, w.flush("stream_snapshots", "rows"))
		}
	}
	if w.options.Legacy {
		result = errors.Join(result, w.flush("stream_snapshots", "page"))
	}
	return result
}

func (w *IngestWriter) policy(table string) (int, time.Duration) {
	switch table {
	case "mixed":
		return 500, time.Second
	case "text_messages", "other_messages":
		return w.options.MessageRows, w.options.MessageInterval
	case "stream_snapshots":
		return w.options.SnapshotRows, w.options.SnapshotInterval
	default:
		return w.options.EventRows, w.options.EventInterval
	}
}

func (w *IngestWriter) add(b *tableBuffer, bytes int) {
	if b.rows() == 0 {
		b.first = w.now()
	}
	b.bytes += bytes
	w.stats.Rows++
	w.stats.Bytes += bytes
	w.stats.PeakRows = max(w.stats.PeakRows, w.stats.Rows)
	w.stats.PeakBytes = max(w.stats.PeakBytes, w.stats.Bytes)
}

// Flush the largest buffer first when the byte budget is reached. One oversized
// row is allowed transiently and written immediately; this isn't a process RSS cap.
func (w *IngestWriter) makeRoom(bytes int) error {
	var result error
	for w.stats.Rows > 0 && bytes > w.options.MaxBytes-w.stats.Bytes {
		largest := ingestTables[0]
		for _, table := range ingestTables {
			if w.buffers[table].bytes > w.buffers[largest].bytes {
				largest = table
			}
		}
		result = errors.Join(result, w.flush(largest, "memory"))
	}
	return result
}

func (w *IngestWriter) FlushDue() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var result error
	now := w.now()
	for _, table := range ingestTables {
		b := w.buffers[table]
		_, interval := w.policy(table)
		due := b.rows() > 0 && now.Sub(b.first) >= interval
		if table == "mixed" {
			due = !now.Before(w.legacyTick)
		}
		if due {
			result = errors.Join(result, w.flush(table, "age"))
		}
	}
	if !now.Before(w.legacyTick) {
		// Keep a fixed one-second cadence, as time.Ticker did in the old writer.
		missed := now.Sub(w.legacyTick)/time.Second + 1
		w.legacyTick = w.legacyTick.Add(missed * time.Second)
	}
	return result
}

// Close drains every table, including after a failed flush, and seals the writer.
// Failed batches are reported and released, never retried blindly: a database
// error can follow a successful commit, and the schema has no deduplication key.
func (w *IngestWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.closed {
		w.closed = true
		for _, table := range ingestTables {
			w.closeErr = errors.Join(w.closeErr, w.flush(table, "shutdown"))
		}
	}
	return w.closeErr
}

func (w *IngestWriter) Stats() BufferStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stats
}

func (w *IngestWriter) flush(table, reason string) error {
	b := w.buffers[table]
	if b.rows() == 0 {
		return nil
	}
	start := w.now()
	var err error
	if table == "stream_snapshots" {
		err = w.sink.SaveStreamSnapshots(b.snapshots)
	} else {
		err = w.sink.SaveChatMessageBatch(b.messages)
	}
	if w.observe != nil {
		w.observe(FlushStats{Table: table, Reason: reason, Rows: b.rows(), Duration: w.now().Sub(start), OldestAge: w.now().Sub(b.first), Err: err})
	}
	w.stats.Rows -= b.rows()
	w.stats.Bytes -= b.bytes
	// Release backing arrays too, so a previous traffic spike doesn't retain them.
	*b = tableBuffer{}
	if err != nil {
		return fmt.Errorf("flush %s: %w", table, err)
	}
	return nil
}

func stringsBytes(values ...string) int {
	n := 0
	for _, s := range values {
		n += len(s)
	}
	return n
}

func messageBytes(m *ChatMessage) int {
	n := int(unsafe.Sizeof(*m)) + 8 + stringsBytes(m.Nickname, m.Message, m.Channel, m.MessageType,
		m.UserID, m.DisplayName, m.Color, m.SubPlan, m.SubPlanName, m.GifterName, m.GifterID,
		m.BanReason, m.TargetUser, m.TargetMessageID, m.RaiderName, m.NoticeMessageID, m.SystemMessage, m.RawMessage)
	n += 64 + len(m.Tags)*64 + len(m.Badges)*16
	for k, v := range m.Tags {
		n += len(k) + len(v)
	}
	return n + stringsBytes(m.Badges...)
}

func snapshotBytes(s *StreamSnapshot) int {
	return int(unsafe.Sizeof(*s)) + 8 + stringsBytes(s.ID, s.UserID, s.UserLogin, s.UserName,
		s.GameID, s.GameName, s.Type, s.Title, s.Language, s.ThumbnailURL) +
		16*(len(s.Tags)+len(s.TagIDs)) + stringsBytes(s.Tags...) + stringsBytes(s.TagIDs...)
}
