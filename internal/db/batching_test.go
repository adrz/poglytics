package db

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type ingestRecorder struct {
	chats [][]*ChatMessage
	pages [][]*StreamSnapshot
	fail  bool
}

func (r *ingestRecorder) SaveChatMessageBatch(value interface{}) error {
	r.chats = append(r.chats, append([]*ChatMessage(nil), value.([]*ChatMessage)...))
	if r.fail {
		return errors.New("ambiguous commit")
	}
	return nil
}
func (r *ingestRecorder) SaveStreamSnapshots(rows []*StreamSnapshot) error {
	r.pages = append(r.pages, append([]*StreamSnapshot(nil), rows...))
	return nil
}

func testIngestWriter(t *testing.T, o BatchOptions, r *ingestRecorder) (*IngestWriter, *time.Time) {
	t.Helper()
	w, err := NewIngestWriter(r, o, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	w.now = func() time.Time { return now }
	w.legacyTick = now.Add(time.Second)
	return w, &now
}

func TestIngestSeparatesTablesAndFlushesOldestRowOnTime(t *testing.T) {
	r := &ingestRecorder{}
	o := DefaultBatchOptions()
	o.MessageRows = 3
	w, now := testIngestWriter(t, o, r)
	input := []*ChatMessage{
		{MessageType: "text_message", Message: "first", Tags: map[string]string{"id": "1"}},
		{MessageType: "ban", TargetUser: "banned"},
		{MessageType: "text_message", Message: "second"},
		{MessageType: "text_message", Message: "third"},
	}
	if err := w.SaveChatMessageBatch(input); err != nil {
		t.Fatal(err)
	}
	clear(input) // The caller owns the slice, the writer owns the rows.
	if len(r.chats) != 1 || len(r.chats[0]) != 3 || r.chats[0][0].Tags["id"] != "1" {
		t.Fatalf("wrong threshold flush: %+v", r.chats)
	}
	*now = now.Add(29 * time.Second)
	_ = w.SaveChatMessageBatch([]*ChatMessage{{MessageType: "timeout", TargetUser: "timed_out"}})
	_ = w.FlushDue()
	if len(r.chats) != 1 {
		t.Fatal("rare event flushed early")
	}
	*now = now.Add(time.Second)
	_ = w.FlushDue()
	if len(r.chats) != 2 || len(r.chats[1]) != 2 || r.chats[1][0].TargetUser != "banned" {
		t.Fatal("new rows postponed the oldest event or changed its order")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if s := w.Stats(); s.Rows != 0 || s.Bytes != 0 {
		t.Fatalf("buffer not drained: %+v", s)
	}
}

func TestSnapshotsCoalescePagesAndFlushOnTimeOrClose(t *testing.T) {
	r := &ingestRecorder{}
	o := DefaultBatchOptions()
	o.SnapshotRows = 4
	w, now := testIngestWriter(t, o, r)
	for i := 0; i < 2; i++ {
		if err := w.SaveStreamSnapshots([]*StreamSnapshot{{ID: fmt.Sprint(i * 2)}, {ID: fmt.Sprint(i*2 + 1)}}); err != nil {
			t.Fatal(err)
		}
	}
	if len(r.pages) != 1 || len(r.pages[0]) != 4 {
		t.Fatal("pages were not combined")
	}
	_ = w.SaveStreamSnapshots([]*StreamSnapshot{{ID: "age"}})
	*now = now.Add(10 * time.Second)
	_ = w.FlushDue()
	_ = w.SaveStreamSnapshots([]*StreamSnapshot{{ID: "shutdown"}})
	_ = w.Close()
	if len(r.pages) != 3 || r.pages[1][0].ID != "age" || r.pages[2][0].ID != "shutdown" {
		t.Fatal("snapshot drain lost rows")
	}
}

func TestIngestMemoryPressureAndOversizedRow(t *testing.T) {
	r := &ingestRecorder{}
	o := DefaultBatchOptions()
	message := &ChatMessage{MessageType: "text_message", Message: strings.Repeat("x", 2048)}
	o.MaxBytes = messageBytes(message) * 2
	w, _ := testIngestWriter(t, o, r)
	for i := 0; i < 7; i++ {
		if err := w.SaveChatMessageBatch([]*ChatMessage{message}); err != nil {
			t.Fatal(err)
		}
		if s := w.Stats(); s.Bytes > o.MaxBytes {
			t.Fatalf("byte budget exceeded: %+v", s)
		}
	}
	_ = w.SaveChatMessageBatch([]*ChatMessage{{MessageType: "notice", Message: strings.Repeat("x", o.MaxBytes+1)}})
	if s := w.Stats(); s.Rows != 0 {
		t.Fatal("oversized row retained")
	}
	_ = w.Close()
	n := 0
	for _, batch := range r.chats {
		n += len(batch)
	}
	if n != 8 {
		t.Fatalf("memory flush lost rows: %d", n)
	}
}

func TestIngestCloseContinuesAfterFailureAndDoesNotRetry(t *testing.T) {
	r := &ingestRecorder{fail: true}
	w, _ := testIngestWriter(t, DefaultBatchOptions(), r)
	_ = w.SaveChatMessageBatch([]*ChatMessage{{MessageType: "ban"}, {MessageType: "text_message"}})
	_ = w.SaveStreamSnapshots([]*StreamSnapshot{{ID: "last"}})
	if err := w.Close(); err == nil {
		t.Fatal("failure hidden")
	}
	if err := w.Close(); err == nil {
		t.Fatal("repeated Close lost error")
	}
	if len(r.chats) != 2 || len(r.pages) != 1 {
		t.Fatal("failed table prevented drain or was retried")
	}
	if err := w.SaveChatMessageBatch([]*ChatMessage{{}}); err == nil {
		t.Fatal("accepted row after close")
	}
	if err := w.SaveStreamSnapshots([]*StreamSnapshot{{}}); err == nil {
		t.Fatal("accepted snapshot after close")
	}
	if s := w.Stats(); s.Rows != 0 || s.Bytes != 0 {
		t.Fatal("failed batch retains memory")
	}
}

func TestIngestConcurrentProducersAndFlush(t *testing.T) {
	r := &ingestRecorder{}
	w, err := NewIngestWriter(r, DefaultBatchOptions(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				id := fmt.Sprintf("%d-%d", worker, i)
				if err := w.SaveChatMessageBatch([]*ChatMessage{{MessageType: "text_message", Message: id}}); err != nil {
					t.Error(err)
				}
				if err := w.SaveStreamSnapshots([]*StreamSnapshot{{ID: id}}); err != nil {
					t.Error(err)
				}
				if err := w.FlushDue(); err != nil {
					t.Error(err)
				}
			}
		}(worker)
	}
	wg.Wait()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	messages, snapshots := map[string]int{}, map[string]int{}
	for _, batch := range r.chats {
		for _, row := range batch {
			messages[row.Message]++
		}
	}
	for _, batch := range r.pages {
		for _, row := range batch {
			snapshots[row.ID]++
		}
	}
	if len(messages) != 800 || !reflect.DeepEqual(messages, snapshots) {
		t.Fatal("concurrent writes lost data")
	}
	for _, count := range messages {
		if count != 1 {
			t.Fatal("duplicate row")
		}
	}
}

func TestLegacyIngestUsesMixed500RowsAndImmediatePages(t *testing.T) {
	r := &ingestRecorder{}
	o := DefaultBatchOptions()
	o.Legacy = true
	w, now := testIngestWriter(t, o, r)
	rows := make([]*ChatMessage, 501)
	for i := range rows {
		rows[i] = &ChatMessage{MessageType: []string{"ban", "text_message"}[i%2]}
	}
	_ = w.SaveChatMessageBatch(rows)
	if len(r.chats) != 1 || len(r.chats[0]) != 500 {
		t.Fatal("legacy row threshold changed")
	}
	*now = now.Add(time.Second)
	_ = w.FlushDue()
	if len(r.chats) != 2 || len(r.chats[1]) != 1 {
		t.Fatal("legacy timer did not drain")
	}
	_ = w.SaveStreamSnapshots([]*StreamSnapshot{{}, {}})
	_ = w.SaveStreamSnapshots([]*StreamSnapshot{{}})
	if len(r.pages) != 2 {
		t.Fatal("legacy pages not saved immediately")
	}
}

func TestInvalidRowsAreRejectedBeforeAcceptingBatch(t *testing.T) {
	w, _ := testIngestWriter(t, DefaultBatchOptions(), &ingestRecorder{})
	if err := w.SaveChatMessageBatch([]*ChatMessage{{}, nil}); err == nil {
		t.Fatal("nil message accepted")
	}
	if err := w.SaveStreamSnapshots([]*StreamSnapshot{{}, nil}); err == nil {
		t.Fatal("nil snapshot accepted")
	}
	if s := w.Stats(); s.Rows != 0 {
		t.Fatal("invalid batch was partially accepted")
	}
}

func TestIngestPersistsAllEventVariantsInTheirExistingTables(t *testing.T) {
	database, err := NewDatabase(&DBConfig{Type: "sqlite", Path: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	w, err := NewIngestWriter(database, DefaultBatchOptions(), nil)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string][]string{
		"text_messages":    {"text_message"},
		"subscriptions":    {"subscription", "resub", "subscription_gift", "mystery_subscription_gift"},
		"bans":             {"ban", "timeout"},
		"deleted_messages": {"delete_message", "clear_chat"},
		"raids":            {"raid"}, "bits": {"bits", "bits_badge_tier"},
		"notices": {"notice", "user_notice"}, "hosts": {"host_target"},
		"other_messages": {"room_state", "user_state", "future_event"},
	}
	for _, kinds := range expected {
		for _, kind := range kinds {
			if err := w.SaveChatMessageBatch([]*ChatMessage{{MessageType: kind, Timestamp: time.Now(), Channel: "#test", Message: kind, RawMessage: kind}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for table, kinds := range expected {
		var count int
		if err := database.GetDB().QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != len(kinds) {
			t.Errorf("%s: got %d rows, want %d", table, count, len(kinds))
		}
	}
}
