package db

import (
	"sync"
	"testing"
	"time"
)

func TestSQLiteConcurrentChatAndStreamWrites(t *testing.T) {
	database, err := NewDatabase(&DBConfig{Type: "sqlite", Path: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(database.Close)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if err := database.SaveChatMessageBatch([]*ChatMessage{{
					MessageType: "text_message", Timestamp: time.Now(), Channel: "#room", Message: "hello",
				}}); err != nil {
					t.Error(err)
				}
				if err := database.SaveStreamSnapshots([]*StreamSnapshot{{
					ID: "stream", UserID: "viewer", UserLogin: "viewer", StartedAt: time.Now(), SnapshotTime: time.Now(),
				}}); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	for _, table := range []string{"text_messages", "stream_snapshots"} {
		var count int
		if err := database.GetDB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 80 {
			t.Errorf("%s contains %d/80 rows: %v", table, count, err)
		}
	}
}
