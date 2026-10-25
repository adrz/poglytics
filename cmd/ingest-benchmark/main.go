// ingest-benchmark replays deterministic synthetic data through the production
// writer. scripts/benchmark.py supplies a fresh, disposable ClickHouse container.
package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"poglytics-scraper/internal/db"
)

type tableResult struct {
	Rows, DistinctRows, NewParts, InsertedRows, Merges uint64
	Checksum                                           string
	MaxBufferedAgeSeconds                              float64
}

type result struct {
	Mode, Database, ClickHouseVersion, GoVersion                         string
	DurationSeconds, IngestSeconds, TailSeconds                          float64
	Rate, SnapshotRate, ExpectedRows, WriteErrors                        int
	DBCPUSeconds, DBTailCPUSeconds, DBIngestCPUPercent, WriterCPUSeconds float64
	MaxSubmissionLagMS                                                   float64
	Buffer                                                               db.BufferStats
	Inserts                                                              uint64
	InsertCPUSeconds, MergeCPUSeconds                                    float64
	ActiveMerges                                                         uint64
	Tables                                                               map[string]*tableResult
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	port := flag.String("port", "", "native port of the disposable benchmark container")
	container := flag.String("container", "", "disposable container for cgroup CPU measurements")
	database := flag.String("database", "poglytics_bench_run", "new database; must start with poglytics_bench_")
	mode := flag.String("mode", "table", "table or legacy")
	seconds := flag.Int("seconds", 60, "paced ingestion duration")
	rate := flag.Int("rate", 600, "chat events per second")
	snapshotRate := flag.Int("snapshot-rate", 60, "snapshots per second, delivered in pages of 100")
	flag.Parse()
	if *port == "" || *container == "" || !regexp.MustCompile(`^poglytics_bench_[a-z0-9_]+$`).MatchString(*database) {
		return fmt.Errorf("explicit benchmark port/container and a poglytics_bench_ database are required")
	}
	if (*mode != "legacy" && *mode != "table") || *seconds < 1 || *rate < 1 || *snapshotRate < 0 {
		return fmt.Errorf("invalid benchmark parameters")
	}
	// A benchmark-only credential; never read the scraper's .env or OAuth secrets.
	dsn := (&url.URL{Scheme: "clickhouse", User: url.UserPassword("default", "benchmark"), Host: net.JoinHostPort("127.0.0.1", *port), Path: "/default"}).String()
	admin, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return err
	}
	defer admin.Close()
	// No IF NOT EXISTS: refuse to reuse a database that could contain other data.
	if _, err := admin.Exec("CREATE DATABASE " + *database); err != nil {
		return err
	}
	sink, err := db.NewDatabase(&db.DBConfig{Type: "clickhouse", Host: "127.0.0.1", Port: *port, User: "default", Password: "benchmark", Database: *database})
	if err != nil {
		return err
	}
	defer sink.Close()
	r := result{Mode: *mode, Database: *database, GoVersion: runtime.Version(), Rate: *rate, SnapshotRate: *snapshotRate, DurationSeconds: float64(*seconds), TailSeconds: 10, Tables: map[string]*tableResult{}}
	if err := admin.QueryRow("SELECT version()").Scan(&r.ClickHouseVersion); err != nil {
		return err
	}
	messages, snapshots, expected := workload(*seconds, *rate, *snapshotRate)
	for table, count := range expected {
		r.Tables[table] = &tableResult{}
		r.ExpectedRows += count
	}
	o := db.DefaultBatchOptions()
	o.Legacy = *mode == "legacy"
	// Allow initial DDL work to settle before starting writer timers and sampling CPU.
	time.Sleep(3 * time.Second)
	writer, err := db.NewIngestWriter(sink, o, func(s db.FlushStats) {
		if s.Err != nil {
			r.WriteErrors++
		}
		if t := r.Tables[s.Table]; t != nil {
			t.MaxBufferedAgeSeconds = max(t.MaxBufferedAgeSeconds, s.OldestAge.Seconds())
		}
	})
	if err != nil {
		return err
	}
	defer writer.Close()
	dbStart, err := containerCPU(*container)
	if err != nil {
		return err
	}
	writerStart, err := processCPU()
	if err != nil {
		return err
	}
	start := time.Now()
	nextSnapshot := 0
	for tick := 1; tick <= *seconds*10; tick++ {
		due := start.Add(time.Duration(tick) * 100 * time.Millisecond)
		if pause := time.Until(due); pause > 0 {
			time.Sleep(pause)
		}
		r.MaxSubmissionLagMS = max(r.MaxSubmissionLagMS, float64(time.Since(due).Microseconds())/1000)
		if err := writer.SaveChatMessageBatch(messages[(tick-1)*(*rate)/10 : tick*(*rate)/10]); err != nil {
			return err
		}
		available := tick * (*snapshotRate) / 10
		for available-nextSnapshot >= 100 {
			if err := writer.SaveStreamSnapshots(snapshots[nextSnapshot : nextSnapshot+100]); err != nil {
				return err
			}
			nextSnapshot += 100
		}
		if err := writer.FlushDue(); err != nil {
			return err
		}
	}
	if err := writer.SaveStreamSnapshots(snapshots[nextSnapshot:]); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	r.IngestSeconds = time.Since(start).Seconds()
	writerEnd, err := processCPU()
	if err != nil {
		return err
	}
	r.WriterCPUSeconds = writerEnd - writerStart
	dbEnd, err := containerCPU(*container)
	if err != nil {
		return err
	}
	r.DBCPUSeconds = dbEnd - dbStart
	r.DBIngestCPUPercent = 100 * r.DBCPUSeconds / r.IngestSeconds // 100% = one CPU core.
	r.Buffer = writer.Stats()
	time.Sleep(time.Duration(r.TailSeconds) * time.Second)
	dbTail, err := containerCPU(*container)
	if err != nil {
		return err
	}
	r.DBTailCPUSeconds = dbTail - dbEnd
	if err := admin.QueryRow("SELECT count() FROM system.merges WHERE database = ?", *database).Scan(&r.ActiveMerges); err != nil {
		return err
	}
	// CPU measurements end before verification and forced log flushing.
	if _, err := admin.Exec("SYSTEM FLUSH LOGS"); err != nil {
		return err
	}
	if err := admin.QueryRow(`SELECT count(), sum(ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) / 1000000.0
		FROM system.query_log WHERE current_database = ? AND query_kind = 'Insert' AND type = 'QueryFinish'`, *database).Scan(&r.Inserts, &r.InsertCPUSeconds); err != nil {
		return err
	}
	if err := admin.QueryRow(`SELECT sum(ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) / 1000000.0
		FROM system.part_log WHERE database = ? AND event_type = 'MergeParts' AND error = 0`, *database).Scan(&r.MergeCPUSeconds); err != nil {
		return err
	}
	for table, count := range expected {
		t := r.Tables[table]
		if err := admin.QueryRow(`SELECT countIf(event_type = 'NewPart'), sumIf(rows, event_type = 'NewPart'), countIf(event_type = 'MergeParts')
			FROM system.part_log WHERE database = ? AND table = ? AND error = 0`, *database, table).Scan(&t.NewParts, &t.InsertedRows, &t.Merges); err != nil {
			return err
		}
		columns, err := hashColumns(admin, *database, table)
		if err != nil {
			return err
		}
		key := "raw_message"
		if table == "text_messages" {
			key = "message"
		}
		if table == "stream_snapshots" {
			key = "stream_id"
		}
		q := "SELECT count(), uniqExact(" + key + "), toString(groupBitXor(cityHash64(" + columns + "))) FROM " + *database + "." + table
		if err := admin.QueryRow(q).Scan(&t.Rows, &t.DistinctRows, &t.Checksum); err != nil {
			return err
		}
		if t.Rows != uint64(count) || t.DistinctRows != uint64(count) || t.InsertedRows != uint64(count) {
			return fmt.Errorf("%s: expected %d distinct rows, got %+v", table, count, t)
		}
	}
	if r.Buffer.Rows != 0 || r.WriteErrors != 0 {
		return fmt.Errorf("writer did not drain cleanly")
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(r)
}

func hashColumns(conn *sql.DB, database, table string) (string, error) {
	rows, err := conn.Query("SELECT name FROM system.columns WHERE database = ? AND table = ? AND name != 'id' ORDER BY position", database, table)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return "", err
		}
		names = append(names, "`"+name+"`")
	}
	return strings.Join(names, ","), rows.Err()
}

func containerCPU(container string) (float64, error) {
	out, err := exec.Command("docker", "exec", container, "cat", "/sys/fs/cgroup/cpu.stat").Output()
	if err != nil {
		return 0, fmt.Errorf("read container CPU (cgroup v2 required): %w", err)
	}
	fields := strings.Fields(string(out))
	for i := 0; i+1 < len(fields); i += 2 {
		if fields[i] == "usage_usec" {
			n, err := strconv.ParseFloat(fields[i+1], 64)
			return n / 1e6, err
		}
	}
	return 0, fmt.Errorf("container CPU counter not found")
}

func processCPU() (float64, error) {
	var r syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
		return 0, err
	}
	return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6, nil
}

func workload(seconds, rate, snapshotRate int) ([]*db.ChatMessage, []*db.StreamSnapshot, map[string]int) {
	rng := rand.New(rand.NewSource(42))
	base := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	counts := map[string]int{}
	messages := make([]*db.ChatMessage, seconds*rate)
	for i := range messages {
		n := rng.Intn(10000)
		kind := "host_target"
		switch {
		case n < 6500:
			kind = "text_message"
		case n < 9880:
			kind = "room_state"
		case n < 9920:
			kind = "subscription"
		case n < 9950:
			kind = "notice"
		case n < 9965:
			kind = "ban"
		case n < 9975:
			kind = "delete_message"
		case n < 9985:
			kind = "raid"
		case n < 9995:
			kind = "bits"
		}
		id := strconv.Itoa(i)
		message := fmt.Sprintf("Synthetic message %d: hello chat %s", i, strings.Repeat("example ", 5+rng.Intn(15)))
		m := &db.ChatMessage{MessageType: kind, Timestamp: base.Add(time.Duration(i) * time.Second / time.Duration(rate)),
			Channel: fmt.Sprintf("#room_%d", rng.Intn(100)), Nickname: "viewer_" + id, DisplayName: "Viewer" + id, UserID: id,
			Message: message, RawMessage: "@id=" + id + ";display-name=Viewer" + id + ";badges=subscriber/12 :viewer!viewer@viewer.tmi.twitch.tv " + kind + " #room :" + message,
			Tags: map[string]string{"id": id, "user-id": id}, Badges: []string{"subscriber/12"},
			SubPlan: "1000", SubPlanName: "Tier 1", Months: 12, CumulativeMonths: 12, StreakMonths: 3,
			TargetUser: "target_" + id, TargetMessageID: id, BanDuration: 600, BanReason: "synthetic",
			RaiderName: "raider_" + id, ViewerCount: 42, BitsAmount: 100, NoticeMessageID: "synthetic_notice", SystemMessage: "synthetic event " + id}
		messages[i] = m
		counts[db.MessageTable(kind)]++
	}
	snapshots := make([]*db.StreamSnapshot, seconds*snapshotRate)
	for i := range snapshots {
		id := strconv.Itoa(i)
		snapshots[i] = &db.StreamSnapshot{ID: id, UserID: id, UserLogin: "streamer_" + id, UserName: "Streamer" + id,
			GameID: "1", GameName: "Synthetic game", Type: "live", Title: "Synthetic stream " + id, ViewerCount: 100,
			StartedAt: base.Add(-time.Hour), SnapshotTime: base.Add(time.Duration(i) * time.Second / time.Duration(snapshotRate)),
			Language: "en", ThumbnailURL: "https://example.invalid/" + id, Tags: []string{"English", "Gaming"}, TagIDs: []string{"1", "2"}}
	}
	if len(snapshots) > 0 {
		counts["stream_snapshots"] = len(snapshots)
	}
	return messages, snapshots, counts
}
