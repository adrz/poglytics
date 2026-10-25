package config

import (
	"fmt"
	"strconv"
	"time"

	"poglytics-scraper/internal/db"
)

func GetBatchOptions(env map[string]string) (db.BatchOptions, error) {
	o := db.DefaultBatchOptions()
	o.Legacy = GetDBType(env) != "clickhouse"
	switch env["DB_BATCH_MODE"] {
	case "":
	case "legacy":
		o.Legacy = true
	case "table":
		o.Legacy = false
	default:
		return o, fmt.Errorf("DB_BATCH_MODE must be table or legacy")
	}
	for name, target := range map[string]*int{
		"DB_BATCH_MESSAGE_ROWS": &o.MessageRows, "DB_BATCH_EVENT_ROWS": &o.EventRows,
		"DB_BATCH_SNAPSHOT_ROWS": &o.SnapshotRows, "DB_BATCH_MAX_BYTES": &o.MaxBytes,
	} {
		if value := env[name]; value != "" {
			n, err := strconv.Atoi(value)
			if err != nil || n < 1 {
				return o, fmt.Errorf("%s must be a positive integer", name)
			}
			*target = n
		}
	}
	for name, target := range map[string]*time.Duration{
		"DB_BATCH_MESSAGE_INTERVAL": &o.MessageInterval, "DB_BATCH_EVENT_INTERVAL": &o.EventInterval,
		"DB_BATCH_SNAPSHOT_INTERVAL": &o.SnapshotInterval,
	} {
		if value := env[name]; value != "" {
			d, err := time.ParseDuration(value)
			if err != nil || d < time.Millisecond {
				return o, fmt.Errorf("%s must be a duration of at least 1ms", name)
			}
			*target = d
		}
	}
	return o, o.Validate()
}
