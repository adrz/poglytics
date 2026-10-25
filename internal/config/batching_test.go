package config

import (
	"testing"
	"time"
)

func TestBatchDefaultsAndOverrides(t *testing.T) {
	for _, backend := range []string{"clickhouse", "sqlite", "postgres"} {
		o, err := GetBatchOptions(map[string]string{"DB_TYPE": backend})
		if err != nil || o.Legacy != (backend != "clickhouse") {
			t.Fatalf("%s: %+v, %v", backend, o, err)
		}
	}
	t.Setenv("DB_BATCH_MODE", "legacy")
	t.Setenv("DB_BATCH_MESSAGE_ROWS", "2000")
	t.Setenv("DB_BATCH_EVENT_INTERVAL", "15s")
	env := map[string]string{"DB_TYPE": "clickhouse", "DB_BATCH_MODE": "table"}
	loadSystemEnv(env)
	o, err := GetBatchOptions(env)
	if err != nil || !o.Legacy || o.MessageRows != 2000 || o.EventInterval != 15*time.Second {
		t.Fatalf("override failed: %+v, %v", o, err)
	}
}

func TestInvalidBatchConfiguration(t *testing.T) {
	for key, value := range map[string]string{
		"DB_BATCH_MODE": "fast", "DB_BATCH_MESSAGE_ROWS": "0", "DB_BATCH_EVENT_ROWS": "-1",
		"DB_BATCH_SNAPSHOT_ROWS": "abc", "DB_BATCH_MAX_BYTES": "0",
		"DB_BATCH_MESSAGE_INTERVAL": "5", "DB_BATCH_EVENT_INTERVAL": "0s", "DB_BATCH_SNAPSHOT_INTERVAL": "-1s",
	} {
		if _, err := GetBatchOptions(map[string]string{key: value}); err == nil {
			t.Errorf("accepted invalid %s=%s", key, value)
		}
	}
}
