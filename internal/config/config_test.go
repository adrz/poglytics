package config

import (
	"os"
	"testing"
)

func TestLoadEnvOverridesFile(t *testing.T) {
	t.Chdir(t.TempDir())
	if err := os.WriteFile(".env", []byte("CLIENT_ID=file\nDB_PASSWORD=secret\nMAX_CHANNELS=10\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CLIENT_ID", "process")
	t.Setenv("DB_PASSWORD", "")
	t.Setenv("MAX_CHANNELS", "20")
	env, err := LoadEnv()
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"CLIENT_ID": "process", "DB_PASSWORD": "", "MAX_CHANNELS": "20"} {
		if env[key] != want {
			t.Errorf("%s = %q, want %q", key, env[key], want)
		}
	}
}

func TestLoadEnvRejectsUnmatchedQuote(t *testing.T) {
	t.Chdir(t.TempDir())
	if err := os.WriteFile(".env", []byte("CLIENT_ID='\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadEnv(); err == nil {
		t.Fatal("expected malformed quote error")
	}
}

func TestClickHouseDefaultPort(t *testing.T) {
	if got := GetDBConfig(map[string]string{"DB_TYPE": "clickhouse"})["port"]; got != "9000" {
		t.Fatalf("ClickHouse port = %s, want 9000", got)
	}
}
