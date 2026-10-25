package config

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// LoadEnv loads environment variables from .env file or system environment
func LoadEnv() (map[string]string, error) {
	env := make(map[string]string)

	file, err := os.Open(".env")
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("open .env: %w", err)
		}
		loadSystemEnv(env)
		return env, nil
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			// Remove quotes if present
			if strings.HasPrefix(value, "\"") || strings.HasPrefix(value, "'") {
				if len(value) < 2 || value[len(value)-1] != value[0] {
					return nil, fmt.Errorf("unmatched quote in .env for %s", key)
				}
				value = value[1 : len(value)-1]
			}
			env[key] = value
		}
	}

	loadSystemEnv(env)
	return env, scanner.Err()
}

// Process values override file defaults, including explicitly empty values.
func loadSystemEnv(env map[string]string) {
	for _, key := range []string{
		"CLIENT_ID", "CLIENT_SECRET", "MAX_CHANNELS", "CHANNELS_PER_CONNECTION", "JOINS_PER_SECOND", "METRICS_PORT",
		"DB_TYPE", "DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME", "DB_SSLMODE", "DB_PATH",
		"DB_BATCH_MODE", "DB_BATCH_MESSAGE_ROWS", "DB_BATCH_EVENT_ROWS", "DB_BATCH_SNAPSHOT_ROWS",
		"DB_BATCH_MESSAGE_INTERVAL", "DB_BATCH_EVENT_INTERVAL", "DB_BATCH_SNAPSHOT_INTERVAL", "DB_BATCH_MAX_BYTES",
	} {
		if value, ok := os.LookupEnv(key); ok {
			env[key] = value
		}
	}
}

// GetDBType returns the database type from environment (defaults to sqlite)
func GetDBType(env map[string]string) string {
	if dbType, ok := env["DB_TYPE"]; ok {
		return strings.ToLower(dbType)
	}
	return "sqlite"
}

// GetDBConfig returns database configuration from environment variables
func GetDBConfig(env map[string]string) map[string]string {
	dbConfig := make(map[string]string)

	dbConfig["type"] = GetDBType(env)
	dbConfig["host"] = getEnvOrDefault(env, "DB_HOST", "localhost")
	port := "5432"
	if dbConfig["type"] == "clickhouse" {
		port = "9000"
	}
	dbConfig["port"] = getEnvOrDefault(env, "DB_PORT", port)
	dbConfig["user"] = getEnvOrDefault(env, "DB_USER", "")
	dbConfig["password"] = getEnvOrDefault(env, "DB_PASSWORD", "")
	dbConfig["database"] = getEnvOrDefault(env, "DB_NAME", "twitch_chat")
	dbConfig["sslmode"] = getEnvOrDefault(env, "DB_SSLMODE", "disable")
	dbConfig["path"] = getEnvOrDefault(env, "DB_PATH", "./chat_messages.db")

	return dbConfig
}

func getEnvOrDefault(env map[string]string, key, defaultValue string) string {
	if value, ok := env[key]; ok && value != "" {
		return value
	}
	return defaultValue
}
