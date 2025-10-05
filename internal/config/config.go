package config

import (
	"bufio"
	"os"
	"strings"
)

// LoadEnv loads environment variables from .env file or system environment
func LoadEnv() (map[string]string, error) {
	env := make(map[string]string)

	file, err := os.Open(".env")
	if err != nil {
		// Try to get from environment variables if .env file doesn't exist
		if clientID := os.Getenv("CLIENT_ID"); clientID != "" {
			env["CLIENT_ID"] = clientID
		}
		if clientSecret := os.Getenv("CLIENT_SECRET"); clientSecret != "" {
			env["CLIENT_SECRET"] = clientSecret
		}
		// Load database configuration from environment
		loadDBEnvVars(env)
		return env, nil
	}
	defer file.Close()

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
			if (strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"")) ||
				(strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'")) {
				value = value[1 : len(value)-1]
			}
			env[key] = value
		}
	}

	return env, scanner.Err()
}

// loadDBEnvVars loads database configuration from environment variables
func loadDBEnvVars(env map[string]string) {
	dbVars := []string{
		"DB_TYPE",
		"DB_HOST",
		"DB_PORT",
		"DB_USER",
		"DB_PASSWORD",
		"DB_NAME",
		"DB_SSLMODE",
		"DB_PATH",
	}

	for _, varName := range dbVars {
		if value := os.Getenv(varName); value != "" {
			env[varName] = value
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
	dbConfig["port"] = getEnvOrDefault(env, "DB_PORT", "5432")
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
