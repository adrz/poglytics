package main

import (
	"fmt"
	"log"

	"twitch-chat-scrapper/internal/config"
	"twitch-chat-scrapper/internal/db"
)

func main() {
	// Load environment configuration
	env, err := config.LoadEnv()
	if err != nil {
		log.Printf("Warning: Failed to load environment: %v", err)
	}

	// Get database configuration
	dbConfig := config.GetDBConfig(env)

	// Create database configuration
	dbCfg := &db.DBConfig{
		Type:     dbConfig["type"],
		Host:     dbConfig["host"],
		Port:     dbConfig["port"],
		User:     dbConfig["user"],
		Password: dbConfig["password"],
		Database: dbConfig["database"],
		SSLMode:  dbConfig["sslmode"],
		Path:     dbConfig["path"],
	}

	// Initialize database
	database, err := db.NewDatabase(dbCfg)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer database.Close()

	fmt.Printf("Connected to %s database\n\n", dbCfg.Type)
	sqlDB := database.GetDB()

	// Query to get messages per second for the last minute
	query := `
	SELECT
		strftime('%Y-%m-%d %H:%M:%S', timestamp) as second_bucket,
		COUNT(*) as message_count
	FROM chat_messages
	WHERE timestamp >= datetime('now', '-1 minute')
	GROUP BY strftime('%Y-%m-%d %H:%M:%S', timestamp)
	ORDER BY second_bucket DESC
	LIMIT 10
	`

	rows, err := sqlDB.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	fmt.Println("Messages per second for the last minute:")
	fmt.Println("=====================================")

	totalMessages := 0
	secondCount := 0

	for rows.Next() {
		var secondBucket string
		var messageCount int
		err := rows.Scan(&secondBucket, &messageCount)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("[%s] %d messages\n", secondBucket, messageCount)
		totalMessages += messageCount
		secondCount++
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	// Calculate average messages per second
	if secondCount > 0 {
		avgPerSecond := float64(totalMessages) / float64(secondCount)
		fmt.Printf("\nSummary for last minute:\n")
		fmt.Printf("Total messages: %d\n", totalMessages)
		fmt.Printf("Seconds with activity: %d\n", secondCount)
		fmt.Printf("Average messages/second: %.2f\n", avgPerSecond)
	} else {
		fmt.Println("\nNo messages found in the last minute.")
	}
}
