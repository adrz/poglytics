package main

import (
	"database/sql"
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

	// 1. Messages per second for last minute
	fmt.Println("🕐 MESSAGES PER SECOND - LAST MINUTE")
	fmt.Println("====================================")
	queryMessagesPerSecond(sqlDB, "1 minute")

	// 2. Messages per minute for last hour
	fmt.Println("\n🕐 MESSAGES PER MINUTE - LAST HOUR")
	fmt.Println("==================================")
	queryMessagesPerMinute(sqlDB, "1 hour")

	// 3. Top channels by activity
	fmt.Println("\n📊 TOP CHANNELS - LAST HOUR")
	fmt.Println("===========================")
	queryTopChannels(sqlDB, "1 hour")

	// 4. Overall statistics
	fmt.Println("\n📈 OVERALL STATISTICS")
	fmt.Println("====================")
	queryOverallStats(sqlDB)
}

func queryMessagesPerSecond(db *sql.DB, timeWindow string) {
	query := fmt.Sprintf(`
	SELECT
		strftime('%%Y-%%m-%%d %%H:%%M:%%S', timestamp) as second_bucket,
		COUNT(*) as message_count
	FROM chat_messages
	WHERE timestamp >= datetime('now', '-%s')
	GROUP BY strftime('%%Y-%%m-%%d %%H:%%M:%%S', timestamp)
	ORDER BY second_bucket DESC
	LIMIT 20
	`, timeWindow)

	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

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

	if secondCount > 0 {
		avgPerSecond := float64(totalMessages) / float64(secondCount)
		fmt.Printf("\n📊 Summary:\n")
		fmt.Printf("   Total messages: %d\n", totalMessages)
		fmt.Printf("   Seconds with activity: %d\n", secondCount)
		fmt.Printf("   Average messages/second: %.2f\n", avgPerSecond)
	} else {
		fmt.Printf("No messages found in the last %s.\n", timeWindow)
	}
}

func queryMessagesPerMinute(db *sql.DB, timeWindow string) {
	query := fmt.Sprintf(`
	SELECT
		strftime('%%Y-%%m-%%d %%H:%%M', timestamp) as minute_bucket,
		COUNT(*) as message_count
	FROM chat_messages
	WHERE timestamp >= datetime('now', '-%s')
	GROUP BY strftime('%%Y-%%m-%%d %%H:%%M', timestamp)
	ORDER BY minute_bucket DESC
	LIMIT 20
	`, timeWindow)

	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	totalMessages := 0
	minuteCount := 0

	for rows.Next() {
		var minuteBucket string
		var messageCount int
		err := rows.Scan(&minuteBucket, &messageCount)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("[%s] %d messages\n", minuteBucket, messageCount)
		totalMessages += messageCount
		minuteCount++
	}

	if minuteCount > 0 {
		avgPerMinute := float64(totalMessages) / float64(minuteCount)
		fmt.Printf("\n📊 Summary:\n")
		fmt.Printf("   Total messages: %d\n", totalMessages)
		fmt.Printf("   Minutes with activity: %d\n", minuteCount)
		fmt.Printf("   Average messages/minute: %.2f\n", avgPerMinute)
	}
}

func queryTopChannels(db *sql.DB, timeWindow string) {
	query := fmt.Sprintf(`
	SELECT
		channel,
		COUNT(*) as message_count,
		COUNT(DISTINCT nickname) as unique_users
	FROM chat_messages
	WHERE timestamp >= datetime('now', '-%s')
	GROUP BY channel
	ORDER BY message_count DESC
	LIMIT 10
	`, timeWindow)

	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var channel string
		var messageCount, uniqueUsers int
		err := rows.Scan(&channel, &messageCount, &uniqueUsers)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%-20s %6d messages, %4d users\n", channel, messageCount, uniqueUsers)
	}
}

func queryOverallStats(db *sql.DB) {
	// Total messages
	var totalMessages int
	err := db.QueryRow("SELECT COUNT(*) FROM chat_messages").Scan(&totalMessages)
	if err != nil {
		log.Fatal(err)
	}

	// Unique users
	var uniqueUsers int
	err = db.QueryRow("SELECT COUNT(DISTINCT nickname) FROM chat_messages").Scan(&uniqueUsers)
	if err != nil {
		log.Fatal(err)
	}

	// Unique channels
	var uniqueChannels int
	err = db.QueryRow("SELECT COUNT(DISTINCT channel) FROM chat_messages").Scan(&uniqueChannels)
	if err != nil {
		log.Fatal(err)
	}

	// Time range
	var firstMessage, lastMessage string
	err = db.QueryRow("SELECT MIN(timestamp), MAX(timestamp) FROM chat_messages").Scan(&firstMessage, &lastMessage)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("📊 Total messages: %d\n", totalMessages)
	fmt.Printf("👥 Unique users: %d\n", uniqueUsers)
	fmt.Printf("📺 Unique channels: %d\n", uniqueChannels)
	fmt.Printf("🕐 Time range: %s to %s\n", firstMessage, lastMessage)
}
