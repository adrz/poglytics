package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"twitch-chat-scrapper/internal/app"
)

func main() {
	subscriber := app.NewSubscriber()

	// Configuration for dynamic channel discovery
	maxChannels := 5000000 // Limit to prevent buffer overflow issues
	fmt.Printf("Starting Twitch chat scraper with dynamic channel discovery (max %d channels)\n", maxChannels)
	fmt.Println("Channels will be automatically discovered from live streams and updated periodically")

	// Handle graceful shutdown on interrupt
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-c
		fmt.Println("\nReceived interrupt signal, shutting down gracefully...")
		subscriber.Shutdown()
		os.Exit(0)
	}()

	subscriber.Run(maxChannels)
}
