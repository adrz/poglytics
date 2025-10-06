package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"twitch-chat-scrapper/internal/app"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	// Configuration
	maxChannels := 50000          // Total channels to monitor
	channelsPerConnection := 2000 // Channels per IRC connection (safe limit for justinfan)
	metricsPort := 9091           // Port for Prometheus metrics endpoint

	// Allow override via environment variables
	if envMax := os.Getenv("MAX_CHANNELS"); envMax != "" {
		if val, err := strconv.Atoi(envMax); err == nil {
			maxChannels = val
		}
	}

	if envPerConn := os.Getenv("CHANNELS_PER_CONNECTION"); envPerConn != "" {
		if val, err := strconv.Atoi(envPerConn); err == nil {
			channelsPerConnection = val
		}
	}

	if envMetricsPort := os.Getenv("METRICS_PORT"); envMetricsPort != "" {
		if val, err := strconv.Atoi(envMetricsPort); err == nil {
			metricsPort = val
		}
	}

	fmt.Printf("Starting Twitch chat scraper with connection pooling\n")
	fmt.Printf("Configuration:\n")
	fmt.Printf("  - Total channels: %d\n", maxChannels)
	fmt.Printf("  - Channels per connection: %d\n", channelsPerConnection)
	fmt.Printf("  - Estimated connections: %d\n", (maxChannels+channelsPerConnection-1)/channelsPerConnection)
	fmt.Printf("  - Metrics endpoint: http://localhost:%d/metrics\n", metricsPort)
	fmt.Println()

	// Start Prometheus metrics HTTP server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		fmt.Printf("Starting Prometheus metrics server on :%d\n", metricsPort)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", metricsPort), nil); err != nil {
			fmt.Printf("Error starting metrics server: %v\n", err)
		}
	}()

	// Create connection pool
	pool, err := app.NewConnectionPool(maxChannels, channelsPerConnection)
	if err != nil {
		fmt.Printf("Failed to create connection pool: %v\n", err)
		os.Exit(1)
	}

	// Handle graceful shutdown on interrupt
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-c
		fmt.Println("\nReceived interrupt signal, shutting down gracefully...")
		pool.Shutdown()
		os.Exit(0)
	}()

	// Start the connection pool
	if err := pool.Start(); err != nil {
		fmt.Printf("Failed to start connection pool: %v\n", err)
		pool.Shutdown()
		os.Exit(1)
	}

	// Keep main thread alive
	select {}
}
