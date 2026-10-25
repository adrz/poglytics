package main

import (
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"poglytics-scraper/internal/app"
	"poglytics-scraper/internal/config"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// Configuration
	maxChannels := 0              // Channel cap; 0 monitors every discovered channel
	channelsPerConnection := 1500 // Channels per IRC connection (safe limit for justinfan)
	joinsPerSecond := 50          // Pool-wide JOIN rate; Twitch drops JOINs above ~100/s per IP
	metricsPort := 9091           // Port for Prometheus metrics endpoint

	env, err := config.LoadEnv()
	if err != nil {
		logger.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}
	for key, target := range map[string]*int{
		"MAX_CHANNELS":            &maxChannels,
		"CHANNELS_PER_CONNECTION": &channelsPerConnection,
		"JOINS_PER_SECOND":        &joinsPerSecond,
		"METRICS_PORT":            &metricsPort,
	} {
		if value := env[key]; value != "" {
			parsed, err := strconv.Atoi(value)
			// MAX_CHANNELS accepts 0, which means "no limit".
			minimum := 1
			if key == "MAX_CHANNELS" {
				minimum = 0
			}
			if err != nil || parsed < minimum || (key == "METRICS_PORT" && parsed > 65535) {
				logger.Error("Invalid integer configuration", "key", key, "minimum", minimum)
				os.Exit(1)
			}
			*target = parsed
		}
	}

	// Connection count is decided after discovery, so it is not known yet.
	channelLimit := "unlimited"
	if maxChannels > 0 {
		channelLimit = strconv.Itoa(maxChannels)
	}
	logger.Info("Starting Twitch chat scraper with connection pooling",
		"channel_limit", channelLimit,
		"channels_per_connection", channelsPerConnection,
		"joins_per_second", joinsPerSecond,
		"metrics_port", metricsPort)

	// Start Prometheus metrics HTTP server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		logger.Info("Starting Prometheus metrics server", "port", metricsPort)
		if err := http.ListenAndServe(":"+strconv.Itoa(metricsPort), nil); err != nil {
			logger.Error("Error starting metrics server", "error", err)
		}
	}()

	// Create connection pool
	pool, err := app.NewConnectionPool(maxChannels, channelsPerConnection, joinsPerSecond)
	if err != nil {
		logger.Error("Failed to create connection pool", "error", err)
		os.Exit(1)
	}

	// Handle graceful shutdown on interrupt
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-c
		logger.Info("Received interrupt signal, shutting down gracefully...")
		pool.Shutdown()
		os.Exit(0)
	}()

	// Start the connection pool
	if err := pool.Start(); err != nil {
		logger.Error("Failed to start connection pool", "error", err)
		pool.Shutdown()
		os.Exit(1)
	}

	// Keep main thread alive
	select {}
}
