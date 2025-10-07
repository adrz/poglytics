package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Message metrics
	MessagesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "twitch_scraper_messages_total",
		Help: "Total number of messages received across all connections",
	})

	MessagesPerSecond = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "twitch_scraper_messages_per_second",
		Help: "Current messages per second (global)",
	})

	MessagesPerConnection = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "twitch_scraper_messages_per_connection_total",
		Help: "Total number of messages per connection",
	}, []string{"connection_id"})

	MessagesPerConnectionRate = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "twitch_scraper_messages_per_connection_rate",
		Help: "Messages per second per connection",
	}, []string{"connection_id"})

	// Channel metrics
	JoinedChannelsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "twitch_scraper_joined_channels_total",
		Help: "Total number of joined channels across all connections",
	})

	JoinedChannelsPerConnection = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "twitch_scraper_joined_channels_per_connection",
		Help: "Number of joined channels per connection",
	}, []string{"connection_id"})

	ChannelJoinAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "twitch_scraper_channel_join_attempts_total",
		Help: "Total number of channel join attempts",
	}, []string{"connection_id", "status"})

	// Connection metrics
	ActiveConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "twitch_scraper_active_connections",
		Help: "Number of active IRC connections",
	})

	ConnectionsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "twitch_scraper_connections_total",
		Help: "Total number of IRC connections (active and inactive)",
	})

	ConnectionReconnects = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "twitch_scraper_connection_reconnects_total",
		Help: "Total number of connection reconnects",
	}, []string{"connection_id"})

	ConnectionStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "twitch_scraper_connection_status",
		Help: "Connection status (1 = connected, 0 = disconnected)",
	}, []string{"connection_id"})

	// Database metrics
	DBBatchInsertDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "twitch_scraper_db_batch_insert_duration_seconds",
		Help:    "Time taken to insert a batch of messages into the database",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms to ~16s
	})

	DBBatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "twitch_scraper_db_batch_size",
		Help:    "Size of message batches written to database",
		Buckets: []float64{10, 50, 100, 200, 500, 1000, 2000, 5000},
	})

	DBWriteErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "twitch_scraper_db_write_errors_total",
		Help: "Total number of database write errors",
	})

	DBMessagesWritten = promauto.NewCounter(prometheus.CounterOpts{
		Name: "twitch_scraper_db_messages_written_total",
		Help: "Total number of messages successfully written to database",
	})

	// Message buffer metrics
	MessageBufferSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "twitch_scraper_message_buffer_size",
		Help: "Current size of central message buffer",
	})

	MessageBufferCapacity = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "twitch_scraper_message_buffer_capacity",
		Help: "Capacity of central message buffer",
	})

	MessageBufferUtilization = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "twitch_scraper_message_buffer_utilization_percent",
		Help: "Message buffer utilization percentage",
	})

	MessagesDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "twitch_scraper_messages_dropped_total",
		Help: "Total number of messages dropped due to buffer overflow",
	})

	// Channel discovery metrics
	ChannelDiscoveryDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "twitch_scraper_channel_discovery_duration_seconds",
		Help:    "Time taken to discover channels from Twitch API",
		Buckets: prometheus.ExponentialBuckets(1, 2, 10), // 1s to ~17min
	})

	ChannelsDiscovered = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "twitch_scraper_channels_discovered",
		Help: "Number of channels discovered in last discovery cycle",
	})

	// IRC protocol metrics
	PingPongLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "twitch_scraper_ping_pong_latency_seconds",
		Help:    "Latency of PING/PONG responses from Twitch IRC",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 10), // 10ms to ~10s
	}, []string{"connection_id"})

	IRCMessagesReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "twitch_scraper_irc_messages_received_total",
		Help: "Total number of IRC messages received by type",
	}, []string{"connection_id", "message_type"})

	// Parsed message type metrics
	ParsedMessagesByType = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "twitch_scraper_parsed_messages_by_type_total",
		Help: "Total number of parsed messages by message type (PRIVMSG, CLEARCHAT, etc.)",
	}, []string{"connection_id", "message_type"})

	// Error metrics
	ParseErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "twitch_scraper_parse_errors_total",
		Help: "Total number of message parse errors",
	}, []string{"connection_id", "error_type"})

	ReadTimeouts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "twitch_scraper_read_timeouts_total",
		Help: "Total number of read timeouts",
	}, []string{"connection_id"})

	// Twitch API rate limit metrics
	TwitchAPIRateLimitRemaining = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "twitch_api_ratelimit_remaining",
		Help: "Number of points remaining in the Twitch API rate limit bucket",
	})

	TwitchAPIRateLimitLimit = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "twitch_api_ratelimit_limit",
		Help: "The rate at which points are added to the Twitch API rate limit bucket",
	})

	TwitchAPIRateLimitReset = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "twitch_api_ratelimit_reset",
		Help: "Unix timestamp when the Twitch API rate limit bucket will be reset to full",
	})
)

// RecordDBBatchInsert records metrics for a database batch insert operation
func RecordDBBatchInsert(duration float64, batchSize int, success bool) {
	DBBatchInsertDuration.Observe(duration)
	DBBatchSize.Observe(float64(batchSize))

	if success {
		DBMessagesWritten.Add(float64(batchSize))
	} else {
		DBWriteErrors.Inc()
	}
}

// UpdateMessageBufferMetrics updates the message buffer metrics
func UpdateMessageBufferMetrics(size, capacity int) {
	MessageBufferSize.Set(float64(size))
	MessageBufferCapacity.Set(float64(capacity))

	if capacity > 0 {
		utilization := (float64(size) / float64(capacity)) * 100
		MessageBufferUtilization.Set(utilization)
	}
}

// RecordChannelJoin records a channel join attempt
func RecordChannelJoin(connectionID string, success bool) {
	status := "success"
	if !success {
		status = "failure"
	}
	ChannelJoinAttempts.WithLabelValues(connectionID, status).Inc()
}

// RecordMessage records a received message
func RecordMessage(connectionID string) {
	MessagesTotal.Inc()
	MessagesPerConnection.WithLabelValues(connectionID).Inc()
}

// RecordIRCMessage records an IRC protocol message
func RecordIRCMessage(connectionID, messageType string) {
	IRCMessagesReceived.WithLabelValues(connectionID, messageType).Inc()
}

// RecordParseError records a message parsing error
func RecordParseError(connectionID, errorType string) {
	ParseErrors.WithLabelValues(connectionID, errorType).Inc()
}

// RecordReadTimeout records a read timeout
func RecordReadTimeout(connectionID string) {
	ReadTimeouts.WithLabelValues(connectionID).Inc()
}

// RecordReconnect records a connection reconnect
func RecordReconnect(connectionID string) {
	ConnectionReconnects.WithLabelValues(connectionID).Inc()
}

// UpdateConnectionStatus updates the connection status metric
func UpdateConnectionStatus(connectionID string, connected bool) {
	status := 0.0
	if connected {
		status = 1.0
	}
	ConnectionStatus.WithLabelValues(connectionID).Set(status)
}

// RecordPingPong records PING/PONG latency
func RecordPingPong(connectionID string, latency float64) {
	PingPongLatency.WithLabelValues(connectionID).Observe(latency)
}

// RecordParsedMessage records a successfully parsed message by its type
func RecordParsedMessage(connectionID, messageType string) {
	ParsedMessagesByType.WithLabelValues(connectionID, messageType).Inc()
}

// UpdateTwitchAPIRateLimit updates the Twitch API rate limit metrics
func UpdateTwitchAPIRateLimit(remaining, limit int, reset int64) {
	TwitchAPIRateLimitRemaining.Set(float64(remaining))
	TwitchAPIRateLimitLimit.Set(float64(limit))
	TwitchAPIRateLimitReset.Set(float64(reset))
}
