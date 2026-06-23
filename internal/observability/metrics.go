package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	httpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "avatars_http_requests_total",
			Help: "Total HTTP requests count.",
		},
		[]string{"method", "route", "status"},
	)

	httpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "avatars_http_request_duration_seconds",
			Help:    "HTTP request duration in seconds.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.3, 0.5, 1, 2, 5, 10},
		},
		[]string{"method", "route", "status"},
	)

	httpErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "avatars_http_errors_total",
			Help: "Total number of HTTP error responses.",
		},
		[]string{"method", "route", "status"},
	)

	uploadsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "avatars_uploads_total",
			Help: "Total number of avatar upload operations.",
		},
		[]string{"status", "user_id"},
	)

	uploadDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "avatars_upload_duration_seconds",
			Help:    "Avatar upload duration in seconds.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10},
		},
		[]string{"status"},
	)

	storageUsage = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "avatars_storage_bytes",
			Help: "Storage usage by user in bytes.",
		},
		[]string{"user_id"},
	)

	operationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "avatars_operations_duration_seconds",
			Help:    "Duration of internal operations.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.3, 0.5, 1, 2, 5},
		},
		[]string{"component", "operation", "status"},
	)

	operationErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "avatars_operations_errors_total",
			Help: "Total number of internal operation errors.",
		},
		[]string{"component", "operation"},
	)

	dbConnections = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "avatars_db_connections",
			Help: "PostgreSQL connection pool stats.",
		},
		[]string{"service", "state"},
	)

	queueDepth = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "avatars_queue_depth",
			Help: "In-flight Kafka messages being processed by workers.",
		},
		[]string{"service", "topic"},
	)

	kafkaMessagesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "avatars_kafka_messages_total",
			Help: "Kafka messages processed/sent.",
		},
		[]string{"service", "topic", "role", "status"},
	)
)

// MetricsHandler возвращает HTTP handler для /metrics.
func MetricsHandler() http.Handler {
	return promhttp.Handler()
}

// ObserveHTTPRequest фиксирует HTTP RED-метрики.
func ObserveHTTPRequest(method, route string, statusCode int, duration time.Duration) {
	if route == "" {
		route = "unknown"
	}
	status := strconv.Itoa(statusCode)

	httpRequestsTotal.WithLabelValues(method, route, status).Inc()
	httpRequestDuration.WithLabelValues(method, route, status).Observe(duration.Seconds())
	if statusCode >= http.StatusBadRequest {
		httpErrorsTotal.WithLabelValues(method, route, status).Inc()
	}
}

// ObserveUpload фиксирует бизнес-метрики загрузки аватарки.
func ObserveUpload(userID, status string, duration time.Duration) {
	if userID == "" {
		userID = "unknown"
	}
	if status == "" {
		status = "unknown"
	}

	uploadsTotal.WithLabelValues(status, userID).Inc()
	uploadDuration.WithLabelValues(status).Observe(duration.Seconds())
}

// AddStorageUsage изменяет метрику занятого хранилища пользователя.
func AddStorageUsage(userID string, deltaBytes int64) {
	if userID == "" {
		userID = "unknown"
	}
	storageUsage.WithLabelValues(userID).Add(float64(deltaBytes))
}

// SetStorageUsage задаёт абсолютное значение занятого хранилища пользователя.
func SetStorageUsage(userID string, bytes int64) {
	if userID == "" {
		userID = "unknown"
	}
	storageUsage.WithLabelValues(userID).Set(float64(bytes))
}

// ObserveOperation фиксирует время выполнения внутренних операций.
func ObserveOperation(component, operation, status string, duration time.Duration) {
	if component == "" {
		component = "unknown"
	}
	if operation == "" {
		operation = "unknown"
	}
	if status == "" {
		status = "unknown"
	}

	operationDuration.WithLabelValues(component, operation, status).Observe(duration.Seconds())
	if status == "error" {
		operationErrors.WithLabelValues(component, operation).Inc()
	}
}

// SetDBConnections записывает статистику пула подключений к БД.
func SetDBConnections(service, state string, value float64) {
	if service == "" {
		service = "unknown"
	}
	if state == "" {
		state = "unknown"
	}
	dbConnections.WithLabelValues(service, state).Set(value)
}

// AddQueueDepth изменяет метрику глубины очереди обработки.
func AddQueueDepth(service, topic string, delta float64) {
	if service == "" {
		service = "unknown"
	}
	if topic == "" {
		topic = "unknown"
	}
	queueDepth.WithLabelValues(service, topic).Add(delta)
}

// ObserveKafkaMessage фиксирует метрики работы с Kafka.
func ObserveKafkaMessage(service, topic, role, status string) {
	if service == "" {
		service = "unknown"
	}
	if topic == "" {
		topic = "unknown"
	}
	if role == "" {
		role = "unknown"
	}
	if status == "" {
		status = "unknown"
	}
	kafkaMessagesTotal.WithLabelValues(service, topic, role, status).Inc()
}
