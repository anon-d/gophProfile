package handler

import (
	"context"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
)

// HealthHandler — обработчик healthcheck.
type HealthHandler struct {
	pool        *pgxpool.Pool
	minioClient *minio.Client
}

// NewHealthHandler создаёт healthcheck-обработчик.
func NewHealthHandler(pool *pgxpool.Pool, minioClient *minio.Client) *HealthHandler {
	return &HealthHandler{pool: pool, minioClient: minioClient}
}

// Health обрабатывает GET /health.
func (h *HealthHandler) Health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	status := map[string]string{
		"status": "ok",
	}
	code := http.StatusOK

	// Проверка PostgreSQL.
	if err := h.pool.Ping(ctx); err != nil {
		status["postgres"] = "down: " + err.Error()
		status["status"] = "degraded"
		code = http.StatusServiceUnavailable
	} else {
		status["postgres"] = "ok"
	}

	// Проверка MinIO.
	if _, err := h.minioClient.ListBuckets(ctx); err != nil {
		status["minio"] = "down: " + err.Error()
		status["status"] = "degraded"
		code = http.StatusServiceUnavailable
	} else {
		status["minio"] = "ok"
	}

	writeJSON(w, code, status)
}
