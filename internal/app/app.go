// Package app собирает все компоненты HTTP-сервера.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	minioclient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/anon-d/gophProfile/internal/config"
	"github.com/anon-d/gophProfile/internal/handler"
	"github.com/anon-d/gophProfile/internal/kafka"
	"github.com/anon-d/gophProfile/internal/logger"
	miniorepo "github.com/anon-d/gophProfile/internal/repository/minio"
	pgrepo "github.com/anon-d/gophProfile/internal/repository/postgres"
	"github.com/anon-d/gophProfile/internal/service"
)

// App — HTTP-сервер с зависимостями.
type App struct {
	server   *http.Server
	pool     *pgxpool.Pool
	producer *kafka.Producer
	log      *slog.Logger
}

// NewApp инициализирует все компоненты и возвращает App.
func NewApp() (*App, error) {
	cfg := config.Load()
	log := logger.Setup(cfg.LogLevel)

	ctx := context.Background()

	// PostgreSQL.
	pool, err := pgxpool.New(ctx, cfg.Postgres.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("postgres ping: %w", err)
	}
	log.Info("connected to postgres")

	pgRepo := pgrepo.New(pool)

	// MinIO.
	mc, err := minioclient.New(cfg.Minio.Endpoint, &minioclient.Options{
		Creds:  credentials.NewStaticV4(cfg.Minio.AccessKey, cfg.Minio.SecretKey, ""),
		Secure: cfg.Minio.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("minio client: %w", err)
	}
	s3Repo, err := miniorepo.New(ctx, mc, cfg.Minio.Bucket)
	if err != nil {
		return nil, fmt.Errorf("minio repo: %w", err)
	}
	log.Info("connected to minio")

	// Kafka producer.
	producer, err := kafka.NewProducer(cfg.Kafka.Brokers)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	log.Info("kafka producer created")

	// Сервис.
	svc := service.NewAvatarService(pgRepo, s3Repo, producer, cfg.Kafka.TopicUpload, cfg.Kafka.TopicDelete, log)

	// Роутер.
	router := handler.NewRouter(svc, pool, mc, log)

	srv := &http.Server{
		Addr:    cfg.Server.Address,
		Handler: router,
	}

	return &App{
		server:   srv,
		pool:     pool,
		producer: producer,
		log:      log,
	}, nil
}

// Run запускает HTTP-сервер.
func (a *App) Run() error {
	a.log.Info("starting HTTP server", "addr", a.server.Addr)
	return a.server.ListenAndServe()
}

// Shutdown корректно останавливает сервер и закрывает ресурсы.
func (a *App) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var shutdownErr error

	if err := a.server.Shutdown(ctx); err != nil {
		a.log.Error("server shutdown", "error", err)
		shutdownErr = err
	}

	a.pool.Close()

	if err := a.producer.Close(); err != nil {
		a.log.Error("kafka producer close", "error", err)
		if shutdownErr == nil {
			shutdownErr = err
		}
	}

	a.log.Info("all resources closed")
	return shutdownErr
}
