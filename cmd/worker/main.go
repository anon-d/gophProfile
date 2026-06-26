package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	minioclient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/anon-d/gophProfile/internal/config"
	"github.com/anon-d/gophProfile/internal/kafka"
	"github.com/anon-d/gophProfile/internal/logger"
	"github.com/anon-d/gophProfile/internal/observability"
	miniorepo "github.com/anon-d/gophProfile/internal/repository/minio"
	pgrepo "github.com/anon-d/gophProfile/internal/repository/postgres"
	"github.com/anon-d/gophProfile/internal/worker"
)

func main() {
	cfg := config.Load()
	slogger := logger.Setup(cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	obsShutdown, err := observability.Init(ctx, observability.Config{
		ServiceName:  "gophprofile-worker",
		Environment:  cfg.Obs.Environment,
		OTLPEndpoint: cfg.Obs.OTLPEndpoint,
	})
	if err != nil {
		slogger.Error("init observability failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := obsShutdown(shutdownCtx); err != nil {
			slogger.Error("otel shutdown failed", "error", err)
		}
	}()

	// PostgreSQL.
	pool, err := pgxpool.New(ctx, cfg.Postgres.DSN)
	if err != nil {
		slogger.Error("postgres init failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	stopDBMetrics := observability.StartDBStatsCollector(ctx, pool, "worker", 10*time.Second)
	defer stopDBMetrics()

	pgRepo := pgrepo.New(pool)

	// MinIO.
	mc, err := minioclient.New(cfg.Minio.Endpoint, &minioclient.Options{
		Creds:  credentials.NewStaticV4(cfg.Minio.AccessKey, cfg.Minio.SecretKey, ""),
		Secure: cfg.Minio.UseSSL,
	})
	if err != nil {
		slogger.Error("minio client init failed", "error", err)
		os.Exit(1)
	}
	s3Repo, err := miniorepo.New(ctx, mc, cfg.Minio.Bucket)
	if err != nil {
		slogger.Error("minio repo init failed", "error", err)
		os.Exit(1)
	}

	// Worker.
	w := worker.New(pgRepo, s3Repo, slogger, cfg.Kafka.TopicUpload, cfg.Kafka.TopicDelete)

	// Kafka consumer.
	topics := []string{cfg.Kafka.TopicUpload, cfg.Kafka.TopicDelete}
	consumer, err := kafka.NewConsumer("worker", cfg.Kafka.Brokers, cfg.Kafka.GroupID, topics, w.HandleMessage, slogger)
	if err != nil {
		slogger.Error("kafka consumer init failed", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	// Endpoint метрик воркера.
	if cfg.Obs.WorkerMetricsAddress != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", observability.MetricsHandler())
		metricsSrv := &http.Server{
			Addr:    cfg.Obs.WorkerMetricsAddress,
			Handler: mux,
		}
		go func() {
			if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slogger.Error("worker metrics server failed", "error", err)
			}
		}()
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
				slogger.Error("worker metrics server shutdown failed", "error", err)
			}
		}()
		slogger.Info("worker metrics endpoint started", "addr", cfg.Obs.WorkerMetricsAddress)
	}

	slogger.Info("worker started", "topics", topics)
	if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
		slogger.Error("consumer run failed", "error", err)
		os.Exit(1)
	}
	slogger.Info("worker stopped")
}
