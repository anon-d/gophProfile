package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	minioclient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/anon-d/gophProfile/internal/config"
	"github.com/anon-d/gophProfile/internal/kafka"
	"github.com/anon-d/gophProfile/internal/logger"
	miniorepo "github.com/anon-d/gophProfile/internal/repository/minio"
	pgrepo "github.com/anon-d/gophProfile/internal/repository/postgres"
	"github.com/anon-d/gophProfile/internal/worker"
)

func main() {
	cfg := config.Load()
	slogger := logger.Setup(cfg.LogLevel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// PostgreSQL.
	pool, err := pgxpool.New(ctx, cfg.Postgres.DSN)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer pool.Close()

	pgRepo := pgrepo.New(pool)

	// MinIO.
	mc, err := minioclient.New(cfg.Minio.Endpoint, &minioclient.Options{
		Creds:  credentials.NewStaticV4(cfg.Minio.AccessKey, cfg.Minio.SecretKey, ""),
		Secure: cfg.Minio.UseSSL,
	})
	if err != nil {
		log.Fatalf("minio client: %v", err)
	}
	s3Repo, err := miniorepo.New(ctx, mc, cfg.Minio.Bucket)
	if err != nil {
		log.Fatalf("minio repo: %v", err)
	}

	// Worker.
	w := worker.New(pgRepo, s3Repo, slogger)

	// Kafka consumer.
	topics := []string{cfg.Kafka.TopicUpload, cfg.Kafka.TopicDelete}
	consumer, err := kafka.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.GroupID, topics, w.HandleMessage, slogger)
	if err != nil {
		log.Fatalf("kafka consumer: %v", err)
	}
	defer consumer.Close()

	// Graceful shutdown.
	go func() {
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
		<-stop
		slogger.Info("shutting down worker...")
		cancel()
	}()

	slogger.Info("worker started", "topics", topics)
	if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("consumer run: %v", err)
	}
	slogger.Info("worker stopped")
}
