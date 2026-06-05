// Package config предоставляет конфигурацию приложения из переменных окружения.
package config

import (
	"os"
	"strings"
)

// Config — корневая структура конфигурации.
type Config struct {
	Server   ServerConfig
	Postgres PostgresConfig
	Minio    MinioConfig
	Kafka    KafkaConfig
	LogLevel string
}

// ServerConfig — настройки HTTP-сервера.
type ServerConfig struct {
	Address string
}

// PostgresConfig — настройки подключения к PostgreSQL.
type PostgresConfig struct {
	DSN string
}

// MinioConfig — настройки подключения к MinIO.
type MinioConfig struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	UseSSL    bool
	Bucket    string
}

// KafkaConfig — настройки подключения к Kafka.
type KafkaConfig struct {
	Brokers     []string
	TopicUpload string
	TopicDelete string
	GroupID     string
}

// Load читает конфигурацию из переменных окружения.
func Load() *Config {
	return &Config{
		Server: ServerConfig{
			Address: getEnv("SERVER_ADDRESS", ":8080"),
		},
		Postgres: PostgresConfig{
			DSN: getEnv("DATABASE_URL", "postgres://gophprofile:gophprofile@localhost:5432/gophprofile?sslmode=disable"),
		},
		Minio: MinioConfig{
			Endpoint:  getEnv("MINIO_ENDPOINT", "localhost:9000"),
			AccessKey: getEnv("MINIO_ACCESS_KEY", "minioadmin"),
			SecretKey: getEnv("MINIO_SECRET_KEY", "minioadmin"),
			UseSSL:    getEnv("MINIO_USE_SSL", "false") == "true",
			Bucket:    getEnv("MINIO_BUCKET", "avatars"),
		},
		Kafka: KafkaConfig{
			Brokers:     strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ","),
			TopicUpload: getEnv("KAFKA_TOPIC_UPLOAD", "avatar-upload"),
			TopicDelete: getEnv("KAFKA_TOPIC_DELETE", "avatar-delete"),
			GroupID:     getEnv("KAFKA_GROUP_ID", "avatar-workers"),
		},
		LogLevel: getEnv("LOG_LEVEL", "info"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
