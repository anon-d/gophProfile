package config

import "testing"

func TestLoad_Defaults(t *testing.T) {
	// t.Setenv автоматически восстанавливает значение после теста.
	// Устанавливаем пустые значения, чтобы проверить дефолты.
	for _, key := range []string{"SERVER_ADDRESS", "DATABASE_URL", "MINIO_ENDPOINT", "KAFKA_BROKERS", "LOG_LEVEL"} {
		t.Setenv(key, "")
	}

	cfg := Load()

	if cfg.Server.Address != ":8080" {
		t.Errorf("expected :8080, got %s", cfg.Server.Address)
	}
	if cfg.Minio.Endpoint != "localhost:9000" {
		t.Errorf("expected localhost:9000, got %s", cfg.Minio.Endpoint)
	}
	if cfg.Minio.Bucket != "avatars" {
		t.Errorf("expected avatars, got %s", cfg.Minio.Bucket)
	}
	if cfg.Kafka.TopicUpload != "avatar-upload" {
		t.Errorf("expected avatar-upload, got %s", cfg.Kafka.TopicUpload)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("expected info, got %s", cfg.LogLevel)
	}
	if len(cfg.Kafka.Brokers) != 1 || cfg.Kafka.Brokers[0] != "localhost:9092" {
		t.Errorf("expected [localhost:9092], got %v", cfg.Kafka.Brokers)
	}
}

func TestLoad_FromEnv(t *testing.T) {
	t.Setenv("SERVER_ADDRESS", ":9090")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("MINIO_BUCKET", "test-avatars")

	cfg := Load()

	if cfg.Server.Address != ":9090" {
		t.Errorf("expected :9090, got %s", cfg.Server.Address)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("expected debug, got %s", cfg.LogLevel)
	}
	if cfg.Minio.Bucket != "test-avatars" {
		t.Errorf("expected test-avatars, got %s", cfg.Minio.Bucket)
	}
}

func TestGetEnv(t *testing.T) {
	t.Setenv("TEST_KEY", "test_value")

	if v := getEnv("TEST_KEY", "default"); v != "test_value" {
		t.Errorf("expected test_value, got %s", v)
	}
	if v := getEnv("NON_EXISTENT_KEY_12345", "fallback"); v != "fallback" {
		t.Errorf("expected fallback, got %s", v)
	}
}
