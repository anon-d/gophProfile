// Package worker реализует фоновую обработку событий Kafka
// (создание миниатюр, удаление файлов из S3).
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg"
	_ "image/png" // регистрация декодера PNG
	"log/slog"
	"time"

	"github.com/IBM/sarama"
	"github.com/anon-d/gophProfile/internal/observability"
	"github.com/nfnt/resize"
	_ "golang.org/x/image/webp" // регистрация декодера WebP

	"github.com/anon-d/gophProfile/internal/domain"
	miniorepo "github.com/anon-d/gophProfile/internal/repository/minio"
	pgrepo "github.com/anon-d/gophProfile/internal/repository/postgres"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// thumbnailSizes — размеры генерируемых миниатюр.
var thumbnailSizes = []struct {
	Name   string
	Width  uint
	Height uint
}{
	{"100x100", 100, 100},
	{"300x300", 300, 300},
}

// Worker — обработчик событий.
type Worker struct {
	db          *pgrepo.Repo
	s3          *miniorepo.Repo
	logger      *slog.Logger
	topicUpload string
	topicDelete string
}

// New создаёт нового Worker.
func New(db *pgrepo.Repo, s3 *miniorepo.Repo, logger *slog.Logger, topicUpload, topicDelete string) *Worker {
	return &Worker{
		db:          db,
		s3:          s3,
		logger:      logger,
		topicUpload: topicUpload,
		topicDelete: topicDelete,
	}
}

// HandleMessage — точка входа для обработки сообщений Kafka.
func (w *Worker) HandleMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	ctx, span := otel.Tracer("gophprofile.worker").Start(ctx, "worker.handle_message")
	span.SetAttributes(
		attribute.String("topic", msg.Topic),
		attribute.Int("partition", int(msg.Partition)),
		attribute.Int64("offset", msg.Offset),
	)
	start := time.Now()
	status := "success"
	defer func() {
		span.SetAttributes(attribute.String("status", status))
		span.End()
		observability.ObserveOperation("worker", "handle_message", status, time.Since(start))
	}()

	switch msg.Topic {
	case w.topicUpload:
		return w.handleUpload(ctx, msg.Value)
	case w.topicDelete:
		return w.handleDelete(ctx, msg.Value)
	default:
		status = "error"
		span.SetStatus(codes.Error, "unknown topic")
		observability.LoggerWithTrace(ctx, w.logger).Warn("unknown topic", "topic", msg.Topic)
		return nil
	}
}

// handleUpload обрабатывает загрузку: скачивает оригинал, создаёт миниатюры, обновляет БД.
func (w *Worker) handleUpload(ctx context.Context, data []byte) error {
	ctx, span := otel.Tracer("gophprofile.worker").Start(ctx, "worker.handle_upload")
	start := time.Now()
	status := "success"
	defer func() {
		span.SetAttributes(attribute.String("status", status))
		span.End()
		observability.ObserveOperation("worker", "handle_upload", status, time.Since(start))
	}()

	var event domain.AvatarUploadEvent
	if err := json.Unmarshal(data, &event); err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("unmarshal upload event: %w", err)
	}

	span.SetAttributes(
		attribute.String("avatar_id", event.AvatarID),
		attribute.String("user_id", event.UserID),
		attribute.String("s3_key", event.S3Key),
	)
	observability.LoggerWithTrace(ctx, w.logger).Info("processing upload", "avatar_id", event.AvatarID)

	// Проверяем идемпотентность: если уже обработано, пропускаем.
	avatar, err := w.db.GetAvatarByID(ctx, event.AvatarID)
	if err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("get avatar: %w", err)
	}
	if avatar.ProcessingStatus == domain.ProcessingStatusCompleted {
		observability.LoggerWithTrace(ctx, w.logger).Info("already processed, skipping", "avatar_id", event.AvatarID)
		return nil
	}

	// Обновляем статус на processing.
	if err := w.db.UpdateProcessingStatus(ctx, event.AvatarID, domain.ProcessingStatusProcessing, nil); err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("update status to processing: %w", err)
	}

	// Скачиваем оригинал из S3.
	imgData, err := w.s3.GetBytes(ctx, event.S3Key)
	if err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		_ = w.db.UpdateProcessingStatus(ctx, event.AvatarID, domain.ProcessingStatusFailed, nil)
		return fmt.Errorf("download original: %w", err)
	}

	// Декодируем изображение.
	img, _, err := image.Decode(bytes.NewReader(imgData))
	if err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		_ = w.db.UpdateProcessingStatus(ctx, event.AvatarID, domain.ProcessingStatusFailed, nil)
		return fmt.Errorf("decode image: %w", err)
	}

	// Создаём миниатюры.
	thumbnails := make(map[string]string)
	for _, ts := range thumbnailSizes {
		thumb := resize.Thumbnail(ts.Width, ts.Height, img, resize.Lanczos3)

		var buf bytes.Buffer
		if err := jpeg.Encode(&buf, thumb, &jpeg.Options{Quality: 85}); err != nil {
			observability.LoggerWithTrace(ctx, w.logger).Error("encode thumbnail failed", "size", ts.Name, "error", err)
			continue
		}

		key := miniorepo.ThumbnailKey(event.AvatarID, ts.Name)
		if err := w.s3.PutBytes(ctx, key, buf.Bytes(), "image/jpeg"); err != nil {
			observability.LoggerWithTrace(ctx, w.logger).Error("upload thumbnail failed", "size", ts.Name, "error", err)
			continue
		}

		thumbnails[ts.Name] = key
	}

	// Обновляем статус и ключи миниатюр в БД.
	if err := w.db.UpdateProcessingStatus(ctx, event.AvatarID, domain.ProcessingStatusCompleted, thumbnails); err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("update status to completed: %w", err)
	}

	observability.LoggerWithTrace(ctx, w.logger).Info("processing completed", "avatar_id", event.AvatarID, "thumbnails", len(thumbnails))
	return nil
}

// handleDelete удаляет файлы из S3.
func (w *Worker) handleDelete(ctx context.Context, data []byte) error {
	ctx, span := otel.Tracer("gophprofile.worker").Start(ctx, "worker.handle_delete")
	start := time.Now()
	status := "success"
	defer func() {
		span.SetAttributes(attribute.String("status", status))
		span.End()
		observability.ObserveOperation("worker", "handle_delete", status, time.Since(start))
	}()

	var event domain.AvatarDeleteEvent
	if err := json.Unmarshal(data, &event); err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("unmarshal delete event: %w", err)
	}

	span.SetAttributes(
		attribute.String("avatar_id", event.AvatarID),
		attribute.Int("keys_count", len(event.S3Keys)),
	)
	observability.LoggerWithTrace(ctx, w.logger).Info("processing delete", "avatar_id", event.AvatarID, "keys_count", len(event.S3Keys))

	for _, key := range event.S3Keys {
		if err := w.s3.Delete(ctx, key); err != nil {
			observability.LoggerWithTrace(ctx, w.logger).Error("delete s3 object failed", "key", key, "error", err)
			// Продолжаем удаление остальных ключей.
		}
	}

	observability.LoggerWithTrace(ctx, w.logger).Info("delete completed", "avatar_id", event.AvatarID)
	return nil
}
