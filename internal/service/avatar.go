// Package service содержит бизнес-логику сервиса аватарок.
package service

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/anon-d/gophProfile/internal/domain"
	"github.com/anon-d/gophProfile/internal/observability"
	miniorepo "github.com/anon-d/gophProfile/internal/repository/minio"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// MaxUploadSize — максимальный размер загружаемого файла (10 МБ).
// Экспортируется только для использования в handler (MaxBytesReader).
const MaxUploadSize = 10 << 20

var allowedMimeTypes = map[string]bool{
	"image/jpeg": true,
	"image/png":  true,
	"image/webp": true,
}

// IsAllowedMimeType проверяет, допустим ли MIME-тип.
func IsAllowedMimeType(mime string) bool {
	return allowedMimeTypes[mime]
}

// AvatarRepo — интерфейс работы с БД аватарок.
type AvatarRepo interface {
	CreateAvatar(ctx context.Context, a *domain.Avatar) (*domain.Avatar, error)
	GetAvatarByID(ctx context.Context, id string) (*domain.Avatar, error)
	GetAvatarByUserID(ctx context.Context, userID string) (*domain.Avatar, error)
	ListAvatarsByUserID(ctx context.Context, userID string) ([]*domain.Avatar, error)
	SoftDeleteAvatar(ctx context.Context, id, userID string) (bool, error)
	SoftDeleteAvatarByUserID(ctx context.Context, userID string) (bool, error)
	UpdateProcessingStatus(ctx context.Context, id, status string, thumbnails map[string]string) error
	UpdateS3Key(ctx context.Context, id, s3Key string) error
}

// FileStorage — интерфейс файлового хранилища.
type FileStorage interface {
	Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
}

// EventProducer — интерфейс отправки событий.
type EventProducer interface {
	Send(ctx context.Context, topic, key string, value interface{}) error
}

// AvatarService — сервис работы с аватарками.
type AvatarService struct {
	db       AvatarRepo
	s3       FileStorage
	producer EventProducer
	topicUp  string
	topicDel string
	logger   *slog.Logger
}

// NewAvatarService создаёт новый сервис.
func NewAvatarService(
	db AvatarRepo,
	s3 FileStorage,
	producer EventProducer,
	topicUpload string,
	topicDelete string,
	logger *slog.Logger,
) *AvatarService {
	return &AvatarService{
		db:       db,
		s3:       s3,
		producer: producer,
		topicUp:  topicUpload,
		topicDel: topicDelete,
		logger:   logger,
	}
}

// Upload загружает аватарку в S3, сохраняет метаданные в БД, отправляет событие в Kafka.
func (s *AvatarService) Upload(ctx context.Context, userID, fileName, mimeType string, size int64, file io.Reader) (*domain.Avatar, error) {
	ctx, span := otel.Tracer("gophprofile.service.avatar").Start(ctx, "service.avatar.upload")
	span.SetAttributes(
		attribute.String("user_id", userID),
		attribute.String("file_name", filepath.Base(fileName)),
		attribute.String("mime_type", mimeType),
		attribute.Int64("file_size", size),
	)
	start := time.Now()
	status := "success"
	defer func() {
		duration := time.Since(start)
		span.SetAttributes(attribute.String("status", status))
		span.End()
		observability.ObserveUpload(userID, status, duration)
		observability.ObserveOperation("service", "upload", status, duration)
	}()

	if size > MaxUploadSize {
		status = "error"
		span.SetStatus(codes.Error, domain.ErrFileTooLarge.Error())
		return nil, domain.ErrFileTooLarge
	}

	if !allowedMimeTypes[mimeType] {
		status = "error"
		span.SetStatus(codes.Error, domain.ErrUnsupportedFormat.Error())
		return nil, domain.ErrUnsupportedFormat
	}

	// Создаём запись в БД с временным s3_key, чтобы получить UUID.
	avatar := &domain.Avatar{
		UserID:    userID,
		FileName:  filepath.Base(fileName),
		MimeType:  mimeType,
		SizeBytes: size,
		S3Key:     "pending",
	}
	created, err := s.db.CreateAvatar(ctx, avatar)
	if err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("create avatar in db: %w", err)
	}
	span.SetAttributes(attribute.String("avatar_id", created.ID))

	// Формируем S3-ключ и загружаем файл.
	s3Key := miniorepo.AvatarKey(userID, created.ID)
	if err := s.s3.Put(ctx, s3Key, file, size, mimeType); err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("upload to s3: %w", err)
	}

	// Обновляем S3-ключ в БД.
	if err := s.db.UpdateS3Key(ctx, created.ID, s3Key); err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("update s3 key in db: %w", err)
	}
	created.S3Key = s3Key

	// Отправляем событие на обработку (создание миниатюр).
	event := domain.AvatarUploadEvent{
		AvatarID: created.ID,
		UserID:   userID,
		S3Key:    s3Key,
	}
	if err := s.producer.Send(ctx, s.topicUp, created.ID, event); err != nil {
		observability.LoggerWithTrace(ctx, s.logger).Error("send upload event failed", "avatar_id", created.ID, "error", err)
	}

	observability.AddStorageUsage(userID, size)
	return created, nil
}

// GetByID возвращает аватарку по ID.
func (s *AvatarService) GetByID(ctx context.Context, id string) (*domain.Avatar, error) {
	ctx, span := otel.Tracer("gophprofile.service.avatar").Start(ctx, "service.avatar.get_by_id")
	span.SetAttributes(attribute.String("avatar_id", id))
	start := time.Now()
	status := "success"
	defer func() {
		duration := time.Since(start)
		span.SetAttributes(attribute.String("status", status))
		span.End()
		observability.ObserveOperation("service", "get_by_id", status, duration)
	}()

	avatar, err := s.db.GetAvatarByID(ctx, id)
	if err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return avatar, nil
}

// GetByUserID возвращает последнюю аватарку пользователя.
func (s *AvatarService) GetByUserID(ctx context.Context, userID string) (*domain.Avatar, error) {
	ctx, span := otel.Tracer("gophprofile.service.avatar").Start(ctx, "service.avatar.get_by_user_id")
	span.SetAttributes(attribute.String("user_id", userID))
	start := time.Now()
	status := "success"
	defer func() {
		duration := time.Since(start)
		span.SetAttributes(attribute.String("status", status))
		span.End()
		observability.ObserveOperation("service", "get_by_user_id", status, duration)
	}()

	avatar, err := s.db.GetAvatarByUserID(ctx, userID)
	if err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return avatar, nil
}

// ListByUserID возвращает все аватарки пользователя.
func (s *AvatarService) ListByUserID(ctx context.Context, userID string) ([]*domain.Avatar, error) {
	ctx, span := otel.Tracer("gophprofile.service.avatar").Start(ctx, "service.avatar.list_by_user_id")
	span.SetAttributes(attribute.String("user_id", userID))
	start := time.Now()
	status := "success"
	defer func() {
		duration := time.Since(start)
		span.SetAttributes(attribute.String("status", status))
		span.End()
		observability.ObserveOperation("service", "list_by_user_id", status, duration)
	}()

	avatars, err := s.db.ListAvatarsByUserID(ctx, userID)
	if err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return avatars, nil
}

// GetFile возвращает содержимое файла из S3.
func (s *AvatarService) GetFile(ctx context.Context, s3Key string) (io.ReadCloser, error) {
	ctx, span := otel.Tracer("gophprofile.service.avatar").Start(ctx, "service.avatar.get_file")
	span.SetAttributes(attribute.String("s3_key", s3Key))
	start := time.Now()
	status := "success"
	defer func() {
		duration := time.Since(start)
		span.SetAttributes(attribute.String("status", status))
		span.End()
		observability.ObserveOperation("service", "get_file", status, duration)
	}()

	rc, err := s.s3.Get(ctx, s3Key)
	if err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return rc, nil
}

// Delete выполняет мягкое удаление и отправляет событие на удаление файлов из S3.
func (s *AvatarService) Delete(ctx context.Context, avatarID, userID string) error {
	ctx, span := otel.Tracer("gophprofile.service.avatar").Start(ctx, "service.avatar.delete")
	span.SetAttributes(
		attribute.String("avatar_id", avatarID),
		attribute.String("user_id", userID),
	)
	start := time.Now()
	status := "success"
	defer func() {
		duration := time.Since(start)
		span.SetAttributes(attribute.String("status", status))
		span.End()
		observability.ObserveOperation("service", "delete", status, duration)
	}()

	avatar, err := s.db.GetAvatarByID(ctx, avatarID)
	if err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("%w: %v", domain.ErrNotFound, err)
	}

	if avatar.UserID != userID {
		status = "error"
		span.SetStatus(codes.Error, domain.ErrForbidden.Error())
		return domain.ErrForbidden
	}

	deleted, err := s.db.SoftDeleteAvatar(ctx, avatarID, userID)
	if err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("soft delete: %w", err)
	}
	if !deleted {
		status = "error"
		span.SetStatus(codes.Error, domain.ErrAlreadyDeleted.Error())
		return domain.ErrAlreadyDeleted
	}

	// Собираем все S3-ключи.
	keys := []string{avatar.S3Key}
	for _, k := range avatar.ThumbnailS3Keys {
		keys = append(keys, k)
	}

	event := domain.AvatarDeleteEvent{AvatarID: avatarID, S3Keys: keys}
	if err := s.producer.Send(ctx, s.topicDel, avatarID, event); err != nil {
		observability.LoggerWithTrace(ctx, s.logger).Error("send delete event failed", "avatar_id", avatarID, "error", err)
	}

	observability.AddStorageUsage(userID, -avatar.SizeBytes)
	return nil
}

// DeleteByUserID удаляет все аватарки пользователя.
func (s *AvatarService) DeleteByUserID(ctx context.Context, userID string) error {
	ctx, span := otel.Tracer("gophprofile.service.avatar").Start(ctx, "service.avatar.delete_by_user_id")
	span.SetAttributes(attribute.String("user_id", userID))
	start := time.Now()
	status := "success"
	defer func() {
		duration := time.Since(start)
		span.SetAttributes(attribute.String("status", status))
		span.End()
		observability.ObserveOperation("service", "delete_by_user_id", status, duration)
	}()

	avatars, err := s.db.ListAvatarsByUserID(ctx, userID)
	if err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("list avatars: %w", err)
	}

	if _, err := s.db.SoftDeleteAvatarByUserID(ctx, userID); err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("soft delete by user: %w", err)
	}

	var totalFreed int64
	for _, avatar := range avatars {
		keys := []string{avatar.S3Key}
		for _, k := range avatar.ThumbnailS3Keys {
			keys = append(keys, k)
		}
		totalFreed += avatar.SizeBytes

		event := domain.AvatarDeleteEvent{AvatarID: avatar.ID, S3Keys: keys}
		if err := s.producer.Send(ctx, s.topicDel, avatar.ID, event); err != nil {
			observability.LoggerWithTrace(ctx, s.logger).Error("send delete event failed", "avatar_id", avatar.ID, "error", err)
		}
	}

	observability.AddStorageUsage(userID, -totalFreed)
	return nil
}
