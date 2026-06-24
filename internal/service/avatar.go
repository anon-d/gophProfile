// Package service содержит бизнес-логику сервиса аватарок.
package service

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"

	"github.com/anon-d/gophProfile/internal/domain"
	miniorepo "github.com/anon-d/gophProfile/internal/repository/minio"
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
	Send(topic, key string, value interface{}) error
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
	if size > MaxUploadSize {
		return nil, domain.ErrFileTooLarge
	}

	if !allowedMimeTypes[mimeType] {
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
		return nil, fmt.Errorf("create avatar in db: %w", err)
	}

	// Формируем S3-ключ и загружаем файл.
	s3Key := miniorepo.AvatarKey(userID, created.ID)
	if err := s.s3.Put(ctx, s3Key, file, size, mimeType); err != nil {
		return nil, fmt.Errorf("upload to s3: %w", err)
	}

	// Обновляем S3-ключ в БД.
	if err := s.db.UpdateS3Key(ctx, created.ID, s3Key); err != nil {
		s.logger.Error("update s3 key failed", "avatar_id", created.ID, "error", err)
	}
	created.S3Key = s3Key

	// Отправляем событие на обработку (создание миниатюр).
	event := domain.AvatarUploadEvent{
		AvatarID: created.ID,
		UserID:   userID,
		S3Key:    s3Key,
	}
	if err := s.producer.Send(s.topicUp, created.ID, event); err != nil {
		s.logger.Error("send upload event failed", "avatar_id", created.ID, "error", err)
	}

	return created, nil
}

// GetByID возвращает аватарку по ID.
func (s *AvatarService) GetByID(ctx context.Context, id string) (*domain.Avatar, error) {
	return s.db.GetAvatarByID(ctx, id)
}

// GetByUserID возвращает последнюю аватарку пользователя.
func (s *AvatarService) GetByUserID(ctx context.Context, userID string) (*domain.Avatar, error) {
	return s.db.GetAvatarByUserID(ctx, userID)
}

// ListByUserID возвращает все аватарки пользователя.
func (s *AvatarService) ListByUserID(ctx context.Context, userID string) ([]*domain.Avatar, error) {
	return s.db.ListAvatarsByUserID(ctx, userID)
}

// GetFile возвращает содержимое файла из S3.
func (s *AvatarService) GetFile(ctx context.Context, s3Key string) (io.ReadCloser, error) {
	return s.s3.Get(ctx, s3Key)
}

// Delete выполняет мягкое удаление и отправляет событие на удаление файлов из S3.
func (s *AvatarService) Delete(ctx context.Context, avatarID, userID string) error {
	avatar, err := s.db.GetAvatarByID(ctx, avatarID)
	if err != nil {
		return fmt.Errorf("%w: %v", domain.ErrNotFound, err)
	}

	if avatar.UserID != userID {
		return domain.ErrForbidden
	}

	deleted, err := s.db.SoftDeleteAvatar(ctx, avatarID, userID)
	if err != nil {
		return fmt.Errorf("soft delete: %w", err)
	}
	if !deleted {
		return domain.ErrAlreadyDeleted
	}

	// Собираем все S3-ключи.
	keys := []string{avatar.S3Key}
	for _, k := range avatar.ThumbnailS3Keys {
		keys = append(keys, k)
	}

	event := domain.AvatarDeleteEvent{AvatarID: avatarID, S3Keys: keys}
	if err := s.producer.Send(s.topicDel, avatarID, event); err != nil {
		s.logger.Error("send delete event failed", "avatar_id", avatarID, "error", err)
	}

	return nil
}

// DeleteByUserID удаляет все аватарки пользователя.
func (s *AvatarService) DeleteByUserID(ctx context.Context, userID string) error {
	avatars, err := s.db.ListAvatarsByUserID(ctx, userID)
	if err != nil {
		return fmt.Errorf("list avatars: %w", err)
	}

	if _, err := s.db.SoftDeleteAvatarByUserID(ctx, userID); err != nil {
		return fmt.Errorf("soft delete by user: %w", err)
	}

	for _, avatar := range avatars {
		keys := []string{avatar.S3Key}
		for _, k := range avatar.ThumbnailS3Keys {
			keys = append(keys, k)
		}
		event := domain.AvatarDeleteEvent{AvatarID: avatar.ID, S3Keys: keys}
		if err := s.producer.Send(s.topicDel, avatar.ID, event); err != nil {
			s.logger.Error("send delete event failed", "avatar_id", avatar.ID, "error", err)
		}
	}

	return nil
}
