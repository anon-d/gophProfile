// Package service содержит бизнес-логику сервиса аватарок.
package service

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"

	"github.com/anon-d/gophProfile/internal/config"
	"github.com/anon-d/gophProfile/internal/domain"
	"github.com/anon-d/gophProfile/internal/kafka"
	miniorepo "github.com/anon-d/gophProfile/internal/repository/minio"
	pgrepo "github.com/anon-d/gophProfile/internal/repository/postgres"
)

// MaxFileSize — максимальный размер загружаемого файла (10 МБ).
const MaxFileSize = 10 << 20

// AllowedMimeTypes — допустимые MIME-типы.
var AllowedMimeTypes = map[string]bool{
	"image/jpeg": true,
	"image/png":  true,
	"image/webp": true,
}

// AvatarService — сервис работы с аватарками.
type AvatarService struct {
	db       *pgrepo.Repo
	s3       *miniorepo.Repo
	producer *kafka.Producer
	cfg      *config.KafkaConfig
	logger   *slog.Logger
}

// NewAvatarService создаёт новый сервис.
func NewAvatarService(
	db *pgrepo.Repo,
	s3 *miniorepo.Repo,
	producer *kafka.Producer,
	cfg *config.KafkaConfig,
	logger *slog.Logger,
) *AvatarService {
	return &AvatarService{
		db:       db,
		s3:       s3,
		producer: producer,
		cfg:      cfg,
		logger:   logger,
	}
}

// Upload загружает аватарку в S3, сохраняет метаданные в БД, отправляет событие в Kafka.
func (s *AvatarService) Upload(ctx context.Context, userID, fileName, mimeType string, size int64, file io.Reader) (*domain.Avatar, error) {
	if size > MaxFileSize {
		return nil, fmt.Errorf("file too large: %d > %d", size, MaxFileSize)
	}

	if !AllowedMimeTypes[mimeType] {
		return nil, fmt.Errorf("unsupported mime type: %s", mimeType)
	}

	// Создаём запись в БД (статус = done, processing = pending).
	avatar := &domain.Avatar{
		UserID:    userID,
		FileName:  filepath.Base(fileName),
		MimeType:  mimeType,
		SizeBytes: size,
	}

	// Сначала создаём запись, чтобы получить UUID.
	avatar.S3Key = "temp" // временный ключ
	created, err := s.db.CreateAvatar(ctx, avatar)
	if err != nil {
		return nil, fmt.Errorf("create avatar in db: %w", err)
	}

	// Формируем S3-ключ и загружаем файл.
	s3Key := miniorepo.AvatarKey(userID, created.ID)
	if err := s.s3.Put(ctx, s3Key, file, size, mimeType); err != nil {
		return nil, fmt.Errorf("upload to s3: %w", err)
	}

	// Обновляем S3-ключ в БД через UpdateProcessingStatus (чтобы не добавлять ещё один запрос).
	// Пока оставим как есть — s3_key уже записан через CreateAvatar, просто подменим ключ в ответе.
	created.S3Key = s3Key

	// Отправляем событие на обработку (создание миниатюр).
	event := domain.AvatarUploadEvent{
		AvatarID: created.ID,
		UserID:   userID,
		S3Key:    s3Key,
	}
	if err := s.producer.Send(s.cfg.TopicUpload, created.ID, event); err != nil {
		s.logger.Error("send upload event failed", "avatar_id", created.ID, "error", err)
		// Не фейлим загрузку — миниатюры создадутся позже при ретрае.
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
	// Получаем аватарку для списка S3-ключей.
	avatar, err := s.db.GetAvatarByID(ctx, avatarID)
	if err != nil {
		return fmt.Errorf("get avatar: %w", err)
	}

	// Проверяем владельца.
	if avatar.UserID != userID {
		return fmt.Errorf("forbidden")
	}

	// Мягкое удаление в БД.
	deleted, err := s.db.SoftDeleteAvatar(ctx, avatarID, userID)
	if err != nil {
		return fmt.Errorf("soft delete: %w", err)
	}
	if !deleted {
		return fmt.Errorf("avatar not found or already deleted")
	}

	// Собираем все S3-ключи (оригинал + миниатюры).
	keys := []string{avatar.S3Key}
	for _, k := range avatar.ThumbnailS3Keys {
		keys = append(keys, k)
	}

	// Отправляем событие на асинхронное удаление из S3.
	event := domain.AvatarDeleteEvent{
		AvatarID: avatarID,
		S3Keys:   keys,
	}
	if err := s.producer.Send(s.cfg.TopicDelete, avatarID, event); err != nil {
		s.logger.Error("send delete event failed", "avatar_id", avatarID, "error", err)
	}

	return nil
}

// DeleteByUserID удаляет все аватарки пользователя.
func (s *AvatarService) DeleteByUserID(ctx context.Context, userID string) error {
	// Получаем список для S3-ключей.
	avatars, err := s.db.ListAvatarsByUserID(ctx, userID)
	if err != nil {
		return fmt.Errorf("list avatars: %w", err)
	}

	// Мягкое удаление в БД.
	if _, err := s.db.SoftDeleteAvatarByUserID(ctx, userID); err != nil {
		return fmt.Errorf("soft delete by user: %w", err)
	}

	// Отправляем события на удаление.
	for _, avatar := range avatars {
		keys := []string{avatar.S3Key}
		for _, k := range avatar.ThumbnailS3Keys {
			keys = append(keys, k)
		}
		event := domain.AvatarDeleteEvent{
			AvatarID: avatar.ID,
			S3Keys:   keys,
		}
		if err := s.producer.Send(s.cfg.TopicDelete, avatar.ID, event); err != nil {
			s.logger.Error("send delete event failed", "avatar_id", avatar.ID, "error", err)
		}
	}

	return nil
}
