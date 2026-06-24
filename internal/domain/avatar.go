// Package domain содержит бизнес-модели приложения.
package domain

import "time"

// Статусы загрузки.
const (
	UploadStatusUploading = "uploading"
	UploadStatusDone      = "done"
	UploadStatusFailed    = "failed"
)

// Статусы обработки (создание миниатюр).
const (
	ProcessingStatusPending    = "pending"
	ProcessingStatusProcessing = "processing"
	ProcessingStatusCompleted  = "completed"
	ProcessingStatusFailed     = "failed"
)

// Thumbnail описывает одну миниатюру.
type Thumbnail struct {
	Size string `json:"size"`
	URL  string `json:"url"`
}

// Avatar — основная модель аватарки.
type Avatar struct {
	ID               string      `json:"id"`
	UserID           string      `json:"user_id"`
	FileName         string      `json:"file_name"`
	MimeType         string      `json:"mime_type"`
	SizeBytes        int64       `json:"size_bytes"`
	S3Key            string      `json:"s3_key"`
	ThumbnailS3Keys  map[string]string `json:"thumbnail_s3_keys,omitempty"`
	UploadStatus     string      `json:"upload_status"`
	ProcessingStatus string      `json:"processing_status"`
	CreatedAt        time.Time   `json:"created_at"`
	UpdatedAt        time.Time   `json:"updated_at"`
	DeletedAt        *time.Time  `json:"deleted_at,omitempty"`
}

// AvatarMetadata — ответ на запрос метаданных.
type AvatarMetadata struct {
	ID         string      `json:"id"`
	UserID     string      `json:"user_id"`
	FileName   string      `json:"file_name"`
	MimeType   string      `json:"mime_type"`
	Size       int64       `json:"size"`
	Thumbnails []Thumbnail `json:"thumbnails"`
	CreatedAt  time.Time   `json:"created_at"`
	UpdatedAt  time.Time   `json:"updated_at"`
}

// --- Kafka-события ---

// AvatarUploadEvent — событие для обработки загруженного файла.
type AvatarUploadEvent struct {
	AvatarID string `json:"avatar_id"`
	UserID   string `json:"user_id"`
	S3Key    string `json:"s3_key"`
}

// AvatarDeleteEvent — событие для удаления файлов.
type AvatarDeleteEvent struct {
	AvatarID string   `json:"avatar_id"`
	S3Keys   []string `json:"s3_keys"`
}
