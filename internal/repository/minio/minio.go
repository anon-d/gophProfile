// Package minio предоставляет репозиторий для хранения файлов аватарок в MinIO (S3).
package minio

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/anon-d/gophProfile/internal/observability"
	"github.com/minio/minio-go/v7"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// Repo — репозиторий для работы с MinIO.
type Repo struct {
	client *minio.Client
	bucket string
}

// New создаёт MinIO-репозиторий, при необходимости создаёт бакет.
func New(ctx context.Context, client *minio.Client, bucket string) (*Repo, error) {
	ctx, span := otel.Tracer("gophprofile.repository.minio").Start(ctx, "minio.new_repo")
	span.SetAttributes(attribute.String("bucket", bucket))
	start := time.Now()
	status := "success"
	defer func() {
		span.SetAttributes(attribute.String("status", status))
		span.End()
		observability.ObserveOperation("minio", "new_repo", status, time.Since(start))
	}()
	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if !exists {
		if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			status = "error"
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, fmt.Errorf("create bucket: %w", err)
		}
	}
	return &Repo{client: client, bucket: bucket}, nil
}

// Client возвращает клиент MinIO для healthcheck.
func (r *Repo) Client() *minio.Client {
	return r.client
}

// AvatarKey формирует ключ для оригинала: avatars/{userID}/{avatarID}.
func AvatarKey(userID, avatarID string) string {
	return fmt.Sprintf("avatars/%s/%s", userID, avatarID)
}

// ThumbnailKey формирует ключ миниатюры.
func ThumbnailKey(avatarID, size string) string {
	return fmt.Sprintf("thumbnails/%s/%s.jpg", avatarID, size)
}

// Put загружает файл в MinIO.
func (r *Repo) Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	ctx, span := otel.Tracer("gophprofile.repository.minio").Start(ctx, "minio.put_object")
	span.SetAttributes(
		attribute.String("bucket", r.bucket),
		attribute.String("key", key),
		attribute.Int64("size", size),
		attribute.String("content_type", contentType),
	)
	start := time.Now()
	status := "success"
	defer func() {
		span.SetAttributes(attribute.String("status", status))
		span.End()
		observability.ObserveOperation("minio", "put_object", status, time.Since(start))
	}()
	_, err := r.client.PutObject(ctx, r.bucket, key, reader, size, minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("put object %s: %w", key, err)
	}
	return nil
}

// PutBytes загружает байты в MinIO.
func (r *Repo) PutBytes(ctx context.Context, key string, data []byte, contentType string) error {
	return r.Put(ctx, key, bytes.NewReader(data), int64(len(data)), contentType)
}

// Get возвращает reader для чтения файла.
func (r *Repo) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	ctx, span := otel.Tracer("gophprofile.repository.minio").Start(ctx, "minio.get_object")
	span.SetAttributes(
		attribute.String("bucket", r.bucket),
		attribute.String("key", key),
	)
	start := time.Now()
	status := "success"
	defer func() {
		span.SetAttributes(attribute.String("status", status))
		span.End()
		observability.ObserveOperation("minio", "get_object", status, time.Since(start))
	}()
	obj, err := r.client.GetObject(ctx, r.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("get object %s: %w", key, err)
	}
	return obj, nil
}

// GetBytes читает файл полностью в память.
func (r *Repo) GetBytes(ctx context.Context, key string) ([]byte, error) {
	rc, err := r.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// Delete удаляет объект.
func (r *Repo) Delete(ctx context.Context, key string) error {
	ctx, span := otel.Tracer("gophprofile.repository.minio").Start(ctx, "minio.delete_object")
	span.SetAttributes(
		attribute.String("bucket", r.bucket),
		attribute.String("key", key),
	)
	start := time.Now()
	status := "success"
	defer func() {
		span.SetAttributes(attribute.String("status", status))
		span.End()
		observability.ObserveOperation("minio", "delete_object", status, time.Since(start))
	}()
	if err := r.client.RemoveObject(ctx, r.bucket, key, minio.RemoveObjectOptions{}); err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("delete object %s: %w", key, err)
	}
	return nil
}
