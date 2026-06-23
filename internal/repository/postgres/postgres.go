// Package postgres предоставляет репозиторий для работы с аватарками в PostgreSQL.
package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/anon-d/gophProfile/internal/observability"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/anon-d/gophProfile/internal/domain"
	db "github.com/anon-d/gophProfile/internal/repository/postgres/gen"
)

// Repo — репозиторий аватарок в PostgreSQL.
type Repo struct {
	q    *db.Queries
	pool *pgxpool.Pool
}

// New создаёт новый экземпляр Repo.
func New(pool *pgxpool.Pool) *Repo {
	return &Repo{
		q:    db.New(pool),
		pool: pool,
	}
}

// Pool возвращает pgxpool.Pool для healthcheck.
func (r *Repo) Pool() *pgxpool.Pool {
	return r.pool
}

// CreateAvatar создаёт запись аватарки в БД.
func (r *Repo) CreateAvatar(ctx context.Context, a *domain.Avatar) (*domain.Avatar, error) {
	ctx, span := otel.Tracer("gophprofile.repository.postgres").Start(ctx, "postgres.create_avatar")
	span.SetAttributes(
		attribute.String("db.system", "postgresql"),
		attribute.String("db.operation", "insert"),
		attribute.String("user_id", a.UserID),
	)
	start := time.Now()
	status := "success"
	defer func() {
		span.SetAttributes(attribute.String("status", status))
		span.End()
		observability.ObserveOperation("postgres", "create_avatar", status, time.Since(start))
	}()
	row, err := r.q.CreateAvatar(ctx, db.CreateAvatarParams{
		UserID:    a.UserID,
		FileName:  a.FileName,
		MimeType:  a.MimeType,
		SizeBytes: a.SizeBytes,
		S3Key:     a.S3Key,
	})
	if err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("create avatar: %w", err)
	}
	span.SetAttributes(attribute.String("avatar_id", uuidToString(row.ID)))
	return rowToAvatar(row), nil
}

// GetAvatarByID возвращает аватарку по ID.
func (r *Repo) GetAvatarByID(ctx context.Context, id string) (*domain.Avatar, error) {
	ctx, span := otel.Tracer("gophprofile.repository.postgres").Start(ctx, "postgres.get_avatar_by_id")
	span.SetAttributes(
		attribute.String("db.system", "postgresql"),
		attribute.String("db.operation", "select"),
		attribute.String("avatar_id", id),
	)
	start := time.Now()
	status := "success"
	defer func() {
		span.SetAttributes(attribute.String("status", status))
		span.End()
		observability.ObserveOperation("postgres", "get_avatar_by_id", status, time.Since(start))
	}()
	row, err := r.q.GetAvatarByID(ctx, uuidToPgtype(id))
	if err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("get avatar: %w", err)
	}
	return rowToAvatar2(row), nil
}

// GetAvatarByUserID возвращает последнюю аватарку пользователя.
func (r *Repo) GetAvatarByUserID(ctx context.Context, userID string) (*domain.Avatar, error) {
	ctx, span := otel.Tracer("gophprofile.repository.postgres").Start(ctx, "postgres.get_avatar_by_user_id")
	span.SetAttributes(
		attribute.String("db.system", "postgresql"),
		attribute.String("db.operation", "select"),
		attribute.String("user_id", userID),
	)
	start := time.Now()
	status := "success"
	defer func() {
		span.SetAttributes(attribute.String("status", status))
		span.End()
		observability.ObserveOperation("postgres", "get_avatar_by_user_id", status, time.Since(start))
	}()
	row, err := r.q.GetAvatarByUserID(ctx, userID)
	if err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("get avatar by user: %w", err)
	}
	return rowToAvatar2(row), nil
}

// ListAvatarsByUserID возвращает все аватарки пользователя.
func (r *Repo) ListAvatarsByUserID(ctx context.Context, userID string) ([]*domain.Avatar, error) {
	ctx, span := otel.Tracer("gophprofile.repository.postgres").Start(ctx, "postgres.list_avatars_by_user_id")
	span.SetAttributes(
		attribute.String("db.system", "postgresql"),
		attribute.String("db.operation", "select"),
		attribute.String("user_id", userID),
	)
	start := time.Now()
	status := "success"
	defer func() {
		span.SetAttributes(attribute.String("status", status))
		span.End()
		observability.ObserveOperation("postgres", "list_avatars_by_user_id", status, time.Since(start))
	}()
	rows, err := r.q.ListAvatarsByUserID(ctx, userID)
	if err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("list avatars: %w", err)
	}
	avatars := make([]*domain.Avatar, 0, len(rows))
	for _, row := range rows {
		avatars = append(avatars, rowToAvatar2(row))
	}
	return avatars, nil
}

// SoftDeleteAvatar выполняет мягкое удаление. Возвращает true, если строка была обновлена.
func (r *Repo) SoftDeleteAvatar(ctx context.Context, id, userID string) (bool, error) {
	ctx, span := otel.Tracer("gophprofile.repository.postgres").Start(ctx, "postgres.soft_delete_avatar")
	span.SetAttributes(
		attribute.String("db.system", "postgresql"),
		attribute.String("db.operation", "update"),
		attribute.String("avatar_id", id),
		attribute.String("user_id", userID),
	)
	start := time.Now()
	status := "success"
	defer func() {
		span.SetAttributes(attribute.String("status", status))
		span.End()
		observability.ObserveOperation("postgres", "soft_delete_avatar", status, time.Since(start))
	}()
	tag, err := r.q.SoftDeleteAvatar(ctx, db.SoftDeleteAvatarParams{
		ID:     uuidToPgtype(id),
		UserID: userID,
	})
	if err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return false, fmt.Errorf("soft delete avatar: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// SoftDeleteAvatarByUserID удаляет все аватарки пользователя.
func (r *Repo) SoftDeleteAvatarByUserID(ctx context.Context, userID string) (bool, error) {
	ctx, span := otel.Tracer("gophprofile.repository.postgres").Start(ctx, "postgres.soft_delete_avatar_by_user_id")
	span.SetAttributes(
		attribute.String("db.system", "postgresql"),
		attribute.String("db.operation", "update"),
		attribute.String("user_id", userID),
	)
	start := time.Now()
	status := "success"
	defer func() {
		span.SetAttributes(attribute.String("status", status))
		span.End()
		observability.ObserveOperation("postgres", "soft_delete_avatar_by_user_id", status, time.Since(start))
	}()
	tag, err := r.q.SoftDeleteAvatarByUserID(ctx, userID)
	if err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return false, fmt.Errorf("soft delete avatar by user: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// UpdateProcessingStatus обновляет статус обработки и ключи миниатюр.
func (r *Repo) UpdateProcessingStatus(ctx context.Context, id, status string, thumbnails map[string]string) error {
	ctx, span := otel.Tracer("gophprofile.repository.postgres").Start(ctx, "postgres.update_processing_status")
	span.SetAttributes(
		attribute.String("db.system", "postgresql"),
		attribute.String("db.operation", "update"),
		attribute.String("avatar_id", id),
		attribute.String("processing_status", status),
	)
	start := time.Now()
	opStatus := "success"
	defer func() {
		span.SetAttributes(attribute.String("status", opStatus))
		span.End()
		observability.ObserveOperation("postgres", "update_processing_status", opStatus, time.Since(start))
	}()
	var thumbJSON []byte
	if thumbnails != nil {
		var err error
		thumbJSON, err = json.Marshal(thumbnails)
		if err != nil {
			opStatus = "error"
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return fmt.Errorf("marshal thumbnails: %w", err)
		}
	}
	if err := r.q.UpdateProcessingStatus(ctx, db.UpdateProcessingStatusParams{
		ID:               uuidToPgtype(id),
		ProcessingStatus: pgtype.Text{String: status, Valid: true},
		ThumbnailS3Keys:  thumbJSON,
	}); err != nil {
		opStatus = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

// UpdateS3Key обновляет S3-ключ аватарки.
func (r *Repo) UpdateS3Key(ctx context.Context, id, s3Key string) error {
	ctx, span := otel.Tracer("gophprofile.repository.postgres").Start(ctx, "postgres.update_s3_key")
	span.SetAttributes(
		attribute.String("db.system", "postgresql"),
		attribute.String("db.operation", "update"),
		attribute.String("avatar_id", id),
		attribute.String("s3_key", s3Key),
	)
	start := time.Now()
	status := "success"
	defer func() {
		span.SetAttributes(attribute.String("status", status))
		span.End()
		observability.ObserveOperation("postgres", "update_s3_key", status, time.Since(start))
	}()

	if err := r.q.UpdateS3Key(ctx, db.UpdateS3KeyParams{
		ID:    uuidToPgtype(id),
		S3Key: s3Key,
	}); err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

// --- помощники ---

func uuidToPgtype(s string) pgtype.UUID {
	var u pgtype.UUID
	_ = u.Scan(s)
	return u
}

func uuidToString(u pgtype.UUID) string {
	if !u.Valid {
		return ""
	}
	b := u.Bytes
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func parseThumbnails(data []byte) map[string]string {
	if len(data) == 0 {
		return nil
	}
	var m map[string]string
	_ = json.Unmarshal(data, &m)
	return m
}

func rowToAvatar(row db.CreateAvatarRow) *domain.Avatar {
	return &domain.Avatar{
		ID:               uuidToString(row.ID),
		UserID:           row.UserID,
		FileName:         row.FileName,
		MimeType:         row.MimeType,
		SizeBytes:        row.SizeBytes,
		S3Key:            row.S3Key,
		ThumbnailS3Keys:  parseThumbnails(row.ThumbnailS3Keys),
		UploadStatus:     row.UploadStatus.String,
		ProcessingStatus: row.ProcessingStatus.String,
		CreatedAt:        row.CreatedAt.Time,
		UpdatedAt:        row.UpdatedAt.Time,
	}
}

func rowToAvatar2(row db.GetAvatarByIDRow) *domain.Avatar {
	return &domain.Avatar{
		ID:               uuidToString(row.ID),
		UserID:           row.UserID,
		FileName:         row.FileName,
		MimeType:         row.MimeType,
		SizeBytes:        row.SizeBytes,
		S3Key:            row.S3Key,
		ThumbnailS3Keys:  parseThumbnails(row.ThumbnailS3Keys),
		UploadStatus:     row.UploadStatus.String,
		ProcessingStatus: row.ProcessingStatus.String,
		CreatedAt:        row.CreatedAt.Time,
		UpdatedAt:        row.UpdatedAt.Time,
	}
}
