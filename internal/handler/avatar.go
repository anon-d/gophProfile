// Package handler содержит HTTP-обработчики REST API.
package handler

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/anon-d/gophProfile/internal/domain"
	"github.com/anon-d/gophProfile/internal/observability"
	"github.com/anon-d/gophProfile/internal/service"
)

// AvatarHandler — обработчики для аватарок.
type AvatarHandler struct {
	svc    *service.AvatarService
	logger *slog.Logger
}

// NewAvatarHandler создаёт обработчик.
func NewAvatarHandler(svc *service.AvatarService, logger *slog.Logger) *AvatarHandler {
	return &AvatarHandler{svc: svc, logger: logger}
}

// Upload обрабатывает POST /api/v1/avatars.
func (h *AvatarHandler) Upload(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "X-User-ID header is required"})
		return
	}

	// Ограничиваем размер тела запроса.
	r.Body = http.MaxBytesReader(w, r.Body, service.MaxUploadSize+1024)

	file, header, err := r.FormFile("image")
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]interface{}{
				"error":    "File too large",
				"max_size": service.MaxUploadSize,
			})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Failed to read file: " + err.Error()})
		return
	}
	defer file.Close()

	// Определяем MIME-тип.
	mimeType := header.Header.Get("Content-Type")
	if mimeType == "" || mimeType == "application/octet-stream" {
		// Определяем по расширению файла.
		ext := strings.ToLower(strings.TrimPrefix(strings.ToLower(header.Filename[strings.LastIndex(header.Filename, "."):]), "."))
		switch ext {
		case "jpg", "jpeg":
			mimeType = "image/jpeg"
		case "png":
			mimeType = "image/png"
		case "webp":
			mimeType = "image/webp"
		}
	}

	if !service.IsAllowedMimeType(mimeType) {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error":   "Invalid file format",
			"details": "Supported formats: jpeg, png, webp",
		})
		return
	}

	avatar, err := h.svc.Upload(r.Context(), userID, header.Filename, mimeType, header.Size, file)
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrFileTooLarge):
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]interface{}{
				"error":    "File too large",
				"max_size": service.MaxUploadSize,
			})
		case errors.Is(err, domain.ErrUnsupportedFormat):
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"error":   "Invalid file format",
				"details": "Supported formats: jpeg, png, webp",
			})
		default:
			observability.LoggerWithTrace(r.Context(), h.logger).Error("upload failed", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Internal server error"})
		}
		return
	}

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"id":         avatar.ID,
		"user_id":    avatar.UserID,
		"url":        "/api/v1/avatars/" + avatar.ID,
		"status":     "processing",
		"created_at": avatar.CreatedAt,
	})
}

// GetAvatar обрабатывает GET /api/v1/avatars/{avatar_id}.
func (h *AvatarHandler) GetAvatar(w http.ResponseWriter, r *http.Request) {
	avatarID := chi.URLParam(r, "avatar_id")

	avatar, err := h.svc.GetByID(r.Context(), avatarID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Avatar not found"})
		return
	}

	// Определяем S3-ключ (оригинал или миниатюра).
	s3Key := avatar.S3Key
	requestedSize := r.URL.Query().Get("size")
	if requestedSize != "" && avatar.ThumbnailS3Keys != nil {
		if thumbKey, ok := avatar.ThumbnailS3Keys[requestedSize]; ok {
			s3Key = thumbKey
		}
	}

	// Получаем файл из S3.
	rc, err := h.svc.GetFile(r.Context(), s3Key)
	if err != nil {
		observability.LoggerWithTrace(r.Context(), h.logger).Error("get file from s3 failed", "s3_key", s3Key, "error", err)
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Avatar not found"})
		return
	}
	defer rc.Close()

	w.Header().Set("Content-Type", avatar.MimeType)
	w.Header().Set("Cache-Control", "max-age=86400")
	w.WriteHeader(http.StatusOK)
	io.Copy(w, rc)
}

// GetUserAvatar обрабатывает GET /api/v1/users/{user_id}/avatar.
func (h *AvatarHandler) GetUserAvatar(w http.ResponseWriter, r *http.Request) {
	userID := chi.URLParam(r, "user_id")

	avatar, err := h.svc.GetByUserID(r.Context(), userID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Avatar not found"})
		return
	}

	s3Key := avatar.S3Key
	rc, err := h.svc.GetFile(r.Context(), s3Key)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Avatar not found"})
		return
	}
	defer rc.Close()

	w.Header().Set("Content-Type", avatar.MimeType)
	w.Header().Set("Cache-Control", "max-age=86400")
	w.WriteHeader(http.StatusOK)
	io.Copy(w, rc)
}

// GetMetadata обрабатывает GET /api/v1/avatars/{avatar_id}/metadata.
func (h *AvatarHandler) GetMetadata(w http.ResponseWriter, r *http.Request) {
	avatarID := chi.URLParam(r, "avatar_id")

	avatar, err := h.svc.GetByID(r.Context(), avatarID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Avatar not found"})
		return
	}

	thumbnails := make([]domain.Thumbnail, 0)
	for sizeName := range avatar.ThumbnailS3Keys {
		thumbnails = append(thumbnails, domain.Thumbnail{
			Size: sizeName,
			URL:  "/api/v1/avatars/" + avatarID + "?size=" + sizeName,
		})
	}

	metadata := domain.AvatarMetadata{
		ID:         avatar.ID,
		UserID:     avatar.UserID,
		FileName:   avatar.FileName,
		MimeType:   avatar.MimeType,
		Size:       avatar.SizeBytes,
		Thumbnails: thumbnails,
		CreatedAt:  avatar.CreatedAt,
		UpdatedAt:  avatar.UpdatedAt,
	}

	writeJSON(w, http.StatusOK, metadata)
}

// ListUserAvatars обрабатывает GET /api/v1/users/{user_id}/avatars.
func (h *AvatarHandler) ListUserAvatars(w http.ResponseWriter, r *http.Request) {
	userID := chi.URLParam(r, "user_id")

	avatars, err := h.svc.ListByUserID(r.Context(), userID)
	if err != nil {
		observability.LoggerWithTrace(r.Context(), h.logger).Error("list avatars failed", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Internal server error"})
		return
	}

	writeJSON(w, http.StatusOK, avatars)
}

// DeleteAvatar обрабатывает DELETE /api/v1/avatars/{avatar_id}.
func (h *AvatarHandler) DeleteAvatar(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "X-User-ID header is required"})
		return
	}

	avatarID := chi.URLParam(r, "avatar_id")

	err := h.svc.Delete(r.Context(), avatarID, userID)
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrForbidden):
			writeJSON(w, http.StatusForbidden, map[string]interface{}{
				"error":   "Forbidden",
				"details": "You can only delete your own avatars",
			})
		case errors.Is(err, domain.ErrNotFound), errors.Is(err, domain.ErrAlreadyDeleted):
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "Avatar not found"})
		default:
			observability.LoggerWithTrace(r.Context(), h.logger).Error("delete avatar failed", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Internal server error"})
		}
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// DeleteUserAvatar обрабатывает DELETE /api/v1/users/{user_id}/avatar.
func (h *AvatarHandler) DeleteUserAvatar(w http.ResponseWriter, r *http.Request) {
	headerUserID := r.Header.Get("X-User-ID")
	pathUserID := chi.URLParam(r, "user_id")

	if headerUserID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "X-User-ID header is required"})
		return
	}

	if headerUserID != pathUserID {
		writeJSON(w, http.StatusForbidden, map[string]interface{}{
			"error":   "Forbidden",
			"details": "You can only delete your own avatars",
		})
		return
	}

	err := h.svc.DeleteByUserID(r.Context(), pathUserID)
	if err != nil {
		observability.LoggerWithTrace(r.Context(), h.logger).Error("delete user avatars failed", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Internal server error"})
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
