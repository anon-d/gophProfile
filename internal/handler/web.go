package handler

import (
	"html/template"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/anon-d/gophProfile/internal/service"
)

// WebHandler — обработчики веб-интерфейса.
type WebHandler struct {
	svc      *service.AvatarService
	logger   *slog.Logger
	tmplDir  string
}

// NewWebHandler создаёт веб-обработчик.
func NewWebHandler(svc *service.AvatarService, logger *slog.Logger, tmplDir string) *WebHandler {
	return &WebHandler{svc: svc, logger: logger, tmplDir: tmplDir}
}

// UploadPage отдаёт страницу загрузки.
func (h *WebHandler) UploadPage(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "web/static/index.html")
}

// UploadAction обрабатывает форму загрузки и редиректит на галерею.
func (h *WebHandler) UploadAction(w http.ResponseWriter, r *http.Request) {
	userID := r.FormValue("userId")
	if userID == "" {
		http.Error(w, "User ID is required", http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("image")
	if err != nil {
		http.Error(w, "Failed to read image", http.StatusBadRequest)
		return
	}
	defer file.Close()

	mimeType := header.Header.Get("Content-Type")
	_, err = h.svc.Upload(r.Context(), userID, header.Filename, mimeType, header.Size, file)
	if err != nil {
		h.logger.Error("web upload failed", "error", err)
		http.Error(w, "Upload failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, "/web/gallery/"+userID, http.StatusSeeOther)
}

// Gallery отображает галерею аватарок пользователя.
func (h *WebHandler) Gallery(w http.ResponseWriter, r *http.Request) {
	userID := chi.URLParam(r, "user_id")

	avatars, err := h.svc.ListByUserID(r.Context(), userID)
	if err != nil {
		h.logger.Error("gallery list failed", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	tmpl, err := template.ParseFiles("web/templates/gallery.html")
	if err != nil {
		h.logger.Error("parse gallery template", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	data := struct {
		UserID  string
		Avatars interface{}
	}{
		UserID:  userID,
		Avatars: avatars,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.Execute(w, data); err != nil {
		h.logger.Error("execute gallery template", "error", err)
	}
}
