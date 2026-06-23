package handler

import (
	"log/slog"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/anon-d/gophProfile/internal/observability"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"

	"github.com/anon-d/gophProfile/internal/service"
)

// NewRouter создаёт Chi-роутер со всеми маршрутами.
func NewRouter(
	svc *service.AvatarService,
	pool *pgxpool.Pool,
	minioClient *minio.Client,
	logger *slog.Logger,
) *chi.Mux {
	r := chi.NewRouter()

	// Middleware.
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(observability.HTTPMiddleware(logger))

	avatarH := NewAvatarHandler(svc, logger)
	healthH := NewHealthHandler(pool, minioClient)
	webH := NewWebHandler(svc, logger, "web/templates")

	// Health.
	r.Get("/health", healthH.Health)
	r.Handle("/metrics", observability.MetricsHandler())

	// REST API.
	r.Route("/api/v1", func(r chi.Router) {
		r.Post("/avatars", avatarH.Upload)
		r.Get("/avatars/{avatar_id}", avatarH.GetAvatar)
		r.Get("/avatars/{avatar_id}/metadata", avatarH.GetMetadata)
		r.Delete("/avatars/{avatar_id}", avatarH.DeleteAvatar)

		r.Get("/users/{user_id}/avatar", avatarH.GetUserAvatar)
		r.Get("/users/{user_id}/avatars", avatarH.ListUserAvatars)
		r.Delete("/users/{user_id}/avatar", avatarH.DeleteUserAvatar)
	})

	// Web-интерфейс.
	r.Get("/web/upload", webH.UploadPage)
	r.Post("/web/upload", webH.UploadAction)
	r.Get("/web/gallery/{user_id}", webH.Gallery)

	return r
}
