package observability

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
)

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(data []byte) (int, error) {
	if rw.status == 0 {
		rw.status = http.StatusOK
	}
	return rw.ResponseWriter.Write(data)
}

// HTTPMiddleware добавляет метрики и структурированные логи HTTP-запросов.
func HTTPMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}

			next.ServeHTTP(rw, r)

			route := r.URL.Path
			if routeCtx := chi.RouteContext(r.Context()); routeCtx != nil {
				if pattern := routeCtx.RoutePattern(); pattern != "" {
					route = pattern
				}
			}

			duration := time.Since(start)
			ObserveHTTPRequest(r.Method, route, rw.status, duration)

			reqLogger := LoggerWithTrace(r.Context(), logger)
			logAttrs := []any{
				"method", r.Method,
				"route", route,
				"status", rw.status,
				"duration_ms", duration.Milliseconds(),
				"remote_addr", r.RemoteAddr,
			}

			switch {
			case rw.status >= http.StatusInternalServerError:
				reqLogger.Error("http request completed", logAttrs...)
			case rw.status >= http.StatusBadRequest:
				reqLogger.Warn("http request completed", logAttrs...)
			default:
				reqLogger.Info("http request completed", logAttrs...)
			}
		})
	}
}
