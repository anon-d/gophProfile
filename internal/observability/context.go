package observability

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// LoggerWithTrace возвращает логгер с trace/span идентификаторами из контекста.
func LoggerWithTrace(ctx context.Context, logger *slog.Logger) *slog.Logger {
	if logger == nil {
		logger = slog.Default()
	}

	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return logger
	}

	return logger.With(
		"trace_id", sc.TraceID().String(),
		"span_id", sc.SpanID().String(),
	)
}
