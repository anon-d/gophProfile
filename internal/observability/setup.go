package observability

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Config — параметры инициализации OpenTelemetry.
type Config struct {
	ServiceName  string
	Environment  string
	OTLPEndpoint string
}

// Init настраивает OTel tracer provider и propagator.
func Init(ctx context.Context, cfg Config) (func(context.Context) error, error) {
	service := strings.TrimSpace(cfg.ServiceName)
	if service == "" {
		service = "gophprofile"
	}

	shutdown := func(context.Context) error { return nil }

	if strings.TrimSpace(cfg.OTLPEndpoint) != "" {
		exporter, err := otlptracegrpc.New(
			ctx,
			otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
			otlptracegrpc.WithInsecure(),
		)
		if err != nil {
			return nil, fmt.Errorf("create otlp trace exporter: %w", err)
		}

		res, err := resource.New(
			ctx,
			resource.WithAttributes(
				semconv.ServiceName(service),
				semconv.DeploymentEnvironment(cfg.Environment),
			),
		)
		if err != nil {
			return nil, fmt.Errorf("create otel resource: %w", err)
		}

		tp := sdktrace.NewTracerProvider(
			sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(1.0))),
			sdktrace.WithBatcher(exporter),
			sdktrace.WithResource(res),
		)
		otel.SetTracerProvider(tp)
		shutdown = tp.Shutdown
	}

	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)

	return shutdown, nil
}
