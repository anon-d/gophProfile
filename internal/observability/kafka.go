package observability

import (
	"context"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// InjectKafkaTrace переносит trace context в Kafka headers.
func InjectKafkaTrace(ctx context.Context, headers *[]sarama.RecordHeader) {
	if headers == nil {
		return
	}

	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	merged := make(map[string]string, len(*headers)+len(carrier))
	for _, h := range *headers {
		merged[string(h.Key)] = string(h.Value)
	}
	for key, value := range carrier {
		merged[key] = value
	}

	out := make([]sarama.RecordHeader, 0, len(merged))
	for key, value := range merged {
		out = append(out, sarama.RecordHeader{
			Key:   []byte(key),
			Value: []byte(value),
		})
	}
	*headers = out
}

// ExtractKafkaTrace извлекает trace context из Kafka headers.
func ExtractKafkaTrace(ctx context.Context, headers []*sarama.RecordHeader) context.Context {
	carrier := propagation.MapCarrier{}
	for _, h := range headers {
		if h == nil {
			continue
		}
		carrier[string(h.Key)] = string(h.Value)
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}
