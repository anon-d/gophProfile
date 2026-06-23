// Package kafka предоставляет producer и consumer для работы с Kafka.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/anon-d/gophProfile/internal/observability"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// Producer — обёртка над sarama.SyncProducer.
type Producer struct {
	producer sarama.SyncProducer
	service  string
}

// NewProducer создаёт Kafka-продюсера.
func NewProducer(service string, brokers []string) (*Producer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 3

	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("new sync producer: %w", err)
	}
	return &Producer{producer: producer, service: service}, nil
}

// Send отправляет JSON-событие в указанный топик.
func (p *Producer) Send(ctx context.Context, topic, key string, value interface{}) error {
	if ctx == nil {
		ctx = context.Background()
	}

	ctx, span := otel.Tracer("gophprofile.kafka.producer").Start(ctx, "kafka.producer.send")
	span.SetAttributes(
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.destination", topic),
		attribute.String("messaging.kafka.message_key", key),
	)

	start := time.Now()
	status := "success"
	defer func() {
		span.SetAttributes(attribute.String("status", status))
		span.End()
		observability.ObserveOperation("kafka_producer", "send", status, time.Since(start))
		observability.ObserveKafkaMessage(p.service, topic, "producer", status)
	}()
	data, err := json.Marshal(value)
	if err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("marshal event: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(data),
		Headers: []sarama.RecordHeader{
			{Key: []byte("content-type"), Value: []byte("application/json")},
		},
	}
	observability.InjectKafkaTrace(ctx, &msg.Headers)

	_, _, err = p.producer.SendMessage(msg)
	if err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("send message: %w", err)
	}
	return nil
}

// Close закрывает продюсера.
func (p *Producer) Close() error {
	return p.producer.Close()
}
