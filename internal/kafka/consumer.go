package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/IBM/sarama"
	"github.com/anon-d/gophProfile/internal/observability"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// MessageHandler — функция обработки сообщения.
type MessageHandler func(ctx context.Context, msg *sarama.ConsumerMessage) error

// Consumer — consumer-group обёртка.
type Consumer struct {
	group   sarama.ConsumerGroup
	handler *groupHandler
	topics  []string
	logger  *slog.Logger
	service string
}

// NewConsumer создаёт Kafka-консьюмера.
func NewConsumer(service string, brokers []string, groupID string, topics []string, handler MessageHandler, logger *slog.Logger) (*Consumer, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	group, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		return nil, fmt.Errorf("new consumer group: %w", err)
	}

	return &Consumer{
		group:   group,
		handler: &groupHandler{handler: handler, logger: logger, service: service},
		topics:  topics,
		logger:  logger,
		service: service,
	}, nil
}

// Run запускает чтение сообщений. Блокирует до отмены контекста.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		if err := c.group.Consume(ctx, c.topics, c.handler); err != nil {
			c.logger.Error("consumer group error", "error", err)
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// Close закрывает консьюмера.
func (c *Consumer) Close() error {
	return c.group.Close()
}

// groupHandler реализует интерфейс sarama.ConsumerGroupHandler.
type groupHandler struct {
	handler MessageHandler
	logger  *slog.Logger
	service string
}

func (h *groupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *groupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *groupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		ctx := observability.ExtractKafkaTrace(session.Context(), msg.Headers)
		ctx, span := otel.Tracer("gophprofile.kafka.consumer").Start(ctx, "kafka.consumer.handle_message")
		span.SetAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", msg.Topic),
			attribute.Int("messaging.kafka.partition", int(msg.Partition)),
			attribute.Int64("messaging.kafka.offset", msg.Offset),
		)
		start := time.Now()
		status := "success"
		observability.AddQueueDepth(h.service, msg.Topic, 1)

		if err := h.handler(ctx, msg); err != nil {
			status = "error"
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			observability.LoggerWithTrace(ctx, h.logger).Error("handle message failed",
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"error", err,
			)
			// Продолжаем обработку — не крашим воркер.
		}
		observability.ObserveKafkaMessage(h.service, msg.Topic, "consumer", status)
		observability.ObserveOperation("kafka_consumer", "handle_message", status, time.Since(start))
		observability.AddQueueDepth(h.service, msg.Topic, -1)
		span.End()
		session.MarkMessage(msg, "")
	}
	return nil
}
