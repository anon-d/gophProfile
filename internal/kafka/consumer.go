package kafka

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/IBM/sarama"
)

// MessageHandler — функция обработки сообщения.
type MessageHandler func(ctx context.Context, msg *sarama.ConsumerMessage) error

// Consumer — consumer-group обёртка.
type Consumer struct {
	group   sarama.ConsumerGroup
	handler *groupHandler
	topics  []string
	logger  *slog.Logger
}

// NewConsumer создаёт Kafka-консьюмера.
func NewConsumer(brokers []string, groupID string, topics []string, handler MessageHandler, logger *slog.Logger) (*Consumer, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	group, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		return nil, fmt.Errorf("new consumer group: %w", err)
	}

	return &Consumer{
		group:   group,
		handler: &groupHandler{handler: handler, logger: logger},
		topics:  topics,
		logger:  logger,
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
}

func (h *groupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *groupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *groupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if err := h.handler(session.Context(), msg); err != nil {
			h.logger.Error("handle message failed",
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"error", err,
			)
			// Продолжаем обработку — не крашим воркер.
		}
		session.MarkMessage(msg, "")
	}
	return nil
}
