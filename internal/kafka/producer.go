// Package kafka предоставляет producer и consumer для работы с Kafka.
package kafka

import (
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
)

// Producer — обёртка над sarama.SyncProducer.
type Producer struct {
	producer sarama.SyncProducer
}

// NewProducer создаёт Kafka-продюсера.
func NewProducer(brokers []string) (*Producer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 3

	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("new sync producer: %w", err)
	}
	return &Producer{producer: producer}, nil
}

// Send отправляет JSON-событие в указанный топик.
func (p *Producer) Send(topic, key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(data),
	}

	_, _, err = p.producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("send message: %w", err)
	}
	return nil
}

// Close закрывает продюсера.
func (p *Producer) Close() error {
	return p.producer.Close()
}
