package dlqreplay

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaStore struct {
	brokers []string
	timeout time.Duration
}

func NewKafkaStore(brokers []string, timeout time.Duration) (*KafkaStore, error) {
	if len(brokers) == 0 || timeout <= 0 {
		return nil, fmt.Errorf("brokers and positive timeout are required")
	}
	for _, broker := range brokers {
		if strings.TrimSpace(broker) == "" {
			return nil, fmt.Errorf("empty Kafka broker")
		}
	}
	return &KafkaStore{brokers: append([]string(nil), brokers...), timeout: timeout}, nil
}

func (s *KafkaStore) Read(ctx context.Context, r Request) (kafka.Message, error) {
	if err := r.Validate(); err != nil {
		return kafka.Message{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	// No GroupID: direct partition reads cannot join/rebalance/commit a group.
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: s.brokers, Topic: r.DLQTopic, Partition: r.Partition,
		MinBytes: 1, MaxBytes: 20 * 1024 * 1024, MaxWait: time.Second, QueueCapacity: 1,
		OffsetOutOfRangeError: true, Dialer: &kafka.Dialer{Timeout: s.timeout}})
	defer reader.Close()
	if err := reader.SetOffset(r.Offset); err != nil {
		return kafka.Message{}, err
	}
	return reader.FetchMessage(ctx)
}

func (s *KafkaStore) Publish(ctx context.Context, m kafka.Message) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	transport := &kafka.Transport{DialTimeout: s.timeout, ClientID: "dlq-replay"}
	defer transport.CloseIdleConnections()
	writer := &kafka.Writer{Addr: kafka.TCP(s.brokers...), Balancer: &kafka.Hash{},
		RequiredAcks: kafka.RequireAll, Async: false, MaxAttempts: 1, BatchSize: 1,
		BatchBytes: 20 * 1024 * 1024, ReadTimeout: s.timeout, WriteTimeout: s.timeout,
		AllowAutoTopicCreation: false, Transport: transport}
	defer writer.Close()
	return writer.WriteMessages(ctx, m)
}
