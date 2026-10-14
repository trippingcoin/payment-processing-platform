package eventconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// DLQ writes and source commits are not an atomic Kafka transaction. Required
// acknowledgements prevent intentional loss; stable dlq_id identifies duplicates.
type KafkaDeadLetterPublisher struct{ writer *kafka.Writer }

func NewKafkaDeadLetterPublisher(brokers []string, sourceTopic, topic string, timeout time.Duration) (*KafkaDeadLetterPublisher, error) {
	if len(brokers) == 0 || strings.TrimSpace(sourceTopic) == "" || strings.TrimSpace(topic) == "" || sourceTopic == topic || timeout <= 0 {
		return nil, fmt.Errorf("DLQ needs brokers, positive timeout and a topic distinct from source")
	}
	for _, broker := range brokers {
		if strings.TrimSpace(broker) == "" {
			return nil, fmt.Errorf("empty DLQ broker")
		}
	}
	return &KafkaDeadLetterPublisher{writer: &kafka.Writer{
		Addr: kafka.TCP(brokers...), Topic: topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll,
		Async: false, MaxAttempts: 1, BatchSize: 1, BatchBytes: 20 * 1024 * 1024, ReadTimeout: timeout, WriteTimeout: timeout,
		AllowAutoTopicCreation: false, Transport: &kafka.Transport{DialTimeout: timeout, ClientID: "consumer-dlq"},
	}}, nil
}

func (p *KafkaDeadLetterPublisher) PublishDeadLetter(ctx context.Context, d DeadLetter) error {
	body, err := json.Marshal(d)
	if err != nil {
		return err
	}
	err = p.writer.WriteMessages(ctx, kafka.Message{Key: []byte(d.ID), Value: body, Time: d.FailedAt,
		Headers: []kafka.Header{{Key: "dlq_id", Value: []byte(d.ID)}, {Key: "consumer_group", Value: []byte(d.ConsumerGroup)}}})
	return singleRecordWriteError(err)
}

// kafka.WriteErrors does not implement Unwrap. We publish exactly one record;
// expose its failure so classification and errors.Is/As retain the real cause.
func singleRecordWriteError(err error) error {
	var failures kafka.WriteErrors
	if errors.As(err, &failures) && len(failures) == 1 && failures[0] != nil {
		return fmt.Errorf("publish DLQ record: %w", failures[0])
	}
	return err
}
func (p *KafkaDeadLetterPublisher) Close() error {
	err := p.writer.Close()
	p.writer.Transport.(*kafka.Transport).CloseIdleConnections()
	return err
}
