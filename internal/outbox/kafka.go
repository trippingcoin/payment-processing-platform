package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"triple-p/cmd/payment-service/repo"
)

type Envelope struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	SchemaVersion int             `json:"schema_version"`
	AggregateID   string          `json:"aggregate_id"`
	OccurredAt    time.Time       `json:"occurred_at"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	Payload       json.RawMessage `json:"payload"`
}

type KafkaPublisher struct{ writer *kafka.Writer }

func NewKafkaPublisher(brokers []string, topic string, timeout time.Duration) *KafkaPublisher {
	return &KafkaPublisher{writer: &kafka.Writer{
		Addr: kafka.TCP(brokers...), Topic: topic, Balancer: &kafka.Hash{},
		RequiredAcks: kafka.RequireAll, Async: false, MaxAttempts: 1,
		BatchSize: 1, ReadTimeout: timeout, WriteTimeout: timeout,
		AllowAutoTopicCreation: false,
		Transport:              &kafka.Transport{DialTimeout: timeout, ClientID: "payment-outbox"},
	}}
}

func (p *KafkaPublisher) Publish(ctx context.Context, event repo.OutboxEvent) error {
	message, err := Message(event)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, message)
}

func (p *KafkaPublisher) Close() error {
	err := p.writer.Close()
	p.writer.Transport.(*kafka.Transport).CloseIdleConnections()
	return err
}

// Message uses the persisted ID and timestamp on every retry. Header values
// remain strings for trace context propagation and consumer deduplication.
func Message(event repo.OutboxEvent) (kafka.Message, error) {
	if !json.Valid(event.Payload) {
		return kafka.Message{}, fmt.Errorf("invalid payload for event %s", event.ID)
	}
	var metadata map[string]json.RawMessage
	if len(event.Headers) > 0 {
		if err := json.Unmarshal(event.Headers, &metadata); err != nil {
			return kafka.Message{}, err
		}
	}
	envelope := Envelope{EventID: event.ID, EventType: event.EventType, SchemaVersion: 1,
		AggregateID: event.AggregateID, OccurredAt: event.CreatedAt.UTC(), Payload: event.Payload}
	if raw := metadata["correlation_id"]; raw != nil {
		if err := json.Unmarshal(raw, &envelope.CorrelationID); err != nil {
			return kafka.Message{}, err
		}
	}
	if raw := metadata["schema_version"]; raw != nil {
		if err := json.Unmarshal(raw, &envelope.SchemaVersion); err != nil {
			return kafka.Message{}, err
		}
		if envelope.SchemaVersion < 1 {
			return kafka.Message{}, fmt.Errorf("invalid schema_version")
		}
	}
	headers := []kafka.Header{{Key: "event_id", Value: []byte(event.ID)}, {Key: "event_type", Value: []byte(event.EventType)}}
	for key, raw := range metadata {
		if key == "event_id" || key == "event_type" {
			continue
		}
		var value string
		if json.Unmarshal(raw, &value) != nil {
			value = string(raw)
		}
		headers = append(headers, kafka.Header{Key: key, Value: []byte(value)})
	}
	value, err := json.Marshal(envelope)
	return kafka.Message{Key: []byte(event.AggregateID), Value: value, Headers: headers, Time: event.CreatedAt}, err
}
