// Package dlqreplay provides single-record, operator-confirmed DLQ replay.
package dlqreplay

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"triple-p/internal/eventconsumer"
)

type Request struct {
	DLQTopic      string
	Partition     int
	Offset        int64
	SourceTopic   string
	ConsumerGroup string
	DLQID         string
	Execute       bool
}

func (r Request) Validate() error {
	if strings.TrimSpace(r.DLQTopic) == "" || strings.TrimSpace(r.SourceTopic) == "" || strings.TrimSpace(r.ConsumerGroup) == "" {
		return fmt.Errorf("dlq-topic, source-topic and consumer-group are required")
	}
	if r.DLQTopic == r.SourceTopic || r.Partition < 0 || r.Offset < 0 {
		return fmt.Errorf("select a nonnegative DLQ partition/offset and a distinct source topic")
	}
	if r.Execute && strings.TrimSpace(r.DLQID) == "" {
		return fmt.Errorf("execute requires dlq-id from a previous dry run")
	}
	return nil
}

// Store deliberately has no commit/delete method. Replay never advances any
// consumer group, removes quarantine records, or edits Inbox/payment state.
type Store interface {
	Read(context.Context, Request) (kafka.Message, error)
	Publish(context.Context, kafka.Message) error
}

type Result struct {
	Status          string `json:"status"`
	DLQID           string `json:"dlq_id"`
	DLQTopic        string `json:"dlq_topic"`
	DLQPartition    int    `json:"dlq_partition"`
	DLQOffset       int64  `json:"dlq_offset"`
	EventID         string `json:"event_id"`
	ConsumerGroup   string `json:"consumer_group"`
	SourceTopic     string `json:"source_topic"`
	SourcePartition int    `json:"source_partition"`
	SourceOffset    int64  `json:"source_offset"`
	ErrorCode       string `json:"error_code"`
	Attempts        int    `json:"attempts"`
}

func Run(ctx context.Context, store Store, r Request) (Result, error) {
	if err := r.Validate(); err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	m, err := store.Read(ctx, r)
	if err != nil {
		return Result{}, fmt.Errorf("read DLQ: %w", err)
	}
	result, message, err := prepare(r, m)
	if err != nil {
		return Result{}, err
	}
	if !r.Execute {
		return result, nil
	}
	result.Status = "not_published"
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := store.Publish(ctx, message); err != nil {
		result.Status = "publish_unconfirmed"
		return result, fmt.Errorf("replay acknowledgement not received; delivery may have occurred, inspect Inbox before retrying: %w", err)
	}
	result.Status = "published"
	return result, nil
}

func prepare(r Request, m kafka.Message) (Result, kafka.Message, error) {
	if m.Topic != r.DLQTopic || m.Partition != r.Partition || m.Offset != r.Offset {
		return Result{}, kafka.Message{}, fmt.Errorf("selected DLQ offset is unavailable; refusing a different record")
	}
	var d eventconsumer.DeadLetter
	if err := json.Unmarshal(m.Value, &d); err != nil {
		return Result{}, kafka.Message{}, fmt.Errorf("invalid DLQ JSON: %w", err)
	}
	if d.SchemaVersion != 1 || d.ID == "" || string(m.Key) != d.ID || d.Source.Partition < 0 || d.Source.Offset < 0 {
		return Result{}, kafka.Message{}, fmt.Errorf("invalid DLQ envelope or unsupported schema")
	}
	if d.Source.Topic != r.SourceTopic || d.ConsumerGroup != r.ConsumerGroup {
		return Result{}, kafka.Message{}, fmt.Errorf("DLQ source topic or consumer group does not match explicit selection")
	}
	if r.DLQID != "" && r.DLQID != d.ID {
		return Result{}, kafka.Message{}, fmt.Errorf("dlq-id confirmation does not match selected record")
	}
	// Decode only to check deduplication identity; never re-serialize the event.
	var event struct {
		EventID string `json:"event_id"`
	}
	if err := json.Unmarshal(d.Source.Value, &event); err != nil || strings.TrimSpace(event.EventID) == "" {
		return Result{}, kafka.Message{}, fmt.Errorf("source must be a JSON event with a nonempty event_id; repair needs a separate workflow")
	}
	headers := make([]kafka.Header, len(d.Source.Headers))
	for i, h := range d.Source.Headers {
		if string(h.Key) == "event_id" && string(h.Value) != event.EventID {
			return Result{}, kafka.Message{}, fmt.Errorf("event_id header conflicts with payload")
		}
		headers[i] = kafka.Header{Key: string(h.Key), Value: h.Value}
	}
	result := Result{Status: "dry_run", DLQID: d.ID, DLQTopic: m.Topic, DLQPartition: m.Partition, DLQOffset: m.Offset,
		EventID: event.EventID, ConsumerGroup: d.ConsumerGroup, SourceTopic: d.Source.Topic,
		SourcePartition: d.Source.Partition, SourceOffset: d.Source.Offset, ErrorCode: d.ErrorCode, Attempts: d.Attempts}
	// A new Kafka timestamp avoids immediately expiring an old replay. The
	// business occurred_at remains unchanged inside the byte-for-byte payload.
	return result, kafka.Message{Topic: d.Source.Topic, Key: d.Source.Key, Value: d.Source.Value, Headers: headers, Time: time.Now().UTC()}, nil
}
