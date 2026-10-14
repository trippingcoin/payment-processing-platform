package eventconsumer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"triple-p/internal/eventerror"
)

// Byte slices are base64 in JSON: malformed JSON, binary data, duplicate headers
// and non-UTF8 header keys must survive quarantine without transformation.
type DeadLetterHeader struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}
type DeadLetterSource struct {
	Topic     string             `json:"topic"`
	Partition int                `json:"partition"`
	Offset    int64              `json:"offset"`
	Time      time.Time          `json:"time"`
	Key       []byte             `json:"key"`
	Value     []byte             `json:"value"`
	Headers   []DeadLetterHeader `json:"headers"`
}
type DeadLetter struct {
	ID             string           `json:"dlq_id"`
	SchemaVersion  int              `json:"schema_version"`
	ConsumerGroup  string           `json:"consumer_group"`
	FailedAt       time.Time        `json:"failed_at"`
	ErrorKind      eventerror.Kind  `json:"error_kind"`
	ErrorCode      string           `json:"error_code"`
	Reason         string           `json:"reason"`
	Attempts       int              `json:"attempts"`
	RetryExhausted bool             `json:"retry_exhausted"`
	Source         DeadLetterSource `json:"source"`
}

type DeadLetterPublisher interface {
	PublishDeadLetter(context.Context, DeadLetter) error
}
type DeadLetterSink struct {
	Publisher     DeadLetterPublisher
	ConsumerGroup string
	Timeout       time.Duration
}

func (s DeadLetterSink) validate() error {
	if s.Publisher == nil || strings.TrimSpace(s.ConsumerGroup) == "" || s.Timeout <= 0 {
		return fmt.Errorf("DLQ requires a publisher, consumer group and positive timeout")
	}
	return nil
}

func deadLetterEligible(err error) bool {
	var f *Failure
	if !errors.As(err, &f) || f.Stage != "handle" {
		return false
	}
	kind := eventerror.Classify(err).Kind
	return kind == eventerror.Permanent || (kind == eventerror.Transient && f.RetryExhausted)
}

func newDeadLetter(group string, m kafka.Message, err error) DeadLetter {
	var f *Failure
	_ = errors.As(err, &f) // Called only after deadLetterEligible.
	identity, _ := json.Marshal(struct {
		Group, Topic string
		Partition    int
		Offset       int64
	}{group, m.Topic, m.Partition, m.Offset})
	hash := sha256.Sum256(identity)
	classification := eventerror.Classify(err)
	reason := f.Cause.Error()
	if len(reason) > 4096 {
		reason = reason[:4096]
	}
	source := DeadLetterSource{Topic: m.Topic, Partition: m.Partition, Offset: m.Offset, Time: m.Time,
		Key: cloneBytes(m.Key), Value: cloneBytes(m.Value), Headers: make([]DeadLetterHeader, len(m.Headers))}
	for i, h := range m.Headers {
		source.Headers[i] = DeadLetterHeader{Key: cloneBytes([]byte(h.Key)), Value: cloneBytes(h.Value)}
	}
	return DeadLetter{ID: hex.EncodeToString(hash[:]), SchemaVersion: 1, ConsumerGroup: group, FailedAt: time.Now().UTC(),
		ErrorKind: classification.Kind, ErrorCode: classification.Code, Reason: reason, Attempts: f.Attempts, RetryExhausted: f.RetryExhausted, Source: source}
}
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
