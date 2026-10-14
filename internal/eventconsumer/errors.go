package eventconsumer

import (
	"errors"
	"fmt"
	"triple-p/internal/eventerror"
)

// Failure keeps transport stage distinct from handler failures. A failed offset
// commit is not a failed business operation and must not be sent to a DLQ.
type Failure struct {
	Stage          string
	Topic          string
	Partition      int
	Offset         int64
	Cause          error
	Attempts       int
	RetryExhausted bool
}

func (e *Failure) Error() string {
	return fmt.Sprintf("%s event topic %s partition %d offset %d: %v", e.Stage, e.Topic, e.Partition, e.Offset, e.Cause)
}
func (e *Failure) Unwrap() error { return e.Cause }

// ErrorFields contains only operational metadata, never event keys or payloads.
func ErrorFields(err error) map[string]any {
	c := eventerror.Classify(err)
	fields := map[string]any{"error_kind": string(c.Kind), "error_code": c.Code}
	var f *Failure
	if errors.As(err, &f) {
		fields["stage"] = f.Stage
		fields["attempts"] = f.Attempts
		fields["retry_exhausted"] = f.RetryExhausted
		if f.Stage != "fetch" {
			fields["topic"] = f.Topic
			fields["partition"] = f.Partition
			fields["offset"] = f.Offset
		}
	}
	return fields
}
