package eventconsumer

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type Reader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
}
type Handler interface {
	Handle(context.Context, kafka.Message) error
}

// Consume is sequential: never commit a later offset after a failed record.
// On any failure the process stops; restart/redelivery is safe via the Inbox.
// CommitInterval must be zero on the underlying Kafka reader (synchronous ACK).
func Consume(ctx context.Context, reader Reader, handler Handler, timeout time.Duration) error {
	if timeout <= 0 {
		return fmt.Errorf("consumer timeout must be positive")
	}
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			return &Failure{Stage: "fetch", Cause: err}
		}
		processCtx, cancel := context.WithTimeout(ctx, timeout)
		err = handler.Handle(processCtx, message)
		cancel()
		if err != nil {
			return &Failure{Stage: "handle", Topic: message.Topic, Partition: message.Partition, Offset: message.Offset, Cause: err}
		}
		commitCtx, cancel := context.WithTimeout(ctx, timeout)
		err = reader.CommitMessages(commitCtx, message)
		cancel()
		if err != nil {
			return &Failure{Stage: "commit", Topic: message.Topic, Partition: message.Partition, Offset: message.Offset, Cause: err}
		}
	}
}
