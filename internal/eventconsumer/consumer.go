package eventconsumer

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
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
// Transient failures retry in place; exhausted or non-transient failures stop
// the process. Restart/redelivery is safe via the Inbox.
// CommitInterval must be zero on the underlying Kafka reader (synchronous ACK).
func Consume(ctx context.Context, reader Reader, handler Handler, timeout time.Duration) error {
	return ConsumeWithRetry(ctx, reader, handler, timeout, DefaultRetryConfig())
}

func ConsumeWithRetry(ctx context.Context, reader Reader, handler Handler, timeout time.Duration, retry RetryConfig) error {
	if timeout <= 0 {
		return fmt.Errorf("consumer timeout must be positive")
	}
	if err := retry.Validate(); err != nil {
		return err
	}
	return consume(ctx, reader, handler, timeout, newRetryRunner(retry))
}

func consume(ctx context.Context, reader Reader, handler Handler, timeout time.Duration, retry retryRunner) error {
	return consumeWithDLQ(ctx, reader, handler, timeout, retry, nil)
}

// ConsumeWithDLQ quarantines permanent or exhausted transient handler failures.
// No source commit is attempted until the DLQ publisher acknowledges the write.
func ConsumeWithDLQ(ctx context.Context, reader Reader, handler Handler, timeout time.Duration, retry RetryConfig, sink DeadLetterSink) error {
	if timeout <= 0 {
		return fmt.Errorf("consumer timeout must be positive")
	}
	if err := retry.Validate(); err != nil {
		return err
	}
	if err := sink.validate(); err != nil {
		return err
	}
	return consumeWithDLQ(ctx, reader, handler, timeout, newRetryRunner(retry), &sink)
}

func consumeWithDLQ(ctx context.Context, reader Reader, handler Handler, timeout time.Duration, retry retryRunner, sink *DeadLetterSink) error {
	for {
		var message kafka.Message
		// Fetch can wait indefinitely on an idle topic. Only actual errors consume
		// its retry budget; the processing timeout starts after receiving a record.
		err := retry.run(ctx, "fetch", kafka.Message{}, func(ctx context.Context) error {
			var err error
			message, err = reader.FetchMessage(ctx)
			return err
		})
		if err != nil {
			return err
		}
		err = retry.run(ctx, "handle", message, func(ctx context.Context) error {
			processCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			return handler.Handle(processCtx, message)
		})
		if err != nil {
			if sink == nil || !deadLetterEligible(err) {
				return err
			}
			handleErr := err
			letter := newDeadLetter(sink.ConsumerGroup, message, handleErr)
			err = retry.run(ctx, "dlq", message, func(ctx context.Context) error {
				publishCtx, cancel := context.WithTimeout(ctx, sink.Timeout)
				defer cancel()
				return sink.Publisher.PublishDeadLetter(publishCtx, letter)
			})
			if err != nil {
				return err
			}
			log.Warn().Fields(ErrorFields(handleErr)).Str("dlq_id", letter.ID).Msg("event acknowledged by DLQ; committing source offset")
		}
		err = retry.run(ctx, "commit", message, func(ctx context.Context) error {
			commitCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			return reader.CommitMessages(commitCtx, message)
		})
		if err != nil {
			return err
		}
	}
}
