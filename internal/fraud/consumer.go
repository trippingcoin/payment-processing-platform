package fraud

import (
	"context"
	"time"
	"triple-p/internal/eventconsumer"
)

// Consume retains the Fraud entry point while sharing offset handling.
func Consume(ctx context.Context, reader eventconsumer.Reader, handler eventconsumer.Handler, timeout time.Duration) error {
	return eventconsumer.Consume(ctx, reader, handler, timeout)
}
