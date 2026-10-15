package outbox

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
	"triple-p/cmd/payment-service/repo"
)

type Repository interface {
	LockNextOutboxEvent(context.Context) (repo.OutboxEvent, error)
	MarkOutboxPublished(context.Context, string, time.Time) error
	ScheduleOutboxRetry(context.Context, string, string, time.Time) error
}

type Transact func(context.Context, func(Repository) error) error
type Publisher interface {
	// Publish returns nil only after the broker acknowledges the message.
	Publish(context.Context, repo.OutboxEvent) error
}

type Config struct {
	PollInterval   time.Duration `env:"OUTBOX_POLL_INTERVAL" envDefault:"1s"`
	PublishTimeout time.Duration `env:"OUTBOX_PUBLISH_TIMEOUT" envDefault:"5s"`
	RetryBase      time.Duration `env:"OUTBOX_RETRY_BASE" envDefault:"1s"`
	RetryMax       time.Duration `env:"OUTBOX_RETRY_MAX" envDefault:"1m"`
}

type Worker struct {
	tx        Transact
	publisher Publisher
	cfg       Config
	observer  Observer
}

type Observer interface {
	OutboxPublish(error, time.Duration)
	OutboxTransaction(string)
}

func New(tx Transact, publisher Publisher, cfg Config, observers ...Observer) (*Worker, error) {
	if tx == nil || publisher == nil || cfg.PollInterval <= 0 || cfg.PublishTimeout <= 0 || cfg.RetryBase <= 0 || cfg.RetryMax < cfg.RetryBase {
		return nil, errors.New("invalid outbox configuration")
	}
	w := &Worker{tx: tx, publisher: publisher, cfg: cfg}
	if len(observers) > 0 {
		w.observer = observers[0]
	}
	return w, nil
}

func PostgresTransactions(store *repo.Store) Transact {
	return func(ctx context.Context, fn func(Repository) error) error {
		return store.WithTx(ctx, pgx.TxOptions{}, func(q *repo.Queries) error { return fn(q) })
	}
}

// Once processes one event. A failed publish commits retry metadata; a DB error
// or cancellation rolls back. A crash between broker ACK and DB commit may
// redeliver the same event_id (at-least-once delivery).
func (w *Worker) Once(ctx context.Context) (bool, error) {
	var found bool
	var publishErr error
	// Reserve extra time for retry metadata after the publication deadline.
	txCtx, cancel := context.WithTimeout(ctx, w.cfg.PublishTimeout+5*time.Second)
	defer cancel()
	err := w.tx(txCtx, func(q Repository) error {
		event, err := q.LockNextOutboxEvent(txCtx)
		if errors.Is(err, repo.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		publishCtx, stop := context.WithTimeout(txCtx, w.cfg.PublishTimeout)
		started := time.Now()
		publishErr = w.publisher.Publish(publishCtx, event)
		stop()
		if w.observer != nil {
			w.observer.OutboxPublish(publishErr, time.Since(started))
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if publishErr != nil {
			return q.ScheduleOutboxRetry(txCtx, event.ID, publishErr.Error(), time.Now().UTC().Add(w.retryDelay(event.Attempts)))
		}
		return q.MarkOutboxPublished(txCtx, event.ID, time.Now().UTC())
	})
	if w.observer != nil {
		switch {
		case err != nil:
			w.observer.OutboxTransaction("error")
		case found && publishErr != nil:
			w.observer.OutboxTransaction("retry_scheduled")
		case found:
			w.observer.OutboxTransaction("published")
		}
	}
	if err != nil {
		return found, fmt.Errorf("outbox transaction: %w", err)
	}
	if publishErr != nil {
		return found, fmt.Errorf("outbox publication scheduled for retry: %w", publishErr)
	}
	return found, nil
}

func (w *Worker) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		found, err := w.Once(ctx)
		if ctx.Err() != nil {
			break
		}
		if err != nil {
			log.Error().Err(err).Msg("outbox iteration failed")
		}
		if found && err == nil {
			continue
		}
		timer := time.NewTimer(w.cfg.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	return ctx.Err()
}

func (w *Worker) retryDelay(attempts int) time.Duration {
	delay := w.cfg.RetryBase
	for i := 0; i < attempts && delay < w.cfg.RetryMax; i++ {
		if delay > w.cfg.RetryMax/2 {
			delay = w.cfg.RetryMax
			break
		}
		delay *= 2
	}
	// Equal jitter in [delay/2, delay], never exceeding the configured cap.
	return delay/2 + time.Duration(rand.Int64N(int64(delay-delay/2)+1))
}
