package repo

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const outboxColumns = `
	id, aggregate_type, aggregate_id, event_type, payload, headers,
	attempts, last_error, created_at, published_at`

func (q *Queries) AppendOutboxEvent(ctx context.Context, event OutboxEvent) (OutboxEvent, error) {
	const statement = `
		INSERT INTO outbox_events (
			id, aggregate_type, aggregate_id, event_type, payload, headers
		) VALUES ($1, $2, $3, $4, $5, COALESCE($6, '{}'::jsonb))
		RETURNING ` + outboxColumns

	created, err := scanOutboxEvent(q.db.QueryRow(ctx, statement,
		event.ID,
		event.AggregateType,
		event.AggregateID,
		event.EventType,
		event.Payload,
		event.Headers,
	))
	if err != nil {
		return OutboxEvent{}, fmt.Errorf("append outbox event: %w", mapPostgresError(err))
	}
	return created, nil
}

// LockOutboxBatch must be called inside Store.WithTx. Locks live until that
// transaction commits or rolls back, allowing multiple relay workers to run.
func (q *Queries) LockOutboxBatch(ctx context.Context, limit int) ([]OutboxEvent, error) {
	if limit <= 0 || limit > 1_000 {
		limit = 100
	}

	const statement = `
		SELECT ` + outboxColumns + `
		FROM outbox_events
		WHERE published_at IS NULL
		ORDER BY created_at, id
		FOR UPDATE SKIP LOCKED
		LIMIT $1`

	rows, err := q.db.Query(ctx, statement, limit)
	if err != nil {
		return nil, fmt.Errorf("lock outbox batch: %w", err)
	}
	defer rows.Close()

	events := make([]OutboxEvent, 0, limit)
	for rows.Next() {
		event, scanErr := scanOutboxEvent(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan outbox event: %w", scanErr)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox events: %w", err)
	}
	return events, nil
}

func (q *Queries) MarkOutboxPublished(ctx context.Context, id string, publishedAt time.Time) error {
	const statement = `
		UPDATE outbox_events
		SET published_at = $2, last_error = NULL
		WHERE id = $1 AND published_at IS NULL`

	tag, err := q.db.Exec(ctx, statement, id, publishedAt)
	if err != nil {
		return fmt.Errorf("mark outbox published: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (q *Queries) MarkOutboxFailed(ctx context.Context, id, reason string) error {
	const statement = `
		UPDATE outbox_events
		SET attempts = attempts + 1, last_error = $2
		WHERE id = $1 AND published_at IS NULL`

	tag, err := q.db.Exec(ctx, statement, id, reason)
	if err != nil {
		return fmt.Errorf("mark outbox failed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func scanOutboxEvent(row pgx.Row) (OutboxEvent, error) {
	var event OutboxEvent
	err := row.Scan(
		&event.ID,
		&event.AggregateType,
		&event.AggregateID,
		&event.EventType,
		&event.Payload,
		&event.Headers,
		&event.Attempts,
		&event.LastError,
		&event.CreatedAt,
		&event.PublishedAt,
	)
	return event, err
}
