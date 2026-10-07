package repo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func (q *Queries) ReserveIdempotencyKey(ctx context.Context, record IdempotencyRecord) (bool, error) {
	const statement = `
		INSERT INTO idempotency_keys (
			client_id, idempotency_key, request_hash, resource_type,
			resource_id, expires_at
		) VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (client_id, idempotency_key) DO NOTHING`

	tag, err := q.db.Exec(ctx, statement,
		record.ClientID,
		record.Key,
		record.RequestHash,
		record.ResourceType,
		record.ResourceID,
		record.ExpiresAt,
	)
	if err != nil {
		return false, fmt.Errorf("reserve idempotency key: %w", mapPostgresError(err))
	}
	return tag.RowsAffected() == 1, nil
}

func (q *Queries) GetIdempotencyKey(ctx context.Context, clientID, key string) (IdempotencyRecord, error) {
	const statement = `
		SELECT client_id, idempotency_key, request_hash, resource_type,
		       resource_id, response_status, response_body, created_at, expires_at
		FROM idempotency_keys
		WHERE client_id = $1
		  AND idempotency_key = $2
		  AND expires_at > now()`

	var record IdempotencyRecord
	err := q.db.QueryRow(ctx, statement, clientID, key).Scan(
		&record.ClientID,
		&record.Key,
		&record.RequestHash,
		&record.ResourceType,
		&record.ResourceID,
		&record.ResponseStatus,
		&record.ResponseBody,
		&record.CreatedAt,
		&record.ExpiresAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return IdempotencyRecord{}, ErrNotFound
	}
	if err != nil {
		return IdempotencyRecord{}, fmt.Errorf("get idempotency key: %w", err)
	}
	return record, nil
}

func (q *Queries) SaveIdempotencyResponse(
	ctx context.Context,
	clientID string,
	key string,
	status int,
	body json.RawMessage,
) error {
	const statement = `
		UPDATE idempotency_keys
		SET response_status = $3, response_body = $4
		WHERE client_id = $1 AND idempotency_key = $2`

	tag, err := q.db.Exec(ctx, statement, clientID, key, status, body)
	if err != nil {
		return fmt.Errorf("save idempotency response: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (q *Queries) DeleteExpiredIdempotencyKeys(ctx context.Context, before time.Time, limit int) (int64, error) {
	if limit <= 0 || limit > 10_000 {
		limit = 1_000
	}

	const statement = `
		WITH expired AS (
			SELECT client_id, idempotency_key
			FROM idempotency_keys
			WHERE expires_at <= $1
			ORDER BY expires_at
			LIMIT $2
		)
		DELETE FROM idempotency_keys i
		USING expired e
		WHERE i.client_id = e.client_id
		  AND i.idempotency_key = e.idempotency_key`

	tag, err := q.db.Exec(ctx, statement, before, limit)
	if err != nil {
		return 0, fmt.Errorf("delete expired idempotency keys: %w", err)
	}
	return tag.RowsAffected(), nil
}
