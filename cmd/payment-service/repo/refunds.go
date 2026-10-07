package repo

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func (q *Queries) CreateRefund(ctx context.Context, refund Refund) (Refund, error) {
	const statement = `
		INSERT INTO refunds (id, payment_id, client_id, amount, status)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, payment_id, client_id, amount, status,
		          failure_code, version, created_at, updated_at`

	created, err := scanRefund(q.db.QueryRow(ctx, statement,
		refund.ID, refund.PaymentID, refund.ClientID, refund.Amount, refund.Status,
	))
	if err != nil {
		return Refund{}, fmt.Errorf("create refund: %w", mapPostgresError(err))
	}
	return created, nil
}

func (q *Queries) GetRefund(ctx context.Context, id string) (Refund, error) {
	const statement = `
		SELECT id, payment_id, client_id, amount, status,
		       failure_code, version, created_at, updated_at
		FROM refunds
		WHERE id = $1`

	refund, err := scanRefund(q.db.QueryRow(ctx, statement, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Refund{}, ErrNotFound
	}
	if err != nil {
		return Refund{}, fmt.Errorf("get refund: %w", err)
	}
	return refund, nil
}

func (q *Queries) UpdateRefundStatus(
	ctx context.Context,
	id string,
	expectedStatus RefundStatus,
	newStatus RefundStatus,
	expectedVersion int64,
	failureCode *string,
) (Refund, error) {
	const statement = `
		UPDATE refunds
		SET status = $3,
		    failure_code = $5,
		    version = version + 1,
		    updated_at = now()
		WHERE id = $1
		  AND status = $2
		  AND version = $4
		RETURNING id, payment_id, client_id, amount, status,
		          failure_code, version, created_at, updated_at`

	refund, err := scanRefund(q.db.QueryRow(ctx, statement,
		id, expectedStatus, newStatus, expectedVersion, failureCode,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		current, getErr := q.GetRefund(ctx, id)
		if getErr != nil {
			return Refund{}, getErr
		}
		if current.Version != expectedVersion {
			return Refund{}, ErrOptimisticLock
		}
		return Refund{}, ErrInvalidState
	}
	if err != nil {
		return Refund{}, fmt.Errorf("update refund status: %w", mapPostgresError(err))
	}
	return refund, nil
}

func (q *Queries) CompletedRefundAmount(ctx context.Context, paymentID string) (int64, error) {
	const statement = `
		SELECT COALESCE(SUM(amount), 0)
		FROM refunds
		WHERE payment_id = $1 AND status = 'completed'`

	var amount int64
	if err := q.db.QueryRow(ctx, statement, paymentID).Scan(&amount); err != nil {
		return 0, fmt.Errorf("sum completed refunds: %w", err)
	}
	return amount, nil
}

func scanRefund(row pgx.Row) (Refund, error) {
	var refund Refund
	err := row.Scan(
		&refund.ID,
		&refund.PaymentID,
		&refund.ClientID,
		&refund.Amount,
		&refund.Status,
		&refund.FailureCode,
		&refund.Version,
		&refund.CreatedAt,
		&refund.UpdatedAt,
	)
	return refund, err
}
