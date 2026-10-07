package repo

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func (q *Queries) CreatePayment(ctx context.Context, payment Payment) (Payment, error) {
	const statement = `
		INSERT INTO payments (
			id, client_id, account_id, amount, currency, status, description
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, client_id, account_id, amount, currency, status,
		          failure_code, description, version, created_at, updated_at`

	created, err := scanPayment(q.db.QueryRow(ctx, statement,
		payment.ID,
		payment.ClientID,
		payment.AccountID,
		payment.Amount,
		payment.Currency,
		payment.Status,
		payment.Description,
	))
	if err != nil {
		return Payment{}, fmt.Errorf("create payment: %w", mapPostgresError(err))
	}
	return created, nil
}

func (q *Queries) GetPayment(ctx context.Context, id string) (Payment, error) {
	const statement = `
		SELECT id, client_id, account_id, amount, currency, status,
		       failure_code, description, version, created_at, updated_at
		FROM payments
		WHERE id = $1`

	payment, err := scanPayment(q.db.QueryRow(ctx, statement, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Payment{}, ErrNotFound
	}
	if err != nil {
		return Payment{}, fmt.Errorf("get payment: %w", err)
	}
	return payment, nil
}

// GetPaymentForUpdate serializes operations such as partial refunds. It must be
// called through Queries received from Store.WithTx, not through the pool-backed
// Store directly, otherwise the row lock is released immediately.
func (q *Queries) GetPaymentForUpdate(ctx context.Context, id string) (Payment, error) {
	const statement = `
		SELECT id, client_id, account_id, amount, currency, status,
		       failure_code, description, version, created_at, updated_at
		FROM payments
		WHERE id = $1
		FOR UPDATE`

	payment, err := scanPayment(q.db.QueryRow(ctx, statement, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Payment{}, ErrNotFound
	}
	if err != nil {
		return Payment{}, fmt.Errorf("lock payment: %w", err)
	}
	return payment, nil
}

func (q *Queries) UpdatePaymentStatus(
	ctx context.Context,
	id string,
	expectedStatus PaymentStatus,
	newStatus PaymentStatus,
	expectedVersion int64,
	failureCode *string,
) (Payment, error) {
	const statement = `
		UPDATE payments
		SET status = $3,
		    failure_code = $5,
		    version = version + 1,
		    updated_at = now()
		WHERE id = $1
		  AND status = $2
		  AND version = $4
		RETURNING id, client_id, account_id, amount, currency, status,
		          failure_code, description, version, created_at, updated_at`

	payment, err := scanPayment(q.db.QueryRow(ctx, statement,
		id, expectedStatus, newStatus, expectedVersion, failureCode,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		current, getErr := q.GetPayment(ctx, id)
		if getErr != nil {
			return Payment{}, getErr
		}
		if current.Version != expectedVersion {
			return Payment{}, ErrOptimisticLock
		}
		return Payment{}, ErrInvalidState
	}
	if err != nil {
		return Payment{}, fmt.Errorf("update payment status: %w", mapPostgresError(err))
	}
	return payment, nil
}

func scanPayment(row pgx.Row) (Payment, error) {
	var payment Payment
	err := row.Scan(
		&payment.ID,
		&payment.ClientID,
		&payment.AccountID,
		&payment.Amount,
		&payment.Currency,
		&payment.Status,
		&payment.FailureCode,
		&payment.Description,
		&payment.Version,
		&payment.CreatedAt,
		&payment.UpdatedAt,
	)
	return payment, err
}
