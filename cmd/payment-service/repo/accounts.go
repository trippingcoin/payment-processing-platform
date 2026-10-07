package repo

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func (q *Queries) CreateAccount(ctx context.Context, account Account) (Account, error) {
	const statement = `
		INSERT INTO accounts (id, user_id, currency, available_balance)
		VALUES ($1, $2, $3, $4)
		RETURNING id, user_id, currency, available_balance, version, created_at, updated_at`

	result, err := scanAccount(q.db.QueryRow(ctx, statement,
		account.ID, account.UserID, account.Currency, account.AvailableBalance,
	))
	if err != nil {
		return Account{}, fmt.Errorf("create account: %w", mapPostgresError(err))
	}
	return result, nil
}

func (q *Queries) GetAccount(ctx context.Context, id string) (Account, error) {
	const statement = `
		SELECT id, user_id, currency, available_balance, version, created_at, updated_at
		FROM accounts
		WHERE id = $1`

	account, err := scanAccount(q.db.QueryRow(ctx, statement, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Account{}, ErrNotFound
	}
	if err != nil {
		return Account{}, fmt.Errorf("get account: %w", err)
	}
	return account, nil
}

func (q *Queries) DebitAccount(
	ctx context.Context,
	id string,
	currency string,
	amount int64,
	expectedVersion int64,
) (Account, error) {
	if amount <= 0 {
		return Account{}, ErrInvalidState
	}

	const statement = `
		UPDATE accounts
		SET available_balance = available_balance - $3,
		    version = version + 1,
		    updated_at = now()
		WHERE id = $1
		  AND currency = $2
		  AND available_balance >= $3
		  AND version = $4
		RETURNING id, user_id, currency, available_balance, version, created_at, updated_at`

	account, err := scanAccount(q.db.QueryRow(ctx, statement, id, currency, amount, expectedVersion))
	if errors.Is(err, pgx.ErrNoRows) {
		return Account{}, q.classifyAccountWriteFailure(ctx, id, currency, amount, expectedVersion, true)
	}
	if err != nil {
		return Account{}, fmt.Errorf("debit account: %w", mapPostgresError(err))
	}
	return account, nil
}

func (q *Queries) CreditAccount(
	ctx context.Context,
	id string,
	currency string,
	amount int64,
	expectedVersion int64,
) (Account, error) {
	if amount <= 0 {
		return Account{}, ErrInvalidState
	}

	const statement = `
		UPDATE accounts
		SET available_balance = available_balance + $3,
		    version = version + 1,
		    updated_at = now()
		WHERE id = $1
		  AND currency = $2
		  AND version = $4
		RETURNING id, user_id, currency, available_balance, version, created_at, updated_at`

	account, err := scanAccount(q.db.QueryRow(ctx, statement, id, currency, amount, expectedVersion))
	if errors.Is(err, pgx.ErrNoRows) {
		return Account{}, q.classifyAccountWriteFailure(ctx, id, currency, amount, expectedVersion, false)
	}
	if err != nil {
		return Account{}, fmt.Errorf("credit account: %w", mapPostgresError(err))
	}
	return account, nil
}

func (q *Queries) classifyAccountWriteFailure(
	ctx context.Context,
	id string,
	currency string,
	amount int64,
	expectedVersion int64,
	isDebit bool,
) error {
	account, err := q.GetAccount(ctx, id)
	if err != nil {
		return err
	}
	if account.Currency != currency {
		return ErrCurrencyMismatch
	}
	if isDebit && account.AvailableBalance < amount {
		return ErrInsufficientFunds
	}
	if account.Version != expectedVersion {
		return ErrOptimisticLock
	}
	return ErrInvalidState
}

func scanAccount(row pgx.Row) (Account, error) {
	var account Account
	err := row.Scan(
		&account.ID,
		&account.UserID,
		&account.Currency,
		&account.AvailableBalance,
		&account.Version,
		&account.CreatedAt,
		&account.UpdatedAt,
	)
	return account, err
}
