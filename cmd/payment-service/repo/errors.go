package repo

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrNotFound            = errors.New("repository: entity not found")
	ErrAlreadyExists       = errors.New("repository: entity already exists")
	ErrInsufficientFunds   = errors.New("repository: insufficient funds")
	ErrOptimisticLock      = errors.New("repository: optimistic lock conflict")
	ErrCurrencyMismatch    = errors.New("repository: currency mismatch")
	ErrInvalidState        = errors.New("repository: invalid state transition")
	ErrIdempotencyConflict = errors.New("repository: idempotency key payload conflict")
)

func mapPostgresError(err error) error {
	if err == nil {
		return nil
	}

	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err
	}

	switch pgErr.Code {
	case "23505": // unique_violation
		return errors.Join(ErrAlreadyExists, err)
	case "23514", "23503": // check_violation, foreign_key_violation
		return errors.Join(ErrInvalidState, err)
	default:
		return err
	}
}
