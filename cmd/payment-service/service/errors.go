package service

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidArgument     = errors.New("payment service: invalid argument")
	ErrPaymentNotFound     = errors.New("payment service: payment not found")
	ErrAccountNotFound     = errors.New("payment service: account not found")
	ErrCurrencyMismatch    = errors.New("payment service: account currency mismatch")
	ErrIdempotencyConflict = errors.New("payment service: idempotency key reused with different request")
	ErrProcessingConflict  = errors.New("payment service: processing conflict; retry later")
)

type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Reason)
}

func (e *ValidationError) Unwrap() error {
	return ErrInvalidArgument
}
