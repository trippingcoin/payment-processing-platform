package repo

import (
	"context"
	"encoding/json"
	"time"
)

type AccountRepository interface {
	CreateAccount(context.Context, Account) (Account, error)
	GetAccount(context.Context, string) (Account, error)
	DebitAccount(context.Context, string, string, int64, int64) (Account, error)
	CreditAccount(context.Context, string, string, int64, int64) (Account, error)
}

type PaymentRepository interface {
	CreatePayment(context.Context, Payment) (Payment, error)
	GetPayment(context.Context, string) (Payment, error)
	GetPaymentForUpdate(context.Context, string) (Payment, error)
	UpdatePaymentStatus(context.Context, string, PaymentStatus, PaymentStatus, int64, *string) (Payment, error)
}

type LedgerRepository interface {
	AppendLedgerEntry(context.Context, LedgerEntry) (LedgerEntry, error)
	ListTransactions(context.Context, string, *TransactionCursor, int) ([]LedgerEntry, *TransactionCursor, error)
}

type RefundRepository interface {
	CreateRefund(context.Context, Refund) (Refund, error)
	GetRefund(context.Context, string) (Refund, error)
	UpdateRefundStatus(context.Context, string, RefundStatus, RefundStatus, int64, *string) (Refund, error)
	CompletedRefundAmount(context.Context, string) (int64, error)
}

type IdempotencyRepository interface {
	ReserveIdempotencyKey(context.Context, IdempotencyRecord) (bool, error)
	GetIdempotencyKey(context.Context, string, string) (IdempotencyRecord, error)
	SaveIdempotencyResponse(context.Context, string, string, int, json.RawMessage) error
	DeleteExpiredIdempotencyKeys(context.Context, time.Time, int) (int64, error)
}

type OutboxRepository interface {
	AppendOutboxEvent(context.Context, OutboxEvent) (OutboxEvent, error)
	LockOutboxBatch(context.Context, int) ([]OutboxEvent, error)
	MarkOutboxPublished(context.Context, string, time.Time) error
	MarkOutboxFailed(context.Context, string, string) error
}

type InboxRepository interface {
	TryBeginInboxEvent(context.Context, string, string, time.Time) (bool, error)
}

var (
	_ AccountRepository     = (*Queries)(nil)
	_ PaymentRepository     = (*Queries)(nil)
	_ LedgerRepository      = (*Queries)(nil)
	_ RefundRepository      = (*Queries)(nil)
	_ IdempotencyRepository = (*Queries)(nil)
	_ OutboxRepository      = (*Queries)(nil)
	_ InboxRepository       = (*Queries)(nil)
)
