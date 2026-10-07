package repo

import (
	"encoding/json"
	"time"
)

type PaymentStatus string

const (
	PaymentPending    PaymentStatus = "pending"
	PaymentProcessing PaymentStatus = "processing"
	PaymentCompleted  PaymentStatus = "completed"
	PaymentFailed     PaymentStatus = "failed"
)

type RefundStatus string

const (
	RefundPending    RefundStatus = "pending"
	RefundProcessing RefundStatus = "processing"
	RefundCompleted  RefundStatus = "completed"
	RefundFailed     RefundStatus = "failed"
)

type LedgerEntryType string

const (
	LedgerDebit  LedgerEntryType = "debit"
	LedgerCredit LedgerEntryType = "credit"
)

type Account struct {
	ID               string
	UserID           string
	Currency         string
	AvailableBalance int64
	Version          int64
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type Payment struct {
	ID          string
	ClientID    string
	AccountID   string
	Amount      int64
	Currency    string
	Status      PaymentStatus
	FailureCode *string
	Description *string
	Version     int64
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type LedgerEntry struct {
	ID           string
	AccountID    string
	PaymentID    *string
	RefundID     *string
	OperationID  string
	EntryType    LedgerEntryType
	Amount       int64
	BalanceAfter int64
	CreatedAt    time.Time
}

type Refund struct {
	ID          string
	PaymentID   string
	ClientID    string
	Amount      int64
	Status      RefundStatus
	FailureCode *string
	Version     int64
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type IdempotencyRecord struct {
	ClientID       string
	Key            string
	RequestHash    []byte
	ResourceType   string
	ResourceID     string
	ResponseStatus *int
	ResponseBody   json.RawMessage
	CreatedAt      time.Time
	ExpiresAt      time.Time
}

type OutboxEvent struct {
	ID            string
	AggregateType string
	AggregateID   string
	EventType     string
	Payload       json.RawMessage
	Headers       json.RawMessage
	Attempts      int
	LastError     *string
	CreatedAt     time.Time
	PublishedAt   *time.Time
}

type TransactionCursor struct {
	CreatedAt time.Time
	ID        string
}
