// Package fraud implements deterministic demo rules, not a production risk model.
package fraud

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"triple-p/internal/eventerror"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"triple-p/internal/outbox"
)

type Rules struct {
	MaxAmount int64  `env:"FRAUD_MAX_AMOUNT_MINOR" envDefault:"1000000"`
	Currency  string `env:"FRAUD_CURRENCY" envDefault:"KZT"`
}

type Payment struct {
	PaymentID string `json:"payment_id"`
	ClientID  string `json:"client_id"`
	AccountID string `json:"account_id"`
	Amount    int64  `json:"amount_minor"`
	Currency  string `json:"currency"`
}

type Decision struct {
	Payment
	SourceEventID string    `json:"source_event_id"`
	Status        string    `json:"status"`
	Reason        string    `json:"reason"`
	RuleVersion   string    `json:"rule_version"`
	MaxAmount     int64     `json:"max_amount_minor"`
	RuleCurrency  string    `json:"rule_currency"`
	DecidedAt     time.Time `json:"decided_at"`
}

type Store interface {
	Record(context.Context, Decision, map[string]string) error
}

type Service struct {
	store Store
	rules Rules
}

func New(store Store, rules Rules) (*Service, error) {
	if store == nil || rules.MaxAmount <= 0 || !validCurrency(rules.Currency) {
		return nil, fmt.Errorf("fraud requires a store, positive amount limit and uppercase three-letter currency")
	}
	return &Service{store: store, rules: rules}, nil
}

func validCurrency(s string) bool {
	if len(s) != 3 {
		return false
	}
	for _, c := range s {
		if c < 'A' || c > 'Z' {
			return false
		}
	}
	return true
}

func (s *Service) Handle(ctx context.Context, message kafka.Message) error {
	var event outbox.Envelope
	if err := json.Unmarshal(message.Value, &event); err != nil {
		return eventerror.Mark(eventerror.Permanent, "invalid_json", fmt.Errorf("invalid event JSON: %w", err))
	}
	if event.EventType == "" {
		return eventerror.Mark(eventerror.Permanent, "invalid_envelope", fmt.Errorf("missing event_type"))
	}
	// Other lifecycle events share the payment topic and require no fraud check.
	if event.EventType != "payment.created" {
		return nil
	}
	if event.SchemaVersion != 1 || event.OccurredAt.IsZero() || len(event.CorrelationID) > 128 {
		return eventerror.Mark(eventerror.Permanent, "invalid_envelope", fmt.Errorf("invalid payment.created envelope"))
	}
	var payment Payment
	if err := json.Unmarshal(event.Payload, &payment); err != nil {
		return eventerror.Mark(eventerror.Permanent, "invalid_payload", fmt.Errorf("invalid payment payload: %w", err))
	}
	for _, id := range []string{event.EventID, payment.PaymentID, payment.ClientID, payment.AccountID} {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil || parsed.String() != id {
			return eventerror.Mark(eventerror.Permanent, "invalid_identifier", fmt.Errorf("event identifiers must be canonical nonzero UUIDs"))
		}
	}
	if event.AggregateID != payment.PaymentID || string(message.Key) != payment.PaymentID || payment.Amount <= 0 || !validCurrency(payment.Currency) {
		return eventerror.Mark(eventerror.Permanent, "invalid_payment", fmt.Errorf("invalid payment.created fields or partition key"))
	}
	decision := Decision{Payment: payment, SourceEventID: event.EventID, Status: "approved", Reason: "within_limit",
		RuleVersion: "amount-limit-v1", MaxAmount: s.rules.MaxAmount, RuleCurrency: s.rules.Currency, DecidedAt: time.Now().UTC()}
	if payment.Currency != s.rules.Currency {
		decision.Status, decision.Reason = "declined", "unsupported_currency"
	} else if payment.Amount > s.rules.MaxAmount {
		decision.Status, decision.Reason = "declined", "amount_limit_exceeded"
	}
	correlation := event.CorrelationID
	if correlation == "" {
		correlation = payment.PaymentID
	}
	headers := map[string]string{"correlation_id": correlation, "causation_id": event.EventID}
	for _, h := range message.Headers {
		if (h.Key == "traceparent" || h.Key == "tracestate") && len(h.Value) <= 512 {
			headers[h.Key] = string(h.Value)
		}
	}
	return s.store.Record(ctx, decision, headers)
}
