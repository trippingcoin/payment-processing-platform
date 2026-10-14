package paymentconsumer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
	"triple-p/cmd/payment-service/service"
	"triple-p/internal/fraud"
	"triple-p/internal/outbox"
)

type Payments interface {
	ApplyFraudDecision(context.Context, service.FraudDecisionCommand) error
}
type Handler struct{ Payments Payments }

func (h Handler) Handle(ctx context.Context, m kafka.Message) error {
	var e outbox.Envelope
	if err := json.Unmarshal(m.Value, &e); err != nil {
		return fmt.Errorf("decode fraud event: %w", err)
	}
	// This dedicated topic contains only decision events; fail closed on unknown types.
	if e.SchemaVersion != 1 || (e.EventType != "fraud.approved" && e.EventType != "fraud.declined") || e.OccurredAt.IsZero() {
		return fmt.Errorf("unsupported fraud envelope")
	}
	var d fraud.Decision
	if err := json.Unmarshal(e.Payload, &d); err != nil {
		return fmt.Errorf("decode fraud decision: %w", err)
	}
	if e.AggregateID != d.PaymentID || string(m.Key) != d.PaymentID || e.EventType != "fraud."+d.Status || d.DecidedAt.IsZero() {
		return fmt.Errorf("inconsistent fraud decision envelope")
	}
	return h.Payments.ApplyFraudDecision(ctx, service.FraudDecisionCommand{
		EventID: e.EventID, SourceEventID: d.SourceEventID, PaymentID: d.PaymentID, ClientID: d.ClientID,
		AccountID: d.AccountID, Amount: d.Amount, Currency: d.Currency, Status: d.Status, Reason: d.Reason, CorrelationID: e.CorrelationID,
	})
}
