package paymentconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/segmentio/kafka-go"
	"triple-p/cmd/payment-service/service"
	"triple-p/internal/eventerror"
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
		return eventerror.Mark(eventerror.Permanent, "invalid_json", fmt.Errorf("decode fraud event: %w", err))
	}
	// This dedicated topic contains only decision events; fail closed on unknown types.
	if e.SchemaVersion != 1 || (e.EventType != "fraud.approved" && e.EventType != "fraud.declined") || e.OccurredAt.IsZero() {
		return eventerror.Mark(eventerror.Permanent, "invalid_envelope", fmt.Errorf("unsupported fraud envelope"))
	}
	var d fraud.Decision
	if err := json.Unmarshal(e.Payload, &d); err != nil {
		return eventerror.Mark(eventerror.Permanent, "invalid_payload", fmt.Errorf("decode fraud decision: %w", err))
	}
	if e.AggregateID != d.PaymentID || string(m.Key) != d.PaymentID || e.EventType != "fraud."+d.Status || d.DecidedAt.IsZero() {
		return eventerror.Mark(eventerror.Permanent, "inconsistent_event", fmt.Errorf("inconsistent fraud decision envelope"))
	}
	err := h.Payments.ApplyFraudDecision(ctx, service.FraudDecisionCommand{
		EventID: e.EventID, SourceEventID: d.SourceEventID, PaymentID: d.PaymentID, ClientID: d.ClientID,
		AccountID: d.AccountID, Amount: d.Amount, Currency: d.Currency, Status: d.Status, Reason: d.Reason, CorrelationID: e.CorrelationID,
	})
	switch {
	case errors.Is(err, service.ErrInvalidFraudDecision):
		return eventerror.Mark(eventerror.Permanent, "invalid_decision", err)
	case errors.Is(err, service.ErrFraudDecisionMismatch):
		return eventerror.Mark(eventerror.Permanent, "decision_mismatch", err)
	case errors.Is(err, service.ErrUnknownFraudSource):
		return eventerror.Mark(eventerror.Permanent, "unknown_source", err)
	// payment.created is emitted after the payment commits, so this is not an
	// expected eventual-consistency delay in the current workflow.
	case errors.Is(err, service.ErrPaymentNotFound):
		return eventerror.Mark(eventerror.Permanent, "payment_not_found", err)
	case errors.Is(err, service.ErrProcessingConflict):
		return eventerror.Mark(eventerror.Transient, "processing_conflict", err)
	default:
		return err
	}
}
