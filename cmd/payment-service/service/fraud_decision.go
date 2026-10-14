package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"triple-p/cmd/payment-service/repo"
)

type FraudDecisionCommand struct {
	EventID, SourceEventID, PaymentID, ClientID, AccountID string
	Amount                                                 int64
	Currency, Status, Reason, CorrelationID                string
}

// ApplyFraudDecision commits Inbox, debit, ledger, status and Outbox together.
// A replay (even with a new event ID) cannot alter a terminal payment.
func (s *Service) ApplyFraudDecision(ctx context.Context, c FraudDecisionCommand) error {
	for _, id := range []string{c.EventID, c.SourceEventID, c.PaymentID, c.ClientID, c.AccountID} {
		u, err := uuid.Parse(id)
		if err != nil || u == uuid.Nil || u.String() != id {
			return fmt.Errorf("invalid fraud decision identifier: %w", ErrInvalidFraudDecision)
		}
	}
	if c.Amount <= 0 || len(c.Currency) != 3 || !isUpperASCII(c.Currency) ||
		(c.Status != "approved" && c.Status != "declined") || c.Reason == "" || len(c.Reason) > 128 || len(c.CorrelationID) > 128 {
		return fmt.Errorf("invalid fraud decision fields: %w", ErrInvalidFraudDecision)
	}
	if c.CorrelationID == "" {
		c.CorrelationID = c.PaymentID
	}
	for attempt := 0; attempt < maxProcessAttempts; attempt++ {
		err := s.transactor.WithinTransaction(ctx, func(r Repository) error {
			fresh, err := r.TryBeginInboxEvent(ctx, "payment-fraud-decisions", c.EventID, s.now().UTC())
			if err != nil || !fresh {
				return err
			}
			p, err := r.GetPaymentForUpdate(ctx, c.PaymentID)
			if errors.Is(err, repo.ErrNotFound) {
				return ErrPaymentNotFound
			}
			if err != nil {
				return err
			}
			if p.ClientID != c.ClientID || p.AccountID != c.AccountID || p.Amount != c.Amount || p.Currency != c.Currency {
				return fmt.Errorf("payment %s: %w", p.ID, ErrFraudDecisionMismatch)
			}
			known, err := r.IsPaymentCreatedEvent(ctx, c.SourceEventID, p.ID)
			if err != nil {
				return err
			}
			if !known {
				return fmt.Errorf("payment %s: %w", p.ID, ErrUnknownFraudSource)
			}
			if p.Status == repo.PaymentCompleted || p.Status == repo.PaymentFailed {
				return nil
			}
			if p.Status != repo.PaymentPending && p.Status != repo.PaymentProcessing {
				return repo.ErrInvalidState
			}
			if c.Status == "declined" {
				failure := "fraud_declined"
				p, err = r.UpdatePaymentStatus(ctx, p.ID, p.Status, repo.PaymentFailed, p.Version, &failure)
				if err != nil {
					return err
				}
				return s.appendPaymentStatusEvent(ctx, r, p, c.CorrelationID)
			}
			if p.Status == repo.PaymentPending {
				p, err = r.UpdatePaymentStatus(ctx, p.ID, p.Status, repo.PaymentProcessing, p.Version, nil)
				if err != nil {
					return err
				}
				if err = s.appendPaymentStatusEvent(ctx, r, p, c.CorrelationID); err != nil {
					return err
				}
			}
			_, err = s.finishPayment(ctx, r, p, c.CorrelationID)
			return err
		})
		if !errors.Is(err, repo.ErrOptimisticLock) {
			return err
		}
	}
	return ErrProcessingConflict
}
