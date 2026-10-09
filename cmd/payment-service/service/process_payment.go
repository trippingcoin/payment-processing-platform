package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"triple-p/cmd/payment-service/repo"

	"github.com/google/uuid"
)

const maxProcessAttempts = 3

const failureInsufficientFunds = "insufficient_funds"

type ProcessPaymentCommand struct {
	PaymentID     string
	CorrelationID string
}

type paymentStatusPayload struct {
	PaymentID   string             `json:"payment_id"`
	ClientID    string             `json:"client_id"`
	AccountID   string             `json:"account_id"`
	Amount      int64              `json:"amount_minor"`
	Currency    string             `json:"currency"`
	Status      repo.PaymentStatus `json:"status"`
	FailureCode *string            `json:"failure_code,omitempty"`
	OccurredAt  string             `json:"occurred_at"`
}

func (s *Service) ProcessPayment(ctx context.Context, command ProcessPaymentCommand) (repo.Payment, error) {
	command.PaymentID = strings.TrimSpace(command.PaymentID)
	command.CorrelationID = strings.TrimSpace(command.CorrelationID)
	if _, err := uuid.Parse(command.PaymentID); err != nil {
		return repo.Payment{}, &ValidationError{Field: "payment_id", Reason: "must be a UUID"}
	}
	if len(command.CorrelationID) > 128 {
		return repo.Payment{}, &ValidationError{Field: "correlation_id", Reason: "must not exceed 128 characters"}
	}
	if command.CorrelationID == "" {
		command.CorrelationID = command.PaymentID
	}

	payment, err := s.startPaymentProcessing(ctx, command)
	if err != nil {
		return repo.Payment{}, err
	}
	if payment.Status == repo.PaymentCompleted || payment.Status == repo.PaymentFailed {
		return payment, nil
	}

	for attempt := 0; attempt < maxProcessAttempts; attempt++ {
		payment, err = s.completePaymentProcessing(ctx, command)
		if !errors.Is(err, repo.ErrOptimisticLock) {
			return payment, err
		}
	}
	return repo.Payment{}, ErrProcessingConflict
}

func (s *Service) startPaymentProcessing(
	ctx context.Context,
	command ProcessPaymentCommand,
) (repo.Payment, error) {
	var result repo.Payment
	err := s.transactor.WithinTransaction(ctx, func(repository Repository) error {
		payment, err := repository.GetPaymentForUpdate(ctx, command.PaymentID)
		if errors.Is(err, repo.ErrNotFound) {
			return ErrPaymentNotFound
		}
		if err != nil {
			return fmt.Errorf("lock payment for processing: %w", err)
		}

		switch payment.Status {
		case repo.PaymentCompleted, repo.PaymentFailed, repo.PaymentProcessing:
			result = payment
			return nil
		case repo.PaymentPending:
		default:
			return fmt.Errorf("unexpected payment status %q: %w", payment.Status, repo.ErrInvalidState)
		}

		processing, err := repository.UpdatePaymentStatus(
			ctx,
			payment.ID,
			repo.PaymentPending,
			repo.PaymentProcessing,
			payment.Version,
			nil,
		)
		if err != nil {
			return fmt.Errorf("mark payment processing: %w", err)
		}
		if err := s.appendPaymentStatusEvent(ctx, repository, processing, command.CorrelationID); err != nil {
			return err
		}
		result = processing
		return nil
	})
	return result, err
}

func (s *Service) completePaymentProcessing(
	ctx context.Context,
	command ProcessPaymentCommand,
) (repo.Payment, error) {
	var result repo.Payment
	err := s.transactor.WithinTransaction(ctx, func(repository Repository) error {
		payment, err := repository.GetPaymentForUpdate(ctx, command.PaymentID)
		if errors.Is(err, repo.ErrNotFound) {
			return ErrPaymentNotFound
		}
		if err != nil {
			return fmt.Errorf("lock processing payment: %w", err)
		}
		if payment.Status == repo.PaymentCompleted || payment.Status == repo.PaymentFailed {
			result = payment
			return nil
		}
		if payment.Status != repo.PaymentProcessing {
			return fmt.Errorf("payment is %q, expected processing: %w", payment.Status, repo.ErrInvalidState)
		}

		account, err := repository.GetAccount(ctx, payment.AccountID)
		if err != nil {
			return fmt.Errorf("load debit account: %w", err)
		}
		debited, err := repository.DebitAccount(
			ctx,
			account.ID,
			payment.Currency,
			payment.Amount,
			account.Version,
		)
		if errors.Is(err, repo.ErrInsufficientFunds) {
			failureCode := failureInsufficientFunds
			failed, updateErr := repository.UpdatePaymentStatus(
				ctx,
				payment.ID,
				repo.PaymentProcessing,
				repo.PaymentFailed,
				payment.Version,
				&failureCode,
			)
			if updateErr != nil {
				return fmt.Errorf("fail payment: %w", updateErr)
			}
			if eventErr := s.appendPaymentStatusEvent(ctx, repository, failed, command.CorrelationID); eventErr != nil {
				return eventErr
			}
			result = failed
			return nil
		}
		if err != nil {
			return fmt.Errorf("debit account: %w", err)
		}

		paymentID := payment.ID
		if _, err := repository.AppendLedgerEntry(ctx, repo.LedgerEntry{
			ID:           s.newID(),
			AccountID:    account.ID,
			PaymentID:    &paymentID,
			OperationID:  payment.ID,
			EntryType:    repo.LedgerDebit,
			Amount:       payment.Amount,
			BalanceAfter: debited.AvailableBalance,
		}); err != nil {
			return fmt.Errorf("append debit ledger entry: %w", err)
		}

		completed, err := repository.UpdatePaymentStatus(
			ctx,
			payment.ID,
			repo.PaymentProcessing,
			repo.PaymentCompleted,
			payment.Version,
			nil,
		)
		if err != nil {
			return fmt.Errorf("complete payment: %w", err)
		}
		if err := s.appendPaymentStatusEvent(ctx, repository, completed, command.CorrelationID); err != nil {
			return err
		}
		result = completed
		return nil
	})
	return result, err
}

func (s *Service) appendPaymentStatusEvent(
	ctx context.Context,
	repository Repository,
	payment repo.Payment,
	correlationID string,
) error {
	payload, err := json.Marshal(paymentStatusPayload{
		PaymentID:   payment.ID,
		ClientID:    payment.ClientID,
		AccountID:   payment.AccountID,
		Amount:      payment.Amount,
		Currency:    payment.Currency,
		Status:      payment.Status,
		FailureCode: payment.FailureCode,
		OccurredAt:  s.now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return fmt.Errorf("marshal payment.%s payload: %w", payment.Status, err)
	}
	headers, err := json.Marshal(map[string]any{
		"correlation_id": correlationID,
		"schema_version": 1,
	})
	if err != nil {
		return fmt.Errorf("marshal payment.%s headers: %w", payment.Status, err)
	}
	if _, err := repository.AppendOutboxEvent(ctx, repo.OutboxEvent{
		ID:            s.newID(),
		AggregateType: "payment",
		AggregateID:   payment.ID,
		EventType:     "payment." + string(payment.Status),
		Payload:       payload,
		Headers:       headers,
	}); err != nil {
		return fmt.Errorf("append payment.%s event: %w", payment.Status, err)
	}
	return nil
}
