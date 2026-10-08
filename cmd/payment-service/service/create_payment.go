package service

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"triple-p/cmd/payment-service/repo"

	"github.com/google/uuid"
)

type CreatePaymentCommand struct {
	ClientID       string
	AccountID      string
	Amount         int64
	Currency       string
	Description    string
	IdempotencyKey string
	CorrelationID  string
}

type CreatePaymentResult struct {
	Payment  repo.Payment
	Replayed bool
}

type paymentCreatedPayload struct {
	PaymentID  string `json:"payment_id"`
	ClientID   string `json:"client_id"`
	AccountID  string `json:"account_id"`
	Amount     int64  `json:"amount_minor"`
	Currency   string `json:"currency"`
	OccurredAt string `json:"occurred_at"`
}

type paymentSnapshot struct {
	ID          string             `json:"id"`
	ClientID    string             `json:"client_id"`
	AccountID   string             `json:"account_id"`
	Amount      int64              `json:"amount_minor"`
	Currency    string             `json:"currency"`
	Status      repo.PaymentStatus `json:"status"`
	FailureCode *string            `json:"failure_code,omitempty"`
	Description *string            `json:"description,omitempty"`
	Version     int64              `json:"version"`
	CreatedAt   time.Time          `json:"created_at"`
	UpdatedAt   time.Time          `json:"updated_at"`
}

func (s *Service) CreatePayment(ctx context.Context, command CreatePaymentCommand) (CreatePaymentResult, error) {
	command = normalizeCreatePaymentCommand(command)
	if err := validateCreatePaymentCommand(command); err != nil {
		return CreatePaymentResult{}, err
	}

	requestHash, err := hashCreatePaymentCommand(command)
	if err != nil {
		return CreatePaymentResult{}, fmt.Errorf("hash create payment command: %w", err)
	}

	now := s.now().UTC()
	paymentID := s.newID()
	eventID := s.newID()
	correlationID := command.CorrelationID
	if correlationID == "" {
		correlationID = paymentID
	}

	result := CreatePaymentResult{}
	err = s.transactor.WithinTransaction(ctx, func(repository Repository) error {
		reserved, reserveErr := repository.ReserveIdempotencyKey(ctx, repo.IdempotencyRecord{
			ClientID:     command.ClientID,
			Key:          command.IdempotencyKey,
			RequestHash:  requestHash,
			ResourceType: "payment",
			ResourceID:   paymentID,
			ExpiresAt:    now.Add(s.idempotencyTTL),
		})
		if reserveErr != nil {
			return fmt.Errorf("reserve idempotency key: %w", reserveErr)
		}

		if !reserved {
			replayed, replayErr := replayCreatePayment(ctx, repository, command, requestHash)
			if replayErr != nil {
				return replayErr
			}
			result = CreatePaymentResult{Payment: replayed, Replayed: true}
			return nil
		}

		account, getAccountErr := repository.GetAccount(ctx, command.AccountID)
		if errors.Is(getAccountErr, repo.ErrNotFound) {
			return ErrAccountNotFound
		}
		if getAccountErr != nil {
			return fmt.Errorf("get payment account: %w", getAccountErr)
		}
		if account.UserID != command.ClientID {
			// Do not expose the existence of an account belonging to another client.
			return ErrAccountNotFound
		}
		if account.Currency != command.Currency {
			return ErrCurrencyMismatch
		}

		var description *string
		if command.Description != "" {
			description = &command.Description
		}

		payment, createErr := repository.CreatePayment(ctx, repo.Payment{
			ID:          paymentID,
			ClientID:    command.ClientID,
			AccountID:   command.AccountID,
			Amount:      command.Amount,
			Currency:    command.Currency,
			Status:      repo.PaymentPending,
			Description: description,
		})
		if createErr != nil {
			return fmt.Errorf("create payment: %w", createErr)
		}

		payload, marshalErr := json.Marshal(paymentCreatedPayload{
			PaymentID:  payment.ID,
			ClientID:   payment.ClientID,
			AccountID:  payment.AccountID,
			Amount:     payment.Amount,
			Currency:   payment.Currency,
			OccurredAt: now.Format(time.RFC3339Nano),
		})
		if marshalErr != nil {
			return fmt.Errorf("marshal payment.created payload: %w", marshalErr)
		}
		headers, marshalErr := json.Marshal(map[string]any{
			"correlation_id": correlationID,
			"schema_version": 1,
		})
		if marshalErr != nil {
			return fmt.Errorf("marshal payment.created headers: %w", marshalErr)
		}

		if _, appendErr := repository.AppendOutboxEvent(ctx, repo.OutboxEvent{
			ID:            eventID,
			AggregateType: "payment",
			AggregateID:   payment.ID,
			EventType:     "payment.created",
			Payload:       payload,
			Headers:       headers,
		}); appendErr != nil {
			return fmt.Errorf("append payment.created event: %w", appendErr)
		}

		responseBody, marshalErr := json.Marshal(snapshotFromPayment(payment))
		if marshalErr != nil {
			return fmt.Errorf("marshal idempotency response: %w", marshalErr)
		}
		if saveErr := repository.SaveIdempotencyResponse(
			ctx,
			command.ClientID,
			command.IdempotencyKey,
			202,
			responseBody,
		); saveErr != nil {
			return fmt.Errorf("save idempotency response: %w", saveErr)
		}

		result = CreatePaymentResult{Payment: payment}
		return nil
	})
	if err != nil {
		return CreatePaymentResult{}, err
	}
	return result, nil
}

func replayCreatePayment(
	ctx context.Context,
	repository Repository,
	command CreatePaymentCommand,
	requestHash []byte,
) (repo.Payment, error) {
	record, err := repository.GetIdempotencyKey(ctx, command.ClientID, command.IdempotencyKey)
	if err != nil {
		return repo.Payment{}, fmt.Errorf("load idempotency key: %w", err)
	}
	if record.ResourceType != "payment" ||
		subtle.ConstantTimeCompare(record.RequestHash, requestHash) != 1 {
		return repo.Payment{}, ErrIdempotencyConflict
	}

	if len(record.ResponseBody) > 0 {
		var snapshot paymentSnapshot
		if err := json.Unmarshal(record.ResponseBody, &snapshot); err != nil {
			return repo.Payment{}, fmt.Errorf("decode stored idempotency response: %w", err)
		}
		return snapshot.payment(), nil
	}

	payment, err := repository.GetPayment(ctx, record.ResourceID)
	if err != nil {
		return repo.Payment{}, fmt.Errorf("load idempotent payment: %w", err)
	}
	return payment, nil
}

func normalizeCreatePaymentCommand(command CreatePaymentCommand) CreatePaymentCommand {
	command.ClientID = strings.TrimSpace(command.ClientID)
	command.AccountID = strings.TrimSpace(command.AccountID)
	command.Currency = strings.ToUpper(strings.TrimSpace(command.Currency))
	command.Description = strings.TrimSpace(command.Description)
	command.CorrelationID = strings.TrimSpace(command.CorrelationID)
	return command
}

func validateCreatePaymentCommand(command CreatePaymentCommand) error {
	if _, err := uuid.Parse(command.ClientID); err != nil {
		return &ValidationError{Field: "client_id", Reason: "must be a UUID"}
	}
	if _, err := uuid.Parse(command.AccountID); err != nil {
		return &ValidationError{Field: "account_id", Reason: "must be a UUID"}
	}
	if command.Amount <= 0 {
		return &ValidationError{Field: "amount_minor", Reason: "must be greater than zero"}
	}
	if len(command.Currency) != 3 || !isUpperASCII(command.Currency) {
		return &ValidationError{Field: "currency", Reason: "must be a three-letter ISO code"}
	}
	if strings.TrimSpace(command.IdempotencyKey) == "" || len(command.IdempotencyKey) > 255 {
		return &ValidationError{Field: "idempotency_key", Reason: "must contain 1 to 255 characters"}
	}
	if len(command.Description) > 500 {
		return &ValidationError{Field: "description", Reason: "must not exceed 500 characters"}
	}
	if len(command.CorrelationID) > 128 {
		return &ValidationError{Field: "correlation_id", Reason: "must not exceed 128 characters"}
	}
	return nil
}

func isUpperASCII(value string) bool {
	for _, character := range value {
		if character < 'A' || character > 'Z' {
			return false
		}
	}
	return true
}

func hashCreatePaymentCommand(command CreatePaymentCommand) ([]byte, error) {
	canonical := struct {
		AccountID   string `json:"account_id"`
		Amount      int64  `json:"amount_minor"`
		Currency    string `json:"currency"`
		Description string `json:"description"`
	}{
		AccountID:   command.AccountID,
		Amount:      command.Amount,
		Currency:    command.Currency,
		Description: command.Description,
	}

	encoded, err := json.Marshal(canonical)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(encoded)
	return hash[:], nil
}

func snapshotFromPayment(payment repo.Payment) paymentSnapshot {
	return paymentSnapshot{
		ID:          payment.ID,
		ClientID:    payment.ClientID,
		AccountID:   payment.AccountID,
		Amount:      payment.Amount,
		Currency:    payment.Currency,
		Status:      payment.Status,
		FailureCode: payment.FailureCode,
		Description: payment.Description,
		Version:     payment.Version,
		CreatedAt:   payment.CreatedAt,
		UpdatedAt:   payment.UpdatedAt,
	}
}

func (snapshot paymentSnapshot) payment() repo.Payment {
	return repo.Payment{
		ID:          snapshot.ID,
		ClientID:    snapshot.ClientID,
		AccountID:   snapshot.AccountID,
		Amount:      snapshot.Amount,
		Currency:    snapshot.Currency,
		Status:      snapshot.Status,
		FailureCode: snapshot.FailureCode,
		Description: snapshot.Description,
		Version:     snapshot.Version,
		CreatedAt:   snapshot.CreatedAt,
		UpdatedAt:   snapshot.UpdatedAt,
	}
}
