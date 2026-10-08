package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"triple-p/cmd/payment-service/repo"

	"github.com/google/uuid"
)

func (s *Service) GetPayment(ctx context.Context, clientID, paymentID string) (repo.Payment, error) {
	clientID = strings.TrimSpace(clientID)
	paymentID = strings.TrimSpace(paymentID)
	if _, err := uuid.Parse(clientID); err != nil {
		return repo.Payment{}, &ValidationError{Field: "client_id", Reason: "must be a UUID"}
	}
	if _, err := uuid.Parse(paymentID); err != nil {
		return repo.Payment{}, &ValidationError{Field: "payment_id", Reason: "must be a UUID"}
	}

	payment, err := s.repository.GetPayment(ctx, paymentID)
	if errors.Is(err, repo.ErrNotFound) {
		return repo.Payment{}, ErrPaymentNotFound
	}
	if err != nil {
		return repo.Payment{}, fmt.Errorf("get payment: %w", err)
	}
	if payment.ClientID != clientID {
		// Deliberately hide whether a payment belonging to another client exists.
		return repo.Payment{}, ErrPaymentNotFound
	}
	return payment, nil
}
