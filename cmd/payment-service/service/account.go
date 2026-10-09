package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"triple-p/cmd/payment-service/repo"

	"github.com/google/uuid"
)

type TransactionPage struct {
	Entries    []repo.LedgerEntry
	NextCursor string
}

type transactionCursorPayload struct {
	CreatedAt time.Time `json:"created_at"`
	ID        string    `json:"id"`
}

func (s *Service) GetAccount(ctx context.Context, clientID, accountID string) (repo.Account, error) {
	clientID, accountID, err := validateAccountAccess(clientID, accountID)
	if err != nil {
		return repo.Account{}, err
	}
	account, err := s.repository.GetAccount(ctx, accountID)
	if errors.Is(err, repo.ErrNotFound) {
		return repo.Account{}, ErrAccountNotFound
	}
	if err != nil {
		return repo.Account{}, fmt.Errorf("get account: %w", err)
	}
	if account.UserID != clientID {
		return repo.Account{}, ErrAccountNotFound
	}
	return account, nil
}

func (s *Service) ListTransactions(
	ctx context.Context,
	clientID string,
	accountID string,
	cursor string,
	limit int,
) (TransactionPage, error) {
	if _, err := s.GetAccount(ctx, clientID, accountID); err != nil {
		return TransactionPage{}, err
	}
	if limit == 0 {
		limit = 50
	}
	if limit < 1 || limit > 200 {
		return TransactionPage{}, &ValidationError{Field: "limit", Reason: "must be between 1 and 200"}
	}

	decodedCursor, err := decodeTransactionCursor(cursor)
	if err != nil {
		return TransactionPage{}, &ValidationError{Field: "cursor", Reason: "is invalid"}
	}
	entries, next, err := s.repository.ListTransactions(ctx, accountID, decodedCursor, limit)
	if err != nil {
		return TransactionPage{}, fmt.Errorf("list transactions: %w", err)
	}

	page := TransactionPage{Entries: entries}
	if next != nil {
		page.NextCursor, err = encodeTransactionCursor(*next)
		if err != nil {
			return TransactionPage{}, fmt.Errorf("encode transaction cursor: %w", err)
		}
	}
	return page, nil
}

func validateAccountAccess(clientID, accountID string) (string, string, error) {
	clientID = strings.TrimSpace(clientID)
	accountID = strings.TrimSpace(accountID)
	if _, err := uuid.Parse(clientID); err != nil {
		return "", "", &ValidationError{Field: "client_id", Reason: "must be a UUID"}
	}
	if _, err := uuid.Parse(accountID); err != nil {
		return "", "", &ValidationError{Field: "account_id", Reason: "must be a UUID"}
	}
	return clientID, accountID, nil
}

func decodeTransactionCursor(cursor string) (*repo.TransactionCursor, error) {
	if cursor == "" {
		return nil, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, err
	}
	var payload transactionCursorPayload
	if err := json.Unmarshal(decoded, &payload); err != nil {
		return nil, err
	}
	if payload.CreatedAt.IsZero() {
		return nil, errors.New("cursor timestamp is empty")
	}
	if _, err := uuid.Parse(payload.ID); err != nil {
		return nil, err
	}
	return &repo.TransactionCursor{CreatedAt: payload.CreatedAt, ID: payload.ID}, nil
}

func encodeTransactionCursor(cursor repo.TransactionCursor) (string, error) {
	encoded, err := json.Marshal(transactionCursorPayload{CreatedAt: cursor.CreatedAt, ID: cursor.ID})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}
