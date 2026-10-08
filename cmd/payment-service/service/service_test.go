package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
	"triple-p/cmd/payment-service/repo"
)

const (
	testClientID  = "00000000-0000-0000-0000-000000000001"
	testAccountID = "00000000-0000-0000-0000-000000000002"
	testPaymentID = "00000000-0000-0000-0000-000000000003"
	testEventID   = "00000000-0000-0000-0000-000000000004"
)

var testNow = time.Date(2026, time.October, 8, 10, 30, 0, 0, time.UTC)

func TestCreatePaymentStoresPaymentIdempotencyAndOutboxAtomically(t *testing.T) {
	repository := newFakeRepository()
	repository.accounts[testAccountID] = repo.Account{
		ID: testAccountID, UserID: testClientID, Currency: "KZT", AvailableBalance: 2_000_000,
	}
	service := newTestService(repository)

	result, err := service.CreatePayment(context.Background(), CreatePaymentCommand{
		ClientID:       testClientID,
		AccountID:      testAccountID,
		Amount:         1_000_000,
		Currency:       "kzt",
		Description:    "  Order #42  ",
		IdempotencyKey: "create-order-42",
		CorrelationID:  "request-42",
	})
	if err != nil {
		t.Fatalf("CreatePayment() error = %v", err)
	}

	if result.Replayed {
		t.Fatal("CreatePayment() unexpectedly reported replay")
	}
	if result.Payment.ID != testPaymentID || result.Payment.Status != repo.PaymentPending {
		t.Fatalf("created payment = %+v", result.Payment)
	}
	if result.Payment.Currency != "KZT" {
		t.Fatalf("payment currency = %q, want KZT", result.Payment.Currency)
	}
	if result.Payment.Description == nil || *result.Payment.Description != "Order #42" {
		t.Fatalf("payment description = %v, want normalized description", result.Payment.Description)
	}
	if len(repository.payments) != 1 || len(repository.outbox) != 1 || len(repository.idempotency) != 1 {
		t.Fatalf(
			"persisted payments/outbox/idempotency = %d/%d/%d, want 1/1/1",
			len(repository.payments), len(repository.outbox), len(repository.idempotency),
		)
	}

	event := repository.outbox[testEventID]
	if event.EventType != "payment.created" || event.AggregateID != testPaymentID {
		t.Fatalf("outbox event = %+v", event)
	}
	var headers map[string]any
	if err := json.Unmarshal(event.Headers, &headers); err != nil {
		t.Fatalf("decode outbox headers: %v", err)
	}
	if headers["correlation_id"] != "request-42" {
		t.Fatalf("correlation_id = %v, want request-42", headers["correlation_id"])
	}

	idempotency := repository.idempotency[idempotencyMapKey(testClientID, "create-order-42")]
	if idempotency.ResponseStatus == nil || *idempotency.ResponseStatus != 202 {
		t.Fatalf("stored response status = %v, want 202", idempotency.ResponseStatus)
	}
}

func TestCreatePaymentReplaysOriginalResponse(t *testing.T) {
	repository := newFakeRepository()
	repository.accounts[testAccountID] = repo.Account{ID: testAccountID, UserID: testClientID, Currency: "KZT"}
	service := newTestService(repository)
	command := CreatePaymentCommand{
		ClientID:       testClientID,
		AccountID:      testAccountID,
		Amount:         1_000_000,
		Currency:       "KZT",
		IdempotencyKey: "same-key",
	}

	first, err := service.CreatePayment(context.Background(), command)
	if err != nil {
		t.Fatalf("first CreatePayment() error = %v", err)
	}
	second, err := service.CreatePayment(context.Background(), command)
	if err != nil {
		t.Fatalf("second CreatePayment() error = %v", err)
	}

	if !second.Replayed {
		t.Fatal("second CreatePayment() did not report replay")
	}
	if !reflect.DeepEqual(second.Payment, first.Payment) {
		t.Fatalf("replayed payment = %+v, want %+v", second.Payment, first.Payment)
	}
	if len(repository.payments) != 1 || len(repository.outbox) != 1 {
		t.Fatalf("replay created duplicates: payments=%d outbox=%d", len(repository.payments), len(repository.outbox))
	}
}

func TestCreatePaymentRejectsIdempotencyKeyWithDifferentPayload(t *testing.T) {
	repository := newFakeRepository()
	repository.accounts[testAccountID] = repo.Account{ID: testAccountID, UserID: testClientID, Currency: "KZT"}
	service := newTestService(repository)
	command := CreatePaymentCommand{
		ClientID:       testClientID,
		AccountID:      testAccountID,
		Amount:         1_000_000,
		Currency:       "KZT",
		IdempotencyKey: "same-key",
	}

	if _, err := service.CreatePayment(context.Background(), command); err != nil {
		t.Fatalf("first CreatePayment() error = %v", err)
	}
	command.Amount++
	_, err := service.CreatePayment(context.Background(), command)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("second CreatePayment() error = %v, want %v", err, ErrIdempotencyConflict)
	}
	if len(repository.payments) != 1 || len(repository.outbox) != 1 {
		t.Fatalf("conflicting retry created duplicates: payments=%d outbox=%d", len(repository.payments), len(repository.outbox))
	}
}

func TestCreatePaymentRollsBackWhenOutboxFails(t *testing.T) {
	repository := newFakeRepository()
	repository.accounts[testAccountID] = repo.Account{ID: testAccountID, UserID: testClientID, Currency: "KZT"}
	repository.appendOutboxError = errors.New("outbox unavailable")
	service := newTestService(repository)

	_, err := service.CreatePayment(context.Background(), CreatePaymentCommand{
		ClientID:       testClientID,
		AccountID:      testAccountID,
		Amount:         1_000_000,
		Currency:       "KZT",
		IdempotencyKey: "rollback-key",
	})
	if err == nil {
		t.Fatal("CreatePayment() error = nil, want outbox error")
	}
	if len(repository.payments) != 0 || len(repository.outbox) != 0 || len(repository.idempotency) != 0 {
		t.Fatalf(
			"failed transaction persisted state: payments=%d outbox=%d idempotency=%d",
			len(repository.payments), len(repository.outbox), len(repository.idempotency),
		)
	}
}

func TestCreatePaymentValidatesCommand(t *testing.T) {
	valid := CreatePaymentCommand{
		ClientID:       testClientID,
		AccountID:      testAccountID,
		Amount:         1,
		Currency:       "KZT",
		IdempotencyKey: "key",
	}
	tests := []struct {
		name   string
		change func(*CreatePaymentCommand)
	}{
		{name: "client id", change: func(command *CreatePaymentCommand) { command.ClientID = "bad" }},
		{name: "account id", change: func(command *CreatePaymentCommand) { command.AccountID = "bad" }},
		{name: "amount", change: func(command *CreatePaymentCommand) { command.Amount = 0 }},
		{name: "currency", change: func(command *CreatePaymentCommand) { command.Currency = "US" }},
		{name: "idempotency key", change: func(command *CreatePaymentCommand) { command.IdempotencyKey = " " }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			command := valid
			tt.change(&command)
			service := newTestService(newFakeRepository())
			_, err := service.CreatePayment(context.Background(), command)
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("CreatePayment() error = %v, want validation error", err)
			}
		})
	}
}

func TestCreatePaymentRejectsForeignAccountAndCurrencyMismatch(t *testing.T) {
	tests := []struct {
		name    string
		account repo.Account
		want    error
	}{
		{
			name:    "foreign account",
			account: repo.Account{ID: testAccountID, UserID: "00000000-0000-0000-0000-000000000099", Currency: "KZT"},
			want:    ErrAccountNotFound,
		},
		{
			name:    "currency mismatch",
			account: repo.Account{ID: testAccountID, UserID: testClientID, Currency: "USD"},
			want:    ErrCurrencyMismatch,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repository := newFakeRepository()
			repository.accounts[testAccountID] = tt.account
			service := newTestService(repository)
			_, err := service.CreatePayment(context.Background(), CreatePaymentCommand{
				ClientID:       testClientID,
				AccountID:      testAccountID,
				Amount:         1_000_000,
				Currency:       "KZT",
				IdempotencyKey: "account-check",
			})
			if !errors.Is(err, tt.want) {
				t.Fatalf("CreatePayment() error = %v, want %v", err, tt.want)
			}
			if len(repository.idempotency) != 0 {
				t.Fatal("failed transaction retained idempotency reservation")
			}
		})
	}
}

func TestGetPaymentChecksClientOwnership(t *testing.T) {
	repository := newFakeRepository()
	repository.payments[testPaymentID] = repo.Payment{ID: testPaymentID, ClientID: testClientID}
	service := newTestService(repository)

	if _, err := service.GetPayment(context.Background(), testClientID, testPaymentID); err != nil {
		t.Fatalf("GetPayment() owner error = %v", err)
	}
	_, err := service.GetPayment(
		context.Background(),
		"00000000-0000-0000-0000-000000000099",
		testPaymentID,
	)
	if !errors.Is(err, ErrPaymentNotFound) {
		t.Fatalf("GetPayment() foreign owner error = %v, want %v", err, ErrPaymentNotFound)
	}
}

func newTestService(repository *fakeRepository) *Service {
	ids := []string{
		testPaymentID,
		testEventID,
		"00000000-0000-0000-0000-000000000005",
		"00000000-0000-0000-0000-000000000006",
	}
	nextID := 0
	return NewWithDependencies(
		repository,
		fakeTransactor{repository: repository},
		WithClock(func() time.Time { return testNow }),
		WithIDGenerator(func() string {
			id := ids[nextID]
			nextID++
			return id
		}),
	)
}

type fakeTransactor struct {
	repository *fakeRepository
}

func (transactor fakeTransactor) WithinTransaction(_ context.Context, fn func(Repository) error) error {
	transaction := transactor.repository.clone()
	if err := fn(transaction); err != nil {
		return err
	}
	transactor.repository.replaceWith(transaction)
	return nil
}

type fakeRepository struct {
	accounts          map[string]repo.Account
	payments          map[string]repo.Payment
	idempotency       map[string]repo.IdempotencyRecord
	outbox            map[string]repo.OutboxEvent
	appendOutboxError error
}

func newFakeRepository() *fakeRepository {
	return &fakeRepository{
		accounts:    make(map[string]repo.Account),
		payments:    make(map[string]repo.Payment),
		idempotency: make(map[string]repo.IdempotencyRecord),
		outbox:      make(map[string]repo.OutboxEvent),
	}
}

func (repository *fakeRepository) clone() *fakeRepository {
	cloned := newFakeRepository()
	cloned.appendOutboxError = repository.appendOutboxError
	for key, value := range repository.accounts {
		cloned.accounts[key] = value
	}
	for key, value := range repository.payments {
		cloned.payments[key] = value
	}
	for key, value := range repository.idempotency {
		value.RequestHash = append([]byte(nil), value.RequestHash...)
		value.ResponseBody = append(json.RawMessage(nil), value.ResponseBody...)
		cloned.idempotency[key] = value
	}
	for key, value := range repository.outbox {
		cloned.outbox[key] = value
	}
	return cloned
}

func (repository *fakeRepository) replaceWith(updated *fakeRepository) {
	repository.accounts = updated.accounts
	repository.payments = updated.payments
	repository.idempotency = updated.idempotency
	repository.outbox = updated.outbox
}

func (repository *fakeRepository) GetAccount(_ context.Context, id string) (repo.Account, error) {
	account, ok := repository.accounts[id]
	if !ok {
		return repo.Account{}, repo.ErrNotFound
	}
	return account, nil
}

func (repository *fakeRepository) CreatePayment(_ context.Context, payment repo.Payment) (repo.Payment, error) {
	if _, exists := repository.payments[payment.ID]; exists {
		return repo.Payment{}, repo.ErrAlreadyExists
	}
	payment.CreatedAt = testNow
	payment.UpdatedAt = testNow
	repository.payments[payment.ID] = payment
	return payment, nil
}

func (repository *fakeRepository) GetPayment(_ context.Context, id string) (repo.Payment, error) {
	payment, ok := repository.payments[id]
	if !ok {
		return repo.Payment{}, repo.ErrNotFound
	}
	return payment, nil
}

func (repository *fakeRepository) ReserveIdempotencyKey(
	_ context.Context,
	record repo.IdempotencyRecord,
) (bool, error) {
	key := idempotencyMapKey(record.ClientID, record.Key)
	if existing, ok := repository.idempotency[key]; ok && existing.ExpiresAt.After(testNow) {
		return false, nil
	}
	record.CreatedAt = testNow
	repository.idempotency[key] = record
	return true, nil
}

func (repository *fakeRepository) GetIdempotencyKey(
	_ context.Context,
	clientID string,
	key string,
) (repo.IdempotencyRecord, error) {
	record, ok := repository.idempotency[idempotencyMapKey(clientID, key)]
	if !ok {
		return repo.IdempotencyRecord{}, repo.ErrNotFound
	}
	return record, nil
}

func (repository *fakeRepository) SaveIdempotencyResponse(
	_ context.Context,
	clientID string,
	key string,
	status int,
	body json.RawMessage,
) error {
	mapKey := idempotencyMapKey(clientID, key)
	record, ok := repository.idempotency[mapKey]
	if !ok {
		return repo.ErrNotFound
	}
	record.ResponseStatus = &status
	record.ResponseBody = append(json.RawMessage(nil), body...)
	repository.idempotency[mapKey] = record
	return nil
}

func (repository *fakeRepository) AppendOutboxEvent(
	_ context.Context,
	event repo.OutboxEvent,
) (repo.OutboxEvent, error) {
	if repository.appendOutboxError != nil {
		return repo.OutboxEvent{}, repository.appendOutboxError
	}
	if _, exists := repository.outbox[event.ID]; exists {
		return repo.OutboxEvent{}, repo.ErrAlreadyExists
	}
	event.CreatedAt = testNow
	repository.outbox[event.ID] = event
	return event, nil
}

func idempotencyMapKey(clientID, key string) string {
	return fmt.Sprintf("%s:%s", clientID, key)
}
