package service

import (
	"context"
	"encoding/json"
	"time"
	"triple-p/cmd/payment-service/repo"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const defaultIdempotencyTTL = 24 * time.Hour

// Repository is intentionally narrower than repo.Queries. The use-case layer
// only sees persistence operations that it actually needs.
type Repository interface {
	GetAccount(context.Context, string) (repo.Account, error)
	CreatePayment(context.Context, repo.Payment) (repo.Payment, error)
	GetPayment(context.Context, string) (repo.Payment, error)
	ReserveIdempotencyKey(context.Context, repo.IdempotencyRecord) (bool, error)
	GetIdempotencyKey(context.Context, string, string) (repo.IdempotencyRecord, error)
	SaveIdempotencyResponse(context.Context, string, string, int, json.RawMessage) error
	AppendOutboxEvent(context.Context, repo.OutboxEvent) (repo.OutboxEvent, error)
}

type Transactor interface {
	WithinTransaction(context.Context, func(Repository) error) error
}

type Service struct {
	repository     Repository
	transactor     Transactor
	now            func() time.Time
	newID          func() string
	idempotencyTTL time.Duration
}

func New(store *repo.Store, options ...Option) *Service {
	return NewWithDependencies(store.Queries, postgresTransactor{store: store}, options...)
}

func NewWithDependencies(repository Repository, transactor Transactor, options ...Option) *Service {
	service := &Service{
		repository:     repository,
		transactor:     transactor,
		now:            time.Now,
		newID:          uuid.NewString,
		idempotencyTTL: defaultIdempotencyTTL,
	}
	for _, option := range options {
		option(service)
	}
	return service
}

type Option func(*Service)

func WithClock(now func() time.Time) Option {
	return func(service *Service) {
		service.now = now
	}
}

func WithIDGenerator(newID func() string) Option {
	return func(service *Service) {
		service.newID = newID
	}
}

func WithIdempotencyTTL(ttl time.Duration) Option {
	return func(service *Service) {
		if ttl > 0 {
			service.idempotencyTTL = ttl
		}
	}
}

type postgresTransactor struct {
	store *repo.Store
}

func (t postgresTransactor) WithinTransaction(ctx context.Context, fn func(Repository) error) error {
	return t.store.WithTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(queries *repo.Queries) error {
		return fn(queries)
	})
}

var (
	_ Repository = (*repo.Queries)(nil)
	_ Transactor = postgresTransactor{}
)
