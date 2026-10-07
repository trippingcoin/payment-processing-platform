//go:build integration

package repo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"triple-p/config"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestConcurrentDebitNeverMakesBalanceNegative(t *testing.T) {
	store := newIntegrationStore(t)
	ctx := context.Background()

	account, err := store.CreateAccount(ctx, Account{
		ID:               "00000000-0000-0000-0000-000000000001",
		UserID:           "00000000-0000-0000-0000-000000000002",
		Currency:         "KZT",
		AvailableBalance: 1_000_000,
	})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}

	const workers = 20
	start := make(chan struct{})
	var (
		wg                  sync.WaitGroup
		succeeded           atomic.Int32
		insufficientOrStale atomic.Int32
	)

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, debitErr := store.DebitAccount(
				ctx,
				account.ID,
				account.Currency,
				account.AvailableBalance,
				account.Version,
			)
			switch {
			case debitErr == nil:
				succeeded.Add(1)
			case errors.Is(debitErr, ErrInsufficientFunds), errors.Is(debitErr, ErrOptimisticLock):
				insufficientOrStale.Add(1)
			default:
				t.Errorf("unexpected debit error: %v", debitErr)
			}
		}()
	}

	close(start)
	wg.Wait()

	if got := succeeded.Load(); got != 1 {
		t.Fatalf("successful debits = %d, want 1", got)
	}
	if got := insufficientOrStale.Load(); got != workers-1 {
		t.Fatalf("rejected debits = %d, want %d", got, workers-1)
	}

	finalAccount, err := store.GetAccount(ctx, account.ID)
	if err != nil {
		t.Fatalf("get final account: %v", err)
	}
	if finalAccount.AvailableBalance != 0 {
		t.Fatalf("final balance = %d, want 0", finalAccount.AvailableBalance)
	}
}

func TestConcurrentIdempotencyReservationHasSingleWinner(t *testing.T) {
	store := newIntegrationStore(t)
	ctx := context.Background()

	const workers = 20
	start := make(chan struct{})
	var (
		wg      sync.WaitGroup
		winners atomic.Int32
	)

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			reserved, err := store.ReserveIdempotencyKey(ctx, IdempotencyRecord{
				ClientID:     "00000000-0000-0000-0000-000000000010",
				Key:          "same-key",
				RequestHash:  []byte("same-payload-hash"),
				ResourceType: "payment",
				ResourceID:   "00000000-0000-0000-0000-000000000011",
				ExpiresAt:    time.Now().Add(time.Hour),
			})
			if err != nil {
				t.Errorf("reserve idempotency key: %v", err)
				return
			}
			if reserved {
				winners.Add(1)
			}
		}()
	}

	close(start)
	wg.Wait()

	if got := winners.Load(); got != 1 {
		t.Fatalf("reservation winners = %d, want 1", got)
	}
}

func newIntegrationStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()

	container, err := postgres.Run(
		ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("payments"),
		postgres.WithUsername("payments"),
		postgres.WithPassword("payments"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	testcontainers.CleanupContainer(t, container)

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}

	cfg := config.Config{}
	cfg.DB.DSN = dsn
	cfg.DB.MaxOpenConns = 25
	cfg.DB.MinIdleConns = 1
	cfg.DB.ConnMaxLifetime = time.Minute
	cfg.DB.ConnMaxIdleTime = time.Minute

	store, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open repository: %v", err)
	}
	t.Cleanup(store.Close)

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve integration test path")
	}
	migrationPath := filepath.Join(
		filepath.Dir(currentFile),
		"..", "..", "..", "migrations", "payment-service", "000001_init.up.sql",
	)
	migration, err := os.ReadFile(migrationPath)
	if err != nil {
		t.Fatalf("read migration %s: %v", migrationPath, err)
	}
	if _, err := store.pool.Exec(ctx, string(migration)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}

	return store
}

func TestMigrationPathIsStable(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	path := filepath.Join(
		filepath.Dir(currentFile),
		"..", "..", "..", "migrations", "payment-service", "000001_init.up.sql",
	)
	if _, err := os.Stat(path); err != nil {
		t.Fatal(fmt.Errorf("migration file unavailable: %w", err))
	}
}
