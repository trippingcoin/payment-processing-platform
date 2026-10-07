package repo

import (
	"context"
	"errors"
	"fmt"
	"time"
	"triple-p/config"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DBTX interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type Store struct {
	pool *pgxpool.Pool
	*Queries
}

type Queries struct {
	db DBTX
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, Queries: NewQueries(pool)}
}

func NewQueries(db DBTX) *Queries {
	return &Queries{db: db}
}

func Open(ctx context.Context, cfg config.Config) (*Store, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.DB.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}

	if cfg.DB.MaxOpenConns > 0 {
		poolConfig.MaxConns = int32(cfg.DB.MaxOpenConns)
	}
	if cfg.DB.MinIdleConns > 0 {
		poolConfig.MinIdleConns = min(int32(cfg.DB.MinIdleConns), poolConfig.MaxConns)
	}
	if cfg.DB.ConnMaxLifetime > 0 {
		poolConfig.MaxConnLifetime = cfg.DB.ConnMaxLifetime
	}
	if cfg.DB.ConnMaxIdleTime > 0 {
		poolConfig.MaxConnIdleTime = cfg.DB.ConnMaxIdleTime
	}

	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for {
		pool, openErr := pgxpool.NewWithConfig(ctx, poolConfig)
		if openErr == nil {
			pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			openErr = pool.Ping(pingCtx)
			cancel()
			if openErr == nil {
				return New(pool), nil
			}
			pool.Close()
		}

		lastErr = openErr
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("connect to postgres: %w", lastErr)
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("connect to postgres: %w", ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}

func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

func (s *Store) Ping(ctx context.Context) error {
	if err := s.pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}
	return nil
}

func (s *Store) WithTx(ctx context.Context, options pgx.TxOptions, fn func(*Queries) error) error {
	tx, err := s.pool.BeginTx(ctx, options)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() { _ = tx.Rollback(context.Background()) }()

	if err := fn(NewQueries(tx)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return fmt.Errorf("commit transaction after rollback: %w", err)
		}
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}
