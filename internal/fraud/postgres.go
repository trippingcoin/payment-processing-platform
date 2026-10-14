package fraud

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"triple-p/internal/eventerror"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"triple-p/cmd/payment-service/repo"
)

type PostgresStore struct{ pool *pgxpool.Pool }

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore { return &PostgresStore{pool: pool} }

// Record atomically deduplicates input and persists the decision plus output.
// A unique payment_id also protects against a producer sending a new event ID
// for the same payment. Rule changes never overwrite an existing decision.
func (s *PostgresStore) Record(ctx context.Context, d Decision, headers map[string]string) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	q := repo.NewQueries(tx)
	inserted, err := q.TryBeginInboxEvent(ctx, "fraud-service", d.SourceEventID, d.DecidedAt)
	if err != nil {
		return err
	}
	if !inserted {
		return tx.Commit(ctx)
	}
	tag, err := tx.Exec(ctx, `INSERT INTO fraud_decisions
		(payment_id, client_id, account_id, amount_minor, currency, source_event_id,
		 status, reason, rule_version, max_amount_minor, rule_currency, decided_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (payment_id) DO NOTHING`, d.PaymentID, d.ClientID, d.AccountID, d.Amount, d.Currency,
		d.SourceEventID, d.Status, d.Reason, d.RuleVersion, d.MaxAmount, d.RuleCurrency, d.DecidedAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		var same bool
		err = tx.QueryRow(ctx, `SELECT client_id=$2 AND account_id=$3 AND amount_minor=$4 AND currency=$5
			FROM fraud_decisions WHERE payment_id=$1`, d.PaymentID, d.ClientID, d.AccountID, d.Amount, d.Currency).Scan(&same)
		if err != nil {
			return err
		}
		if !same {
			return eventerror.Mark(eventerror.Permanent, "decision_mismatch", fmt.Errorf("conflicting payload for payment %s", d.PaymentID))
		}
		return tx.Commit(ctx)
	}
	payload, err := json.Marshal(d)
	if err != nil {
		return err
	}
	metadata, err := json.Marshal(headers)
	if err != nil {
		return err
	}
	_, err = q.AppendOutboxEvent(ctx, repo.OutboxEvent{ID: uuid.NewString(), AggregateType: "payment",
		AggregateID: d.PaymentID, EventType: "fraud." + d.Status, Payload: payload, Headers: metadata})
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
