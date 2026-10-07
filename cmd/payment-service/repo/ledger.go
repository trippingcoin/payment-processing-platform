package repo

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const ledgerColumns = `
	id, account_id, payment_id, refund_id, operation_id, entry_type,
	amount, balance_after, created_at`

func (q *Queries) AppendLedgerEntry(ctx context.Context, entry LedgerEntry) (LedgerEntry, error) {
	const statement = `
		INSERT INTO ledger_entries (
			id, account_id, payment_id, refund_id, operation_id,
			entry_type, amount, balance_after
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING ` + ledgerColumns

	created, err := scanLedgerEntry(q.db.QueryRow(ctx, statement,
		entry.ID,
		entry.AccountID,
		entry.PaymentID,
		entry.RefundID,
		entry.OperationID,
		entry.EntryType,
		entry.Amount,
		entry.BalanceAfter,
	))
	if err != nil {
		return LedgerEntry{}, fmt.Errorf("append ledger entry: %w", mapPostgresError(err))
	}
	return created, nil
}

func (q *Queries) ListTransactions(
	ctx context.Context,
	accountID string,
	cursor *TransactionCursor,
	limit int,
) ([]LedgerEntry, *TransactionCursor, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}

	const firstPage = `
		SELECT ` + ledgerColumns + `
		FROM ledger_entries
		WHERE account_id = $1
		ORDER BY created_at DESC, id DESC
		LIMIT $2`
	const nextPage = `
		SELECT ` + ledgerColumns + `
		FROM ledger_entries
		WHERE account_id = $1
		  AND (created_at, id) < ($2, $3)
		ORDER BY created_at DESC, id DESC
		LIMIT $4`

	var (
		rows pgx.Rows
		err  error
	)
	if cursor == nil {
		rows, err = q.db.Query(ctx, firstPage, accountID, limit+1)
	} else {
		rows, err = q.db.Query(ctx, nextPage, accountID, cursor.CreatedAt, cursor.ID, limit+1)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("list transactions: %w", err)
	}
	defer rows.Close()

	entries := make([]LedgerEntry, 0, limit)
	for rows.Next() {
		entry, scanErr := scanLedgerEntry(rows)
		if scanErr != nil {
			return nil, nil, fmt.Errorf("scan transaction: %w", scanErr)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate transactions: %w", err)
	}

	var next *TransactionCursor
	if len(entries) > limit {
		entries = entries[:limit]
		last := entries[len(entries)-1]
		next = &TransactionCursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	return entries, next, nil
}

func scanLedgerEntry(row pgx.Row) (LedgerEntry, error) {
	var entry LedgerEntry
	err := row.Scan(
		&entry.ID,
		&entry.AccountID,
		&entry.PaymentID,
		&entry.RefundID,
		&entry.OperationID,
		&entry.EntryType,
		&entry.Amount,
		&entry.BalanceAfter,
		&entry.CreatedAt,
	)
	return entry, err
}
