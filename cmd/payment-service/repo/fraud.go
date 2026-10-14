package repo

import "context"

// IsPaymentCreatedEvent binds a decision to an event actually produced here.
// Retain payment.created Outbox rows while fraud decisions can be replayed.
func (q *Queries) IsPaymentCreatedEvent(ctx context.Context, eventID, paymentID string) (bool, error) {
	var found bool
	err := q.db.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM outbox_events
		WHERE id=$1 AND aggregate_id=$2 AND aggregate_type='payment' AND event_type='payment.created')`, eventID, paymentID).Scan(&found)
	return found, err
}
