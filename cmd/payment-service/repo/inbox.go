package repo

import (
	"context"
	"fmt"
	"time"
)

func (q *Queries) TryBeginInboxEvent(
	ctx context.Context,
	consumer string,
	eventID string,
	processedAt time.Time,
) (bool, error) {
	const statement = `
		INSERT INTO consumer_inbox (consumer_name, event_id, processed_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (consumer_name, event_id) DO NOTHING`

	tag, err := q.db.Exec(ctx, statement, consumer, eventID, processedAt)
	if err != nil {
		return false, fmt.Errorf("begin inbox event: %w", mapPostgresError(err))
	}
	return tag.RowsAffected() == 1, nil
}
