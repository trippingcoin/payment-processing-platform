DROP INDEX outbox_aggregate_pending_idx;
DROP INDEX outbox_sequence_idx;
ALTER TABLE outbox_events DROP COLUMN next_attempt_at, DROP COLUMN sequence_id;
