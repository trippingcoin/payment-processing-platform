CREATE SEQUENCE outbox_sequence;
ALTER TABLE outbox_events
    ADD COLUMN sequence_id bigint,
    ADD COLUMN next_attempt_at timestamptz NOT NULL DEFAULT now();

-- Preserve the existing queue order when upgrading a populated database.
WITH ordered AS (
    SELECT id, row_number() OVER (ORDER BY created_at, id) AS position
    FROM outbox_events
)
UPDATE outbox_events e SET sequence_id = o.position FROM ordered o WHERE e.id = o.id;
SELECT setval('outbox_sequence', COALESCE(MAX(sequence_id), 0) + 1, false) FROM outbox_events;
ALTER SEQUENCE outbox_sequence OWNED BY outbox_events.sequence_id;
ALTER TABLE outbox_events
    ALTER COLUMN sequence_id SET DEFAULT nextval('outbox_sequence'),
    ALTER COLUMN sequence_id SET NOT NULL;
CREATE UNIQUE INDEX outbox_sequence_idx ON outbox_events (sequence_id);
CREATE INDEX outbox_aggregate_pending_idx
    ON outbox_events (aggregate_type, aggregate_id, sequence_id)
    WHERE published_at IS NULL;
