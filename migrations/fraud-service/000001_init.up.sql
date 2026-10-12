CREATE TABLE fraud_decisions (
    payment_id uuid PRIMARY KEY,
    client_id uuid NOT NULL,
    account_id uuid NOT NULL,
    amount_minor bigint NOT NULL CHECK (amount_minor > 0),
    currency varchar(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    source_event_id uuid NOT NULL UNIQUE,
    status text NOT NULL CHECK (status IN ('approved', 'declined')),
    reason text NOT NULL,
    rule_version text NOT NULL,
    max_amount_minor bigint NOT NULL CHECK (max_amount_minor > 0),
    rule_currency varchar(3) NOT NULL,
    decided_at timestamptz NOT NULL
);

CREATE TABLE consumer_inbox (
    consumer_name varchar(128) NOT NULL,
    event_id uuid NOT NULL,
    processed_at timestamptz NOT NULL,
    PRIMARY KEY (consumer_name, event_id)
);

-- Same relay contract as Payment's Outbox; separate database and migrations.
CREATE TABLE outbox_events (
    id uuid PRIMARY KEY,
    aggregate_type varchar(64) NOT NULL,
    aggregate_id uuid NOT NULL,
    event_type varchar(128) NOT NULL,
    payload jsonb NOT NULL,
    headers jsonb NOT NULL DEFAULT '{}'::jsonb,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    sequence_id bigserial NOT NULL UNIQUE,
    next_attempt_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX outbox_aggregate_pending_idx ON outbox_events (aggregate_type, aggregate_id, sequence_id)
    WHERE published_at IS NULL;
