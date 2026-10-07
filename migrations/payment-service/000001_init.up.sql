CREATE TABLE accounts (
    id                uuid PRIMARY KEY,
    user_id           uuid NOT NULL,
    currency          varchar(3) NOT NULL,
    available_balance bigint NOT NULL DEFAULT 0,
    version           bigint NOT NULL DEFAULT 0,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT accounts_balance_non_negative CHECK (available_balance >= 0),
    CONSTRAINT accounts_currency_format CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT accounts_user_currency_unique UNIQUE (user_id, currency)
);

CREATE TABLE payments (
    id           uuid PRIMARY KEY,
    client_id    uuid NOT NULL,
    account_id   uuid NOT NULL REFERENCES accounts(id),
    amount       bigint NOT NULL,
    currency     varchar(3) NOT NULL,
    status       varchar(16) NOT NULL,
    failure_code varchar(64),
    description  varchar(500),
    version      bigint NOT NULL DEFAULT 0,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT payments_amount_positive CHECK (amount > 0),
    CONSTRAINT payments_currency_format CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT payments_status_valid CHECK (
        status IN ('pending', 'processing', 'completed', 'failed')
    ),
    CONSTRAINT payments_failure_consistent CHECK (
        (status = 'failed' AND failure_code IS NOT NULL)
        OR (status <> 'failed' AND failure_code IS NULL)
    )
);

CREATE INDEX payments_account_history_idx
    ON payments (account_id, created_at DESC, id DESC);

CREATE INDEX payments_client_history_idx
    ON payments (client_id, created_at DESC, id DESC);

CREATE INDEX payments_status_created_idx
    ON payments (status, created_at)
    WHERE status IN ('pending', 'processing');

CREATE TABLE refunds (
    id           uuid PRIMARY KEY,
    payment_id   uuid NOT NULL REFERENCES payments(id),
    client_id    uuid NOT NULL,
    amount       bigint NOT NULL,
    status       varchar(16) NOT NULL,
    failure_code varchar(64),
    version      bigint NOT NULL DEFAULT 0,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT refunds_amount_positive CHECK (amount > 0),
    CONSTRAINT refunds_status_valid CHECK (
        status IN ('pending', 'processing', 'completed', 'failed')
    ),
    CONSTRAINT refunds_failure_consistent CHECK (
        (status = 'failed' AND failure_code IS NOT NULL)
        OR (status <> 'failed' AND failure_code IS NULL)
    )
);

CREATE INDEX refunds_payment_idx
    ON refunds (payment_id, created_at DESC, id DESC);

CREATE TABLE ledger_entries (
    id            uuid PRIMARY KEY,
    account_id    uuid NOT NULL REFERENCES accounts(id),
    payment_id    uuid REFERENCES payments(id),
    refund_id     uuid REFERENCES refunds(id),
    operation_id  uuid NOT NULL,
    entry_type    varchar(8) NOT NULL,
    amount        bigint NOT NULL,
    balance_after bigint NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ledger_entry_type_valid CHECK (entry_type IN ('debit', 'credit')),
    CONSTRAINT ledger_amount_positive CHECK (amount > 0),
    CONSTRAINT ledger_balance_non_negative CHECK (balance_after >= 0),
    CONSTRAINT ledger_reference_valid CHECK (
        (entry_type = 'debit' AND payment_id IS NOT NULL AND refund_id IS NULL)
        OR (entry_type = 'credit' AND refund_id IS NOT NULL)
    ),
    CONSTRAINT ledger_operation_once UNIQUE (operation_id)
);

CREATE INDEX ledger_account_history_idx
    ON ledger_entries (account_id, created_at DESC, id DESC);

CREATE TABLE idempotency_keys (
    client_id       uuid NOT NULL,
    idempotency_key varchar(255) NOT NULL,
    request_hash    bytea NOT NULL,
    resource_type   varchar(32) NOT NULL,
    resource_id     uuid NOT NULL,
    response_status integer,
    response_body   jsonb,
    created_at      timestamptz NOT NULL DEFAULT now(),
    expires_at      timestamptz NOT NULL,
    PRIMARY KEY (client_id, idempotency_key),
    CONSTRAINT idempotency_key_not_blank CHECK (length(idempotency_key) > 0),
    CONSTRAINT idempotency_expiry_valid CHECK (expires_at > created_at),
    CONSTRAINT idempotency_response_status_valid CHECK (
        response_status IS NULL OR response_status BETWEEN 100 AND 599
    )
);

CREATE INDEX idempotency_expiry_idx ON idempotency_keys (expires_at);

CREATE TABLE outbox_events (
    id             uuid PRIMARY KEY,
    aggregate_type varchar(64) NOT NULL,
    aggregate_id   uuid NOT NULL,
    event_type     varchar(128) NOT NULL,
    payload        jsonb NOT NULL,
    headers        jsonb NOT NULL DEFAULT '{}'::jsonb,
    attempts       integer NOT NULL DEFAULT 0,
    last_error     text,
    created_at     timestamptz NOT NULL DEFAULT now(),
    published_at   timestamptz,
    CONSTRAINT outbox_attempts_non_negative CHECK (attempts >= 0)
);

CREATE INDEX outbox_unpublished_idx
    ON outbox_events (created_at, id)
    WHERE published_at IS NULL;

CREATE TABLE consumer_inbox (
    consumer_name varchar(128) NOT NULL,
    event_id      uuid NOT NULL,
    processed_at  timestamptz NOT NULL,
    PRIMARY KEY (consumer_name, event_id)
);

CREATE INDEX consumer_inbox_retention_idx ON consumer_inbox (processed_at);
