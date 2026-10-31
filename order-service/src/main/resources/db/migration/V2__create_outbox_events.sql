CREATE TABLE outbox_events (
    id             UUID         PRIMARY KEY,
    position       BIGINT       GENERATED ALWAYS AS IDENTITY UNIQUE,
    aggregate_type VARCHAR(64)  NOT NULL,
    aggregate_id   VARCHAR(64)  NOT NULL,
    event_type     VARCHAR(64)  NOT NULL,
    payload        JSONB        NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ
);

-- The relay only looks for unpublished rows, so the index covers only those
CREATE INDEX idx_outbox_events_unpublished ON outbox_events (position) WHERE published_at IS NULL;
