-- No relay here, only CDC: rows are deleted in the same transaction, so no position or published_at
CREATE TABLE outbox_events (
    id             UUID         PRIMARY KEY,
    aggregate_type VARCHAR(64)  NOT NULL,
    aggregate_id   VARCHAR(64)  NOT NULL,
    event_type     VARCHAR(64)  NOT NULL,
    payload        JSONB        NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE debezium_heartbeat (
    id INT PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL
);
INSERT INTO debezium_heartbeat (id, ts) VALUES (1, now());

CREATE PUBLICATION outbox_publication FOR TABLE outbox_events, debezium_heartbeat;
GRANT SELECT ON outbox_events TO debezium;
GRANT SELECT, UPDATE ON debezium_heartbeat TO debezium;
