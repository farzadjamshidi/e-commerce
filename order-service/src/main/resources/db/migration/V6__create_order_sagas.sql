-- The orchestrator's memory: one row per order, saying which step the saga is waiting for
CREATE TABLE order_sagas (
    order_id   UUID        PRIMARY KEY REFERENCES orders (id),
    state      VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
