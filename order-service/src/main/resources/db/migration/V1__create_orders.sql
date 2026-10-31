CREATE TABLE orders (
    id           UUID          PRIMARY KEY,
    customer_id  VARCHAR(64)   NOT NULL,
    sku          VARCHAR(64)   NOT NULL,
    quantity     INT           NOT NULL CHECK (quantity > 0),
    total_amount NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0),
    currency     CHAR(3)       NOT NULL,
    status       VARCHAR(32)   NOT NULL,
    version      BIGINT        NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX idx_orders_customer_id ON orders (customer_id);
