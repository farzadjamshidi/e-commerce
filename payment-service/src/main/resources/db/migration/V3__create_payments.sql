-- One payment attempt per order; the primary key also blocks a second charge for the same order
CREATE TABLE payments (
    order_id    UUID          PRIMARY KEY,
    customer_id VARCHAR(64)   NOT NULL,
    amount      NUMERIC(12,2) NOT NULL CHECK (amount >= 0),
    currency    VARCHAR(3)    NOT NULL,
    status      VARCHAR(32)   NOT NULL,
    reason      VARCHAR(64),
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT now()
);
