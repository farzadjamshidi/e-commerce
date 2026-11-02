-- One reservation per order, so it can be released for that order alone
CREATE TABLE reservations (
    order_id UUID PRIMARY KEY,
    sku VARCHAR(64) NOT NULL REFERENCES stock (sku),
    quantity INT NOT NULL CHECK (quantity > 0),
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Earlier reservations have no owning row and could never be released, so give them back
UPDATE stock SET available = available + reserved, reserved = 0, updated_at = now();
