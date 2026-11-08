-- Orders the saga has asked to refund. A charge for one of them must never take money,
-- even when it arrives after the refund, for example replayed by hand from the DLT.
CREATE TABLE voided_orders (
    order_id  UUID        PRIMARY KEY,
    voided_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
