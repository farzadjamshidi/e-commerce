-- The timeout job only looks for old PENDING orders; finished orders never enter this index
CREATE INDEX idx_orders_pending_created_at ON orders (created_at) WHERE status = 'PENDING';
