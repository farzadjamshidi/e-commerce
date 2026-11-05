-- The timeout job looks for sagas that stayed too long in one state
CREATE INDEX idx_order_sagas_state_updated_at ON order_sagas (state, updated_at);
