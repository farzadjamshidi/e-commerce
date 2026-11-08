-- The context of the request that started the saga. A timeout has no request of its own, so it continues this trace.
ALTER TABLE order_sagas ADD COLUMN traceparent VARCHAR(64);
