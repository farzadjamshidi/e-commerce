-- W3C trace context of the transaction that wrote the row. Debezium copies it to a Kafka header.
ALTER TABLE outbox_events ADD COLUMN traceparent VARCHAR(64);
