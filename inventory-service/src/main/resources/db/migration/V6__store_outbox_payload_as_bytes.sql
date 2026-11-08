-- An Avro payload is binary. JSON payloads keep their text, as UTF-8 bytes, so consumers see no change.
ALTER TABLE outbox_events ALTER COLUMN payload TYPE BYTEA USING convert_to(payload::text, 'UTF8');
