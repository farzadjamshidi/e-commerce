-- The orchestrator writes to several topics, so each row names its own.
-- The default only fills existing rows: dropping it makes every new write choose a topic.
ALTER TABLE outbox_events ADD COLUMN destination VARCHAR(128) NOT NULL DEFAULT 'orders';
ALTER TABLE outbox_events ALTER COLUMN destination DROP DEFAULT;
