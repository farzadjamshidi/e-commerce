-- Only the outbox leaves this database through CDC, not the orders table
CREATE PUBLICATION outbox_publication FOR TABLE outbox_events;
GRANT SELECT ON outbox_events TO debezium;
