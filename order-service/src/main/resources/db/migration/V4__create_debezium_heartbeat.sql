-- Debezium updates this row periodically, so the slot always has a recent change in order_db to confirm
CREATE TABLE debezium_heartbeat (
    id INT PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL
);
INSERT INTO debezium_heartbeat (id, ts) VALUES (1, now());
GRANT SELECT, UPDATE ON debezium_heartbeat TO debezium;
ALTER PUBLICATION outbox_publication ADD TABLE debezium_heartbeat;
