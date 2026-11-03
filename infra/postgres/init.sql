-- One database and one user per service. A service cannot connect to another service's database.
CREATE USER order_service WITH PASSWORD 'order_service';
CREATE DATABASE order_db OWNER order_service;
REVOKE CONNECT ON DATABASE order_db FROM PUBLIC;

CREATE USER inventory_service WITH PASSWORD 'inventory_service';
CREATE DATABASE inventory_db OWNER inventory_service;
REVOKE CONNECT ON DATABASE inventory_db FROM PUBLIC;

-- Debezium reads the WAL of order_db. Table access is granted by order-service's own migration.
CREATE USER debezium WITH REPLICATION PASSWORD 'debezium';
GRANT CONNECT ON DATABASE order_db TO debezium;
GRANT CONNECT ON DATABASE inventory_db TO debezium;

CREATE USER payment_service WITH PASSWORD 'payment_service';
CREATE DATABASE payment_db OWNER payment_service;
REVOKE CONNECT ON DATABASE payment_db FROM PUBLIC;
GRANT CONNECT ON DATABASE payment_db TO debezium;
