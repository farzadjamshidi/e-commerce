-- One database and one user per service. A service cannot connect to another service's database.
CREATE USER order_service WITH PASSWORD 'order_service';
CREATE DATABASE order_db OWNER order_service;
REVOKE CONNECT ON DATABASE order_db FROM PUBLIC;

CREATE USER inventory_service WITH PASSWORD 'inventory_service';
CREATE DATABASE inventory_db OWNER inventory_service;
REVOKE CONNECT ON DATABASE inventory_db FROM PUBLIC;
