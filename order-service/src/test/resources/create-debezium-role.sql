-- In Docker Compose infra/postgres/init.sql creates this role, and a migration grants it access to the outbox
CREATE ROLE debezium;
