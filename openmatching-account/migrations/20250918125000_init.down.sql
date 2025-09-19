-- Rollback account system tables and types
DROP TABLE IF EXISTS transactions;
DROP TYPE IF EXISTS transaction_status;
DROP TYPE IF EXISTS transaction_kind;
DROP TABLE IF EXISTS accounts;