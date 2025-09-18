-- Add down migration script here
DROP TABLE transactions;
DROP TYPE transaction_status;
DROP TYPE transaction_type;
DROP TABLE accounts;