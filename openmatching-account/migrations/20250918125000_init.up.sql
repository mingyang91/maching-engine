-- Add up migration script here
-- Add migration script here

CREATE TABLE accounts(
    username VARCHAR(255) NOT NULL,
    asset VARCHAR(255) NOT NULL,
    balance BIGINT NOT NULL CHECK (balance >= 0),
    pending_credit BIGINT NOT NULL CHECK (pending_credit >= 0),
    pending_debit BIGINT NOT NULL CHECK (pending_debit >= 0),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (username, asset)
);

CREATE TYPE transaction_type AS ENUM ('transfer', 'deposit', 'withdrawal', 'fee');
CREATE TYPE transaction_status AS ENUM ('pending', 'completed', 'failed');

CREATE TABLE transactions(
    id UUID NOT NULL,
    type transaction_type NOT NULL,
    debitor VARCHAR(255) NOT NULL,
    creditor VARCHAR(255) NOT NULL,
    asset VARCHAR(255) NOT NULL,
    amount BIGINT NOT NULL CHECK (amount > 0),
    status transaction_status NOT NULL,
    extra JSONB,
    external_id VARCHAR(255) UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, type)
);
