-- Account system with improved naming conventions

CREATE TABLE accounts(
    username VARCHAR(255) NOT NULL,
    asset VARCHAR(255) NOT NULL,
    balance NUMERIC NOT NULL CHECK (balance >= 0),
    reserved NUMERIC NOT NULL CHECK (reserved >= 0),  -- funds reserved for pending withdrawals  
    incoming NUMERIC NOT NULL CHECK (incoming >= 0),  -- funds tracked for pending deposits
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (username, asset)
);

CREATE TYPE transaction_kind AS ENUM ('transfer', 'deposit', 'withdrawal', 'fee');
CREATE TYPE transaction_status AS ENUM ('pending', 'completed', 'failed');

CREATE TABLE transactions(
    id UUID PRIMARY KEY,  -- simplified: id alone is unique
    kind transaction_kind NOT NULL,
    sender VARCHAR(255) NOT NULL,     -- was: debitor
    receiver VARCHAR(255) NOT NULL,   -- was: creditor  
    asset VARCHAR(255) NOT NULL,
    amount NUMERIC NOT NULL CHECK (amount > 0),
    status transaction_status NOT NULL,
    extra JSONB,
    external_id VARCHAR(255) UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
