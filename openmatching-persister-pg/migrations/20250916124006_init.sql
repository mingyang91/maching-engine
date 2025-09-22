-- Clean, simple, separated tables for Buy and Sell orders

-- Buy orders table - tracks MONEY
CREATE TABLE buy_orders(
    key uuid PRIMARY KEY,
    status SMALLINT NOT NULL,           -- 0=Open, 1=PartiallyFilled, 2=Filled, 3=Cancelled
    order_type SMALLINT NOT NULL,       -- 0=Limit, 1=Market
    limit_price REAL NOT NULL,          -- Max price per unit (0 for market orders)
    total_funds REAL NOT NULL,          -- Total money allocated for buying
    funds_remaining REAL NOT NULL,      -- Money still available
    target_quantity BIGINT NOT NULL,    -- Desired quantity to buy
    filled_quantity BIGINT NOT NULL DEFAULT 0, -- Quantity already bought
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Sell orders table - tracks SHARES
CREATE TABLE sell_orders(
    key uuid PRIMARY KEY,
    status SMALLINT NOT NULL,           -- 0=Open, 1=PartiallyFilled, 2=Filled, 3=Cancelled
    order_type SMALLINT NOT NULL,       -- 0=Limit, 1=Market
    limit_price REAL NOT NULL,          -- Min price per unit (0 for market orders)
    total_quantity BIGINT NOT NULL,     -- Total shares to sell
    remaining_quantity BIGINT NOT NULL, -- Shares still available to sell
    total_proceeds REAL NOT NULL DEFAULT 0, -- Money earned from sales
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for buy orders (sorted high to low for matching)
CREATE INDEX idx_buy_orders_active ON buy_orders(status)
WHERE status IN (0, 1); -- Open or PartiallyFilled

CREATE INDEX idx_buy_orders_matching ON buy_orders(limit_price DESC, created_at)
WHERE status IN (0, 1);

-- Indexes for sell orders (sorted low to high for matching)
CREATE INDEX idx_sell_orders_active ON sell_orders(status)
WHERE status IN (0, 1); -- Open or PartiallyFilled

CREATE INDEX idx_sell_orders_matching ON sell_orders(limit_price ASC, created_at)
WHERE status IN (0, 1);

-- No triggers needed - keep it simple
-- The application can update timestamps if needed