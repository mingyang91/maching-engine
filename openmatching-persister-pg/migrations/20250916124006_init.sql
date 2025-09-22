-- Single table for atomic operations with clean Buy/Sell separation
-- Best of both worlds: atomicity + performance + type safety

CREATE TABLE orders(
    key uuid PRIMARY KEY,
    side SMALLINT NOT NULL,             -- 0=BUY, 1=SELL
    status SMALLINT NOT NULL,           -- 0=Open, 1=PartiallyFilled, 2=Filled, 3=Cancelled
    order_type SMALLINT NOT NULL,       -- 0=Limit, 1=Market
    
    -- Common price field (different meaning for buy vs sell)
    limit_price REAL NOT NULL,          -- Buy: max price, Sell: min price, 0 for market
    
    -- Buy-specific fields (used when side=0)
    total_funds REAL,                   -- Total money allocated for buying
    funds_remaining REAL,               -- Money still available
    target_quantity BIGINT,             -- Desired quantity to buy
    filled_quantity BIGINT,             -- Quantity already bought
    
    -- Sell-specific fields (used when side=1)
    total_quantity BIGINT,              -- Total shares to sell
    remaining_quantity BIGINT,          -- Shares still available
    total_proceeds REAL,                -- Money earned from sales
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Ensure data integrity
    CONSTRAINT chk_buy_fields CHECK (
        side != 0 OR (total_funds IS NOT NULL AND funds_remaining IS NOT NULL 
                      AND target_quantity IS NOT NULL AND filled_quantity IS NOT NULL)
    ),
    CONSTRAINT chk_sell_fields CHECK (
        side != 1 OR (total_quantity IS NOT NULL AND remaining_quantity IS NOT NULL 
                      AND total_proceeds IS NOT NULL)
    )
);

-- Indexes for buy orders (high to low price for matching)
CREATE INDEX idx_orders_buy_active ON orders(status)
WHERE side = 0 AND status IN (0, 1);

CREATE INDEX idx_orders_buy_matching ON orders(limit_price DESC, created_at)
WHERE side = 0 AND status IN (0, 1);

-- Indexes for sell orders (low to high price for matching)
CREATE INDEX idx_orders_sell_active ON orders(status)
WHERE side = 1 AND status IN (0, 1);

CREATE INDEX idx_orders_sell_matching ON orders(limit_price ASC, created_at)
WHERE side = 1 AND status IN (0, 1);