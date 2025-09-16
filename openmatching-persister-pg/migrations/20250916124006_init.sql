-- Add migration script here
CREATE TABLE all_orders(
    key uuid PRIMARY KEY,
    price REAL NOT NULL,
    quantity BIGINT NOT NULL,
    remaining BIGINT NOT NULL,
    side SMALLINT NOT NULL,
    status SMALLINT NOT NULL,
    order_type SMALLINT NOT NULL
);

CREATE INDEX idx_all_orders_status_side ON all_orders(status, side)
WHERE status IN (0, 1); -- Open or PartiallyFilled