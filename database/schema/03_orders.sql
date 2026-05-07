-- =============================================================
-- TABLE: orders
-- Description:
-- Order header information
-- One order can contain multiple order_items
-- =============================================================

CREATE TABLE IF NOT EXISTS orders (
    order_id      SERIAL PRIMARY KEY,
    customer_id   INT NOT NULL REFERENCES customers(customer_id),
    order_date    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status        VARCHAR(50) NOT NULL DEFAULT 'pending',
    channel       VARCHAR(50) NOT NULL DEFAULT 'web',
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_customer
    ON orders(customer_id);

CREATE INDEX IF NOT EXISTS idx_orders_status
    ON orders(status);

CREATE INDEX IF NOT EXISTS idx_orders_date
    ON orders(order_date DESC);

COMMENT ON TABLE orders IS 'Order header transactions';
COMMENT ON COLUMN orders.status IS 'pending | confirmed | shipped | delivered | cancelled';
COMMENT ON COLUMN orders.channel IS 'web | mobile App | store | B2B';