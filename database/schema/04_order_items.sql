-- =============================================================
-- TABLE: order_items
-- Description:
-- Order line items
-- Stores historical purchase price snapshot
-- =============================================================

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id  SERIAL PRIMARY KEY,
    order_id       INT NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id     INT NOT NULL REFERENCES products(product_id),
    quantity       INT NOT NULL CHECK (quantity > 0),
    unit_price     NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    total_amount   NUMERIC(14,2)
        GENERATED ALWAYS AS (quantity * unit_price) STORED
);

CREATE INDEX IF NOT EXISTS idx_order_items_order
    ON order_items(order_id);

CREATE INDEX IF NOT EXISTS idx_order_items_product
    ON order_items(product_id);

COMMENT ON TABLE order_items IS 'Detailed products purchased per order';
COMMENT ON COLUMN order_items.unit_price IS 'Historical product price snapshot at purchase time';