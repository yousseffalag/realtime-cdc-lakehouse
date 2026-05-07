-- =============================================================
-- TABLE: products
-- Description:
-- Product catalog (CDC-tracked)
-- =============================================================

CREATE TABLE IF NOT EXISTS products (
    product_id   SERIAL PRIMARY KEY,
    category     VARCHAR(100) NOT NULL,
    brand        VARCHAR(100),
    unit_price   NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    stock_qty    INT NOT NULL DEFAULT 0 CHECK (stock_qty >= 0),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_products_category
    ON products(category);

CREATE INDEX IF NOT EXISTS idx_products_brand
    ON products(brand);

COMMENT ON TABLE products IS 'Product catalog and inventory';
COMMENT ON COLUMN products.stock_qty IS 'Current available inventory quantity';