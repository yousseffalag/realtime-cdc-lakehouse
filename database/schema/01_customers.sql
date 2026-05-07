-- =============================================================
-- TABLE: customers
-- Description:
-- Customer master data (CDC-tracked)
-- =============================================================

CREATE TABLE IF NOT EXISTS customers (
    customer_id       SERIAL PRIMARY KEY,
    full_name         VARCHAR(255) NOT NULL,
    city              VARCHAR(100),
    segment           VARCHAR(50) NOT NULL DEFAULT 'regular',
    registration_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_customers_city
    ON customers(city);

CREATE INDEX IF NOT EXISTS idx_customers_segment
    ON customers(segment);

COMMENT ON TABLE customers IS 'Customer master records';
COMMENT ON COLUMN customers.segment IS 'Customer segmentation: regular, vip, enterprise, etc.';