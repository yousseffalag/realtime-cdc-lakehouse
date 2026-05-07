-- =============================================================
-- TABLE: payments
-- Description:
-- Payment transactions linked to orders
-- =============================================================

CREATE TABLE IF NOT EXISTS payments (
    payment_id      SERIAL PRIMARY KEY,
    order_id        INT NOT NULL REFERENCES orders(order_id),
    payment_date    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payment_method  VARCHAR(50) NOT NULL,
    amount          NUMERIC(14,2) NOT NULL CHECK (amount >= 0),
    payment_status  VARCHAR(50) NOT NULL DEFAULT 'completed'
);

CREATE INDEX IF NOT EXISTS idx_payments_order
    ON payments(order_id);

CREATE INDEX IF NOT EXISTS idx_payments_method
    ON payments(payment_method);

COMMENT ON TABLE payments IS 'Payment transactions for customer orders';
COMMENT ON COLUMN payments.payment_method IS 'card | paypal | cash | bank_transfer';