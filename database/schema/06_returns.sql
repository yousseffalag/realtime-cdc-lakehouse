-- =============================================================
-- TABLE: returns
-- Description:
-- Product returns / cancellations
-- =============================================================

CREATE TABLE IF NOT EXISTS returns (
    return_id      SERIAL PRIMARY KEY,
    order_id       INT NOT NULL REFERENCES orders(order_id),
    return_date    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reason         VARCHAR(255) NOT NULL,
    status         VARCHAR(50) NOT NULL DEFAULT 'requested'
);

CREATE INDEX IF NOT EXISTS idx_returns_order
    ON returns(order_id);

CREATE INDEX IF NOT EXISTS idx_returns_status
    ON returns(status);

COMMENT ON TABLE returns IS 'Returns and cancellation events';
COMMENT ON COLUMN returns.reason IS 'defective | customer regret | wrong item | damaged';
COMMENT ON COLUMN returns.status IS 'requested | approved | rejected | completed';