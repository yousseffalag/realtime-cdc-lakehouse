-- =========================================================
-- CREATE DEBEZIUM USER
-- =========================================================

CREATE ROLE debezium WITH
LOGIN
PASSWORD 'dbz'
REPLICATION;

-- =========================================================
-- DATABASE ACCESS
-- =========================================================

GRANT CONNECT ON DATABASE cdc_db TO debezium;

-- =========================================================
-- SCHEMA ACCESS
-- =========================================================

\c cdc_db

GRANT USAGE ON SCHEMA public TO debezium;
GRANT CREATE ON SCHEMA public TO debezium;

-- =========================================================
-- TABLE PRIVILEGES
-- =========================================================

GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;

-- =========================================================
-- OWNERSHIP OF TABLES
-- =========================================================

ALTER TABLE customers   OWNER TO debezium;
ALTER TABLE products    OWNER TO debezium;
ALTER TABLE orders      OWNER TO debezium;
ALTER TABLE order_items OWNER TO debezium;
ALTER TABLE payments    OWNER TO debezium;
ALTER TABLE returns     OWNER TO debezium;

-- =========================================================
-- DEFAULT PRIVILEGES FOR FUTURE TABLES
-- =========================================================

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO debezium;