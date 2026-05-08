-- =========================================================
-- DEBEZIUM ROLE (CDC - READ WAL ONLY)
-- PRODUCTION-STYLE SAFE CONFIG
-- =========================================================

-- 1. Create role (NO SUPERUSER, NO OWNERSHIP)
DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'debezium') THEN
      CREATE ROLE debezium WITH
         LOGIN
         PASSWORD 'dbz'
         REPLICATION;
   END IF;
END
$$;

-- =========================================================
-- 2. DATABASE ACCESS
-- =========================================================

GRANT CONNECT ON DATABASE cdc_db TO debezium;

-- =========================================================
-- 3. SCHEMA ACCESS
-- =========================================================

GRANT USAGE ON SCHEMA public TO debezium;

-- =========================================================
-- 4. READ-ONLY TABLE ACCESS (WAL reading requirement)
-- =========================================================

GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;

-- future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO debezium;

-- =========================================================
-- 5. IMPORTANT: PUBLICATION (MANUAL CONTROL - BEST PRACTICE)
-- =========================================================

-- Create publication manually (run once as admin/superuser)
-- CREATE PUBLICATION dbz_publication FOR TABLE customers, orders, payments;

-- =========================================================
-- 6. REPLICATION PRIVILEGE
-- =========================================================

ALTER ROLE debezium WITH REPLICATION;