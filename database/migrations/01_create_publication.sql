-- =========================================================
-- POSTGRES LOGICAL REPLICATION PUBLICATION (DEBEZIUM)
-- =========================================================

-- Create publication explicitly (BEST PRACTICE for production CDC)

DO
$$
BEGIN
   IF NOT EXISTS (
      SELECT 1
      FROM pg_publication
      WHERE pubname = 'dbz_publication'
   ) THEN

      CREATE PUBLICATION dbz_publication
      FOR TABLE
         customers,
         products,
         orders,
         order_items,
         payments,
         returns;

   END IF;
END
$$;

-- =========================================================
-- VERIFICATION QUERY (optional manual check)
-- =========================================================

-- SELECT * FROM pg_publication;