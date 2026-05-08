#!/bin/bash
set -e

echo "Starting CDC database initialization..."

DB_NAME=${POSTGRES_DB:-cdc_db}
DB_USER=${POSTGRES_USER:-cdc_user}

run_sql_dir () {
  DIR=$1
  LABEL=$2

  if [ -d "$DIR" ]; then
    echo "Running $LABEL scripts from $DIR"

    for file in $(ls "$DIR"/*.sql 2>/dev/null | sort); do
      echo "Executing: $file"
      psql -U "$DB_USER" -d "$DB_NAME" -f "$file"
    done
  else
    echo "Directory $DIR not found, skipping..."
  fi
}

# =========================================================
# 1. ROLES (must be first)
# =========================================================
run_sql_dir "/database/roles" "ROLE SETUP"

# =========================================================
# 2. SCHEMA (tables creation)
# =========================================================
run_sql_dir "/database/schema" "SCHEMA"

# =========================================================
# 3. MIGRATIONS (publication, replication, CDC infra)
# =========================================================
run_sql_dir "/database/migrations" "MIGRATIONS"

# =========================================================
# 4. SEEDS (data insertion)
# =========================================================
run_sql_dir "/database/seeds" "SEEDS"

echo "Database initialization completed successfully."