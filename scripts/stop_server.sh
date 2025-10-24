#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "Stopping all PostgreSQL servers..."

# Stop postgres-only (port 5003)
if [ -d "postgres-only" ]; then
    ./postgres-only/pgsql/bin/pg_ctl -D ./postgres-only/pgsql/data stop 2>/dev/null || true
    echo "  - postgres-only stopped"
fi

# Stop postgres-pgd (port 5002)
if [ -d "postgres-pgd" ]; then
    ./postgres-pgd/pgsql/bin/pg_ctl -D ./postgres-pgd/pgsql/data stop 2>/dev/null || true
    echo "  - postgres-pgd stopped"
fi

# Stop duckdb-pg_ext (port 5001)
if [ -d "duckdb-pg_ext" ]; then
    ./duckdb-pg_ext/pgsql/bin/pg_ctl -D ./duckdb-pg_ext/pgsql/data stop 2>/dev/null || true
    echo "  - duckdb-pg_ext stopped"
fi

# Stop vista (port 5004)
if [ -d "vista" ]; then
    ./vista/pgsql/bin/pg_ctl -D ./vista/pgsql/data stop 2>/dev/null || true
    echo "  - vista stopped"
fi

echo "All servers stopped."
