#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --baseline)
            BASELINE="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 --baseline <postgres-only|postgres-pgd|duckdb-pg_ext|vista>"
            exit 1
            ;;
    esac
done

# If BASELINE is not set, alert and exit
if [[ -z "$BASELINE" ]]; then
    echo "Error: --baseline argument is required."
    echo "Usage: $0 --baseline <postgres-only|postgres-pgd|duckdb-pg_ext|vista>"
    exit 1
fi

if [[ "$BASELINE" == "vista" ]]; then
    echo "Starting VISTA PostgreSQL server..."
    PG_PORT=5004
    DATA_DIR="./vista/pgsql/data"
    BIN_DIR="./vista/pgsql/bin"
elif [[ "$BASELINE" == "postgres-only" ]]; then
    echo "Starting postgres-only PostgreSQL server..."
    PG_PORT=5003
    DATA_DIR="./postgres-only/pgsql/data"
    BIN_DIR="./postgres-only/pgsql/bin"
elif [[ "$BASELINE" == "postgres-pgd" ]]; then
    echo "Starting postgres-pgd (pg_duckdb) PostgreSQL server..."
    PG_PORT=5002
    DATA_DIR="./postgres-pgd/pgsql/data"
    BIN_DIR="./postgres-pgd/pgsql/bin"
    # Set LD_LIBRARY_PATH for pg_duckdb
    export LD_LIBRARY_PATH=$(pwd)/postgres-pgd/pgsql/lib:$LD_LIBRARY_PATH
elif [[ "$BASELINE" == "duckdb-pg_ext" ]]; then
    echo "Starting duckdb-pg_ext PostgreSQL server..."
    PG_PORT=5001
    DATA_DIR="./duckdb-pg_ext/pgsql/data"
    BIN_DIR="./duckdb-pg_ext/pgsql/bin"
else
    echo "Invalid baseline option: $BASELINE"
    echo "Usage: $0 --baseline <postgres-only|postgres-pgd|duckdb-pg_ext|vista>"
    exit 1
fi

# Start the PostgreSQL server
$BIN_DIR/pg_ctl -D $DATA_DIR -l $DATA_DIR/../logfile start
echo "PostgreSQL server started on port $PG_PORT with data directory $DATA_DIR"
