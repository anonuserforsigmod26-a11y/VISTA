#!/bin/bash

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EVAL_ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$SCRIPT_DIR"

# Configuration
DATA_SOURCE=""
BASELINE=""
ITERATION=3

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --data)
            DATA_SOURCE="$2"
            shift 2
            ;;
        --baseline)
            BASELINE="$2"
            shift 2
            ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
done

# Validate arguments
if [ -z "$DATA_SOURCE" ]; then
    echo "Error: --data argument is required"
    echo "Usage: $0 --data <data-directory> --baseline <pgonly|duckdb_ext|pg_duckdb|vista>"
    echo "Example: $0 --data /path/to/data --baseline pgonly"
    exit 1
fi

if [ -z "$BASELINE" ]; then
    echo "Error: --baseline argument is required"
    echo "Usage: $0 --data <data-directory> --baseline <pgonly|duckdb_ext|pg_duckdb|vista>"
    echo "Example: $0 --data /path/to/data --baseline pgonly"
    exit 1
fi

# Validate baseline value
case $BASELINE in
    pgonly|duckdb_ext|pg_duckdb|vista)
        ;;
    *)
        echo "Error: Invalid baseline value '$BASELINE'"
        echo "Valid values: pgonly, duckdb_ext, pg_duckdb, vista"
        exit 1
        ;;
esac

# Convert DATA_SOURCE to absolute path if it's relative
if [[ ! "$DATA_SOURCE" = /* ]]; then
    DATA_SOURCE="$(cd "$(dirname "$DATA_SOURCE")" && pwd)/$(basename "$DATA_SOURCE")"
fi

if [ $BASELINE == "pgonly" ]; then
    BASELINE_PG_DIR="$EVAL_ROOT_DIR/postgres-only/pgsql/data"
    PGCONF="$SCRIPT_DIR/pgconf/postgresql_vanilla.conf"
elif [ $BASELINE == "duckdb_ext" ]; then
    BASELINE_PG_DIR="$EVAL_ROOT_DIR/duckdb-pg_ext/pgsql/data"
    PGCONF="$SCRIPT_DIR/pgconf/postgresql_duckdb_ext.conf"
elif [ $BASELINE == "pg_duckdb" ]; then
    BASELINE_PG_DIR="$EVAL_ROOT_DIR/postgres-pgd/pgsql/data"
    PGCONF="$SCRIPT_DIR/pgconf/postgresql_pgd.conf"
elif [ $BASELINE == "vista" ]; then
    BASELINE_PG_DIR="$EVAL_ROOT_DIR/vista/pgsql/data"
    PGCONF="$SCRIPT_DIR/pgconf/postgresql_vista.conf"
else
    echo "Unknown baseline: $BASELINE"
    exit 1
fi

# Create results directory with timestamp
TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
mkdir -p "$SCRIPT_DIR/${BASELINE}_$TIMESTAMP"

# Array of OLTP worker counts
WORKER_COUNTS=(12)

for n_worker in "${WORKER_COUNTS[@]}"; do
    echo "Running with $n_worker OLTP workers..."

    # 0. Stop server
    (cd "$EVAL_ROOT_DIR/scripts" && ./stop_server.sh)

    # 1. Remove old data directory and copy new one
    rm -rf "$BASELINE_PG_DIR"
    cp -r "$DATA_SOURCE" "$BASELINE_PG_DIR"

    # 2. Copy postgres_only config.
    cp "$PGCONF" "$BASELINE_PG_DIR/postgresql.conf"

    # 3. Drop caches
    (cd "$EVAL_ROOT_DIR/evaluation/.config_scripts" && ./drop_cache.sh)

    # 4. Start server
    if [ $BASELINE == "vista" ]; then
        (cd "$EVAL_ROOT_DIR/scripts" && \
            cgexec -g 'cpu,cpuset,memory:vista_nohp' ./start_server.sh --baseline vista)
    elif [ $BASELINE == "pg_duckdb" ]; then
        (cd "$EVAL_ROOT_DIR/scripts" && \
            cgexec -g 'cpu,cpuset,memory:vista_base' ./start_server.sh --baseline postgres-pgd)
    elif [ $BASELINE == "duckdb_ext" ]; then
        (cd "$EVAL_ROOT_DIR/scripts" && \
            cgexec -g 'cpu,cpuset,memory:vista_base' ./start_server.sh --baseline duckdb-pg_ext)
    elif [ $BASELINE == "pgonly" ]; then
        (cd "$EVAL_ROOT_DIR/scripts" && \
            cgexec -g 'cpu,cpuset,memory:vista_base' ./start_server.sh --baseline postgres-only)
    else
        echo "Unknown baseline: $BASELINE"
        exit 1
    fi

    # Wait for startup
    sleep 5

    # 5. Send dummy.sql to vista server
    if [ $BASELINE == "vista" ]; then
        echo "Sending dummy.sql to vista server..."
        "$EVAL_ROOT_DIR/vista/pgsql/bin/psql" -h localhost -p 5004 -U vista -d chbenchmark -f "$SCRIPT_DIR/dummy.sql"
    fi

    # 6. Run HTAP workload
    (cd "$SCRIPT_DIR" && \
    ./experiment.py \
        --baseline $BASELINE \
        --iteration $ITERATION \
        --tpcc-worker $n_worker \
        --output "$SCRIPT_DIR/${BASELINE}_$TIMESTAMP/olap_isol_${n_worker}.json")

    echo "Completed run with $n_worker OLTP workers"
    echo "----------------------------------------"
done

echo "All runs completed!"
