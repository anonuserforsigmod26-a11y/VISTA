#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

# Build CH-benCHmark dataset using postgres-only
# --warehouse <num> : number of warehouses to generate (default: 10)

# Default values
NR_WAREHOUSE=10

echo "You can specify --warehouse <num> option."

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --warehouse)
            NR_WAREHOUSE="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--warehouse <num>]"
            exit 1
            ;;
    esac
done

echo "==========================================="
echo "Building CH-benCHmark Dataset"
echo "==========================================="
echo "  Warehouses: $NR_WAREHOUSE"
echo "  Using: postgres-only (port 5003)"
echo ""

sleep 1

# Check if postgres-only exists
if [ ! -d "postgres-only" ]; then
    echo "Error: 'postgres-only' directory not found. Please run setup_pgonly.sh first."
    exit 1
fi

PG_PORT=5003
PSQL_CMD="./postgres-only/pgsql/bin/psql -p $PG_PORT -U vista -d vista"
DATA_DIR="./postgres-only/pgsql/data"

# Increase max_connections for dataset generation
echo "Configuring PostgreSQL for dataset generation..."
sed -i "s/^#*max_connections.*/max_connections = 600/" $DATA_DIR/postgresql.conf

# Start PostgreSQL server
echo "Starting PostgreSQL server..."
./postgres-only/pgsql/bin/pg_ctl -D $DATA_DIR -l ./postgres-only/pgsql/logfile start

echo "Waiting for PostgreSQL to start..."
sleep 3

# Download and build CH-benCHmark dataset
if [ -d "citus-benchmark" ]; then
    echo "citus-benchmark directory already exists, skipping clone"
else
    echo "Cloning CH-benCHmark repository..."
    git clone https://github.com/citusdata/citus-benchmark.git citus-benchmark
    echo "CH-benCHmark repository downloaded successfully"
fi

export PGHOST=localhost
export PGPORT=$PG_PORT
export PGUSER=vista
export PGDATABASE=chbenchmark
export PGPASSWORD=vista

echo "Creating chbenchmark database..."
$PSQL_CMD -c "DROP DATABASE IF EXISTS chbenchmark;"
$PSQL_CMD -c "CREATE DATABASE chbenchmark;"

cd citus-benchmark
echo "Building CH-benCHmark dataset with $NR_WAREHOUSE warehouses..."
PGNUMVU=$([ "$NR_WAREHOUSE" -le "$(nproc)" ] && echo "$NR_WAREHOUSE" || echo "$(nproc)")
echo "Using $PGNUMVU parallel connections for data generation."
sleep 1

sed -i "/pg_num_vu/ c\diset tpcc pg_num_vu $PGNUMVU" ./build.tcl
sed -i "s/^diset tpcc pg_count_ware .*/diset tpcc pg_count_ware $NR_WAREHOUSE/" ./build.tcl

./build.sh --ch --no-citus

cd ..

# Stop PostgreSQL server
echo "Stopping PostgreSQL server..."
./postgres-only/pgsql/bin/pg_ctl -D $DATA_DIR stop

echo ""
echo "==========================================="
echo "Copying dataset..."
echo "==========================================="

mkdir -p dataset
cp -r postgres-only/pgsql/data dataset/warehouse-$NR_WAREHOUSE

echo ""
echo "==========================================="
echo "Dataset build completed!"
echo "==========================================="
echo ""
echo "Dataset location: $(pwd)/dataset/warehouse-$NR_WAREHOUSE"
echo ""
