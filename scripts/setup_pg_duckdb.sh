#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

PG_DUCKDB_PORT=5002

echo "=========================================="
echo "pg_duckdb Setup Script"
echo "=========================================="
echo ""

# Create postgres-pgd directory
mkdir -p postgres-pgd
cd postgres-pgd

# ==========================================
# Step 1: Clone Repositories
# ==========================================
echo "=========================================="
echo "Step 1: Clone PostgreSQL and pg_duckdb"
echo "=========================================="

# Clone PostgreSQL repository
if [ -d "postgres_for_pg_duckdb" ]; then
    echo "postgres_for_pg_duckdb directory already exists, skipping clone"
else
    git clone --depth=1 --branch=REL_15_2 https://github.com/postgres/postgres.git postgres_for_pg_duckdb
    echo "PostgreSQL repository downloaded successfully"
fi

# Clone pg_duckdb repository
if [ -d "pg_duckdb" ]; then
    echo "pg_duckdb directory already exists, skipping clone"
else
    git clone --depth=1 --branch=v1.0.0 https://github.com/duckdb/pg_duckdb.git pg_duckdb
    echo "pg_duckdb repository downloaded successfully"
fi

echo ""

# ==========================================
# Step 2: Build PostgreSQL
# ==========================================
echo "=========================================="
echo "Step 2: Build PostgreSQL"
echo "=========================================="

echo "Configuring PostgreSQL for pg_duckdb..."
cd postgres_for_pg_duckdb
./configure --prefix=$(pwd)/../pgsql

echo "Building PostgreSQL..."
make -j

echo "Installing PostgreSQL..."
make install

cd ..

echo ""

# ==========================================
# Step 3: Build and Install pg_duckdb
# ==========================================
echo "=========================================="
echo "Step 3: Build and Install pg_duckdb"
echo "=========================================="
echo "NOTE: Make sure you have run requirements.sh first to install system dependencies."
echo ""

cd pg_duckdb

echo "Building DuckDB dependency..."
make duckdb

echo "Building pg_duckdb..."
make -j
make install PG_CONFIG=../pgsql/bin/pg_config
cd ..

echo ""
echo "=========================================="
echo "pg_duckdb setup completed!"
echo "=========================================="
