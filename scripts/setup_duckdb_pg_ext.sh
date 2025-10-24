#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

PG_DUCKDB_POSTGRES_PORT=5001

echo "=========================================="
echo "DuckDB postgres_scanner Extension Setup"
echo "=========================================="
echo ""

# Create duckdb-pg_ext directory
mkdir -p duckdb-pg_ext
cd duckdb-pg_ext

# ==========================================
# Step 1: Clone and Setup duckdb-postgres
# ==========================================
echo "=========================================="
echo "Step 1: Clone and Setup duckdb-postgres"
echo "=========================================="

if [ ! -d "duckdb-postgres" ]; then
    echo "Cloning duckdb-postgres (duckdb version v1.2.2)..."
    git clone https://github.com/duckdb/duckdb-postgres.git duckdb-postgres
else
    echo "duckdb-postgres directory already exists, skipping clone"
fi

cd duckdb-postgres

echo "Checkout to the duckdb 1.2.2 support version..."
git checkout 98210d5 # Initial commit of duckdb-postgres v1.2.2 support.

echo "Submodule initiation started..."
git submodule init
git submodule update --init --recursive

echo "Building duckdb-postgres..."
make -j

cd ..
echo ""

# ==========================================
# Step 2: Clone and Build PostgreSQL
# ==========================================
echo "=========================================="
echo "Step 2: Clone and Build PostgreSQL"
echo "=========================================="

if [ ! -d "postgres_for_duckdb-postgres" ]; then
    echo "Cloning PostgreSQL 15.2..."
    git clone --depth=1 --branch=REL_15_2 https://github.com/postgres/postgres.git postgres_for_duckdb-postgres
else
    echo "postgres_for_duckdb-postgres directory already exists, skipping clone"
fi

cd postgres_for_duckdb-postgres

echo "Configuring PostgreSQL..."
./configure --prefix=$(pwd)/../pgsql

echo "Building and installing PostgreSQL..."
make -j
make install

cd ..

echo ""

echo "=========================================="
echo "DuckDB postgres_scanner setup completed!"
echo "=========================================="
