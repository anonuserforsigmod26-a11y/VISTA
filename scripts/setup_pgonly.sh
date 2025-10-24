#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

PG_PORT=5003

echo "=========================================="
echo "PostgreSQL-only Setup Script"
echo "=========================================="
echo ""

# Check if postgres-only source exists
if [ ! -d "postgres-only" ]; then
    echo "Downloading PostgreSQL 15.2..."
    git clone --depth=1 --branch=REL_15_2 https://github.com/postgres/postgres.git postgres-only
    echo "PostgreSQL repository downloaded successfully"
else
    echo "postgres-only directory exists"
fi

cd postgres-only

# Check if PostgreSQL source is valid
if [ ! -f "configure" ]; then
    echo "Error: PostgreSQL source not found in postgres-only directory"
    exit 1
fi

mkdir -p ./pgsql

echo "=========================================="
echo "Step 1: Configure PostgreSQL"
echo "=========================================="
./configure --prefix=$(pwd)/pgsql

echo ""
echo "=========================================="
echo "Step 2: Build PostgreSQL"
echo "=========================================="
make -j

echo ""
echo "=========================================="
echo "Step 3: Install PostgreSQL"
echo "=========================================="
make install

echo ""
echo "=========================================="
echo "Step 4: Initialize Database"
echo "=========================================="
mkdir -p ./pgsql/data
./pgsql/bin/initdb -D ./pgsql/data

echo "Configuring PostgreSQL port to ${PG_PORT}..."
sed -i "s/^#*port.*/port = $PG_PORT/" ./pgsql/data/postgresql.conf

echo ""
echo "=========================================="
echo "Step 5: Create User and Database"
echo "=========================================="
echo "Starting PostgreSQL..."
./pgsql/bin/pg_ctl -D ./pgsql/data -l ./pgsql/logfile start

sleep 2

echo "Creating vista user and database..."
./pgsql/bin/createuser -p $PG_PORT -s vista
./pgsql/bin/createdb -p $PG_PORT -O vista vista

echo "Stopping PostgreSQL..."
./pgsql/bin/pg_ctl -D ./pgsql/data stop

echo ""
echo "=========================================="
echo "PostgreSQL-only setup completed!"
echo "=========================================="
