#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/../vista"

VISTA_PG_PORT=5004

# Create VISTA lock directory
mkdir -p /run/lock/vista

# Color echo
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "VISTA Setup Script"
echo "=========================================="
echo ""

# ==========================================
# Step 1: Setup DuckDB v1.2.2
# ==========================================
echo "=========================================="
echo "Step 1: Setup DuckDB v1.2.2"
echo "=========================================="

echo "Downloading original duckdb v1.2.2..."
if [ -d ".__duckdb" ]; then
    echo ".__duckdb directory already exists, skipping clone"
else
    git clone --branch=v1.2.2 --depth 1 https://github.com/duckdb/duckdb.git .__duckdb
    echo ".__duckdb repository downloaded successfully"
fi

cd .__duckdb
echo "Building DuckDB v1.2.2..."
make -j
echo "DuckDB build completed."
cd ..

echo ""
echo "Step 1 completed successfully!"
echo "DuckDB v1.2.2 is ready at: $(pwd)/.__duckdb"
echo ""

# ==========================================
# Step 2: Setup libvista and lib-scan
# ==========================================
echo "=========================================="
echo "Step 2: Setup libvista and lib-scan"
echo "=========================================="

# Check if DuckDB is built
if [ ! -f ".__duckdb/build/release/src/libduckdb.so" ]; then
    echo "Error: DuckDB is not built. Step 1 may have failed."
    exit 1
fi

FULL_PATH=$(pwd)

# Configure lib-scan paths
echo "Configuring lib-scan paths..."
sed -i "s|^PURE_DUCKDB *=.*|PURE_DUCKDB = ${FULL_PATH}/.__duckdb|g" lib-scan/Makefile
sed -i "s|^#define POSTGRES_DATA_DIR .*|#define POSTGRES_DATA_DIR \"${FULL_PATH}/pgsql/data\"|g" lib-scan/include/file_access.h

echo "Building libvista..."
cd libvista
make clean
make -j
echo "libvista build completed."
cd ..

echo "Building lib-scan..."
cd lib-scan
make clean
make -j
echo "lib-scan build completed."
cd ..

echo ""
echo "Step 2 completed successfully!"
echo "libvista is ready at: ${FULL_PATH}/libvista/libvista.so"
echo "lib-scan is ready at: ${FULL_PATH}/lib-scan/libscan.so"
echo ""

# ==========================================
# Step 3: Setup VISTA PostgreSQL
# ==========================================
echo "=========================================="
echo "Step 3: Setup VISTA PostgreSQL"
echo "=========================================="
echo "Building and installing VISTA PostgreSQL with VISTA buffer enabled..."

cd postgres
./scripts/build_and_install.sh --enable-vista-buffer
cd ..

echo "Fixing PostgreSQL directory ownership..."
sudo chown -R $USER:$USER ./pgsql

echo ""
echo "Step 3 completed successfully!"
echo "PostgreSQL is installed at: $(pwd)/pgsql"
echo ""

# ==========================================
# Step 4: Setup VISTA DuckDB Extension
# ==========================================
echo "=========================================="
echo "Step 4: Setup VISTA DuckDB Extension"
echo "=========================================="

# Check if lib-scan is built
if [ ! -f "lib-scan/libscan.so" ]; then
    echo "Error: lib-scan is not built. Step 2 may have failed."
    exit 1
fi

echo "Creating vista-caches directory..."
mkdir -p ${FULL_PATH}/../vista-caches

echo "Building VISTA DuckDB extension..."
cd duckdb-ex

# Configure libscan.so and libvista.so paths in CMakeLists.txt
sed -i "s|/[^ ]*libscan\.so|${FULL_PATH}/lib-scan/libscan.so|" CMakeLists.txt
sed -i "s|/[^ ]*libvista\.so|${FULL_PATH}/libvista/libvista.so|" CMakeLists.txt

# Configure cache directory path in global_cache_manager.cpp
sed -i "s|/path/to/your/vista-caches/|${FULL_PATH}/../vista-caches/|g" src/global_cache_manager.cpp

make -j
echo "VISTA DuckDB extension build completed."

cd ..

echo ""
echo "Step 4 completed successfully!"
echo "DuckDB extension is ready at: ${FULL_PATH}/duckdb-ex/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension"
echo ""

echo "=========================================="
echo "All steps completed successfully!"
echo "=========================================="
