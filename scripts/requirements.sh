#!/bin/bash
set -e

echo "=========================================="
echo "Installing System Requirements"
echo "=========================================="
echo ""
echo "This script installs system-level dependencies required for:"
echo "  - pg_duckdb"
echo "  - PostgreSQL development"
echo ""
echo "This needs to be run with sudo privileges."
echo ""

# Install PostgreSQL common packages
echo "Installing PostgreSQL common packages..."
sudo apt install -y postgresql-common

echo "Adding PostgreSQL APT repository..."
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y

echo "Installing PostgreSQL server development packages..."
sudo apt install -y postgresql-server-dev-15

# Install build dependencies
echo "Installing build dependencies..."
sudo apt install -y \
    build-essential \
    libreadline-dev \
    zlib1g-dev \
    flex \
    bison \
    libxml2-dev \
    libxslt-dev \
    libssl-dev \
    libxml2-utils \
    xsltproc \
    pkg-config \
    libc++-dev \
    libc++abi-dev \
    libglib2.0-dev \
    libtinfo6 \
    cmake \
    libstdc++-12-dev \
    liblz4-dev \
    ninja-build

echo ""
echo "Removing conflicting system DuckDB installation..."
sudo rm -rf /usr/local/include/duckdb
sudo rm -f /usr/local/lib/libduckdb*

echo ""
echo "=========================================="
echo "System requirements installed successfully!"
echo "=========================================="
echo ""
