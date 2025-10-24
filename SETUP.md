# Evaluation Setup

## 1. Build Systems

From `supplementary-material/` directory, build all systems:

```bash
# Install dependencies
./scripts/requirements.sh

# Build each system
./scripts/setup_vista.sh
./scripts/setup_duckdb_pg_ext.sh
./scripts/setup_pg_duckdb.sh
./scripts/setup_pgonly.sh

# Create dataset (1000 warehouses)
./scripts/build_dataset.sh --warehouse 1000
```

## 2. System Configuration

Before running experiments, create cgroups:

```bash
sudo ./evaluation/.config_scripts/create_eval_cgroup.sh
```

This creates:
- `vista_base`: 32 CPUs (NUMA0), 64GB - for pgonly, duckdb_ext, pg_duckdb
- `vista_nohp`: 32 CPUs (16 NUMA0 + 16 NUMA1), 60GB - for vista

## 3. Experiments

### olap-only/
OLAP-only workload: Runs 22 CH-Benchmark queries without concurrent OLTP.

Usage:
```bash
./run-olap_only.py --data <data-dir> --baseline <pgonly|duckdb_ext|pg_duckdb|vista>
```

Example:
```bash
./run-olap_only.py --baseline vista --iter 1 --data ../../dataset/warehouse-1000 \
    --repeat 2 --excluding "21" --output vista.json --timeout 600 --no-random
```

- `--repeat 2`: First run is cold (no cache), second run is warm (cached)
- `--iter`: Number of iterations
- `--excluding`: Skip specific queries (e.g., "21" or "5,21")
- `--timeout`: Query timeout in seconds

### olap-with-oltp/
HTAP workload: Runs OLAP queries with concurrent TPC-C workload.

Usage:
```bash
./run_eval.sh --data <data-dir> --baseline <pgonly|duckdb_ext|pg_duckdb|vista>
```

Example:
```bash
./run_eval.sh --data ../../dataset/warehouse-1000 --baseline vista
```

To modify OLTP workers or iterations, edit the script:
- Line 11: `ITERATION=3` - Number of iterations
- Line 84: `WORKER_COUNTS=(4 12)` - OLTP worker count

## Baseline Ports

- `pgonly`: PostgreSQL only (port 5003)
- `duckdb_ext`: DuckDB postgres_scanner extension (port 5001)
- `pg_duckdb`: pg_duckdb extension (port 5002)
- `vista`: VISTA (port 5004)
