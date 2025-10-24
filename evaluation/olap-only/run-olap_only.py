#!/usr/bin/env python3
"""
CH-Benchmark OLAP Query Runner (Cold Cache Mode)
Executes TPC-H style queries with server restart and cache drop before each query
"""

import argparse
import os
import sys
import psycopg2
import time
import random
import json
import subprocess
import threading
import re
import signal
import shutil
import glob
from datetime import datetime
from typing import List, Dict, Tuple, Optional
from pathlib import Path
from statistics import geometric_mean


class BenchmarkConfig:
    """Configuration for benchmark execution"""

    VALID_BASELINES = ["postgres-only", "postgres-pgd", "duckdb-pg_ext", "vista"]
    BASELINE_PORTS = {
        "postgres-only": 5003,
        "postgres-pgd": 5002,
        "duckdb-pg_ext": None,  # Uses DuckDB CLI directly
        "vista": None,  # Uses DuckDB CLI directly
    }

    # Data directory paths for each baseline
    DATA_DIRS = {
        "postgres-only": "postgres-only/pgsql/data",
        "postgres-pgd": "postgres-pgd/pgsql/data",
        "duckdb-pg_ext": "duckdb-pg_ext/pgsql/data",
        "vista": "vista/pgsql/data"
    }

    # PostgreSQL config file names
    PG_CONFIGS = {
        "postgres-only": "postgresql_vanilla.conf",
        "postgres-pgd": "postgresql_pgd.conf",
        "duckdb-pg_ext": "postgresql_duckdb_ext.conf",
        "vista": "postgresql_vista.conf"
    }

    # cgroup mappings for PostgreSQL
    PG_CGROUPS = {
        "postgres-only": "vista_base",
        "postgres-pgd": "vista_base",
        "duckdb-pg_ext": "vista_base",
        "vista": "vista_nohp"
    }

    # cgroup mappings for DuckDB CLI
    DUCKDB_CGROUPS = {
        "duckdb-pg_ext": "vista_base",
        "vista": "vista_nohp"
    }

    def __init__(self, baseline: str, port: int = None, show_result: bool = False,
                 iterations: int = None, output: str = None, init_file: str = None,
                 timeout: int = 1800, excluding: str = None, no_random: bool = False,
                 repeat: int = 1, data_source: str = None):
        self.baseline = baseline
        self.show_result = show_result
        self.iterations = iterations
        self.output = output
        self.init_file = init_file
        self.timeout = timeout
        self.no_random = no_random
        self.repeat = repeat
        self.data_source = data_source

        # Parse excluding queries
        self.excluded_queries = set()
        if excluding:
            try:
                query_nums = [num.strip() for num in excluding.split(',')]
                self.excluded_queries = {f"Q{num}" for num in query_nums}
            except Exception as e:
                raise ValueError(f"Invalid --excluding format: {excluding}")

        # Validate baseline
        if baseline not in self.VALID_BASELINES:
            raise ValueError(f"Invalid baseline: {baseline}")

        # Set port
        if port is not None:
            self.port = port
        else:
            self.port = self.BASELINE_PORTS.get(baseline)

        # Database connection settings
        self.host = "localhost"
        self.dbname = "chbenchmark"
        self.user = "vista"
        self.password = "vista"


class ServerManager:
    """Manages server start/stop and cache operations"""

    def __init__(self, config: BenchmarkConfig):
        self.config = config
        # Get the root directory (supplementary-material/) as absolute path
        self.root_dir = Path(__file__).parent.parent.parent.resolve()

    def copy_dataset(self):
        """Copy fresh dataset from source to baseline data directory"""
        if not self.config.data_source:
            print("\nWarning: No data source specified, skipping dataset copy")
            return

        print("\nCopying fresh dataset...")

        # Convert to absolute path if relative
        data_source = Path(self.config.data_source)
        if not data_source.is_absolute():
            data_source = data_source.resolve()

        if not data_source.exists():
            print(f"  Error: Data source not found: {data_source}")
            sys.exit(1)

        # Get target data directory
        baseline = self.config.baseline
        data_dir = self.root_dir / BenchmarkConfig.DATA_DIRS[baseline]

        print(f"  Source: {data_source}")
        print(f"  Target: {data_dir}")

        # Remove old data directory
        if data_dir.exists():
            print(f"  Removing old data directory...")
            shutil.rmtree(data_dir)

        # Copy new dataset
        try:
            print(f"  Copying dataset (this may take a while)...")
            shutil.copytree(data_source, data_dir)
            print(f"  Dataset copied successfully")
        except Exception as e:
            print(f"  Error: Failed to copy dataset: {e}")
            sys.exit(1)

    def setup_config(self):
        """Copy PostgreSQL config file (do this once at the beginning)"""
        print("\nSetting up PostgreSQL configuration...")

        baseline = self.config.baseline
        config_file = BenchmarkConfig.PG_CONFIGS[baseline]
        data_dir = self.root_dir / BenchmarkConfig.DATA_DIRS[baseline]

        # pgconf is in the same directory as this script
        script_dir = Path(__file__).parent.resolve()
        source_config = script_dir / "pgconf" / config_file
        dest_config = data_dir / "postgresql.conf"

        if not source_config.exists():
            print(f"  Error: Config file not found: {source_config}")
            sys.exit(1)

        try:
            shutil.copy(source_config, dest_config)
            print(f"  Copied {config_file} to {data_dir}")
        except Exception as e:
            print(f"  Warning: Failed to copy config: {e}")

    def stop_server(self):
        """Stop the database server"""
        print("  Stopping server...")
        try:
            subprocess.run(
                ['./scripts/stop_server.sh'],
                cwd=self.root_dir,
                check=False,  # Don't fail if server is already stopped
                capture_output=True,
                text=True,
                timeout=30
            )
            time.sleep(2)
        except subprocess.TimeoutExpired:
            print("  Warning: Timeout stopping server")
        except Exception as e:
            print(f"  Warning: Error stopping server: {e}")

    def drop_cache(self):
        """Drop system caches"""
        print("  Dropping caches...")
        try:
            subprocess.run(
                ['./drop_cache.sh'],
                cwd=self.root_dir / 'evaluation' / '.config_scripts',
                check=True,
                capture_output=True,
                text=True,
                timeout=180
            )
        except subprocess.TimeoutExpired:
            print("  Warning: Timeout dropping cache (180s)")
        except subprocess.CalledProcessError as e:
            print(f"  Warning: Error dropping cache (exit code {e.returncode})")
            print("  Make sure to run 'sudo -v' before executing this script!")

    def start_server(self):
        """Start the database server with appropriate cgroup"""
        # Get cgroup for this baseline
        cgroup_name = BenchmarkConfig.PG_CGROUPS[self.config.baseline]

        print(f"  Starting PostgreSQL in cgroup '{cgroup_name}'...")

        cmd = ['cgexec', '-g', f'cpu,cpuset,memory:{cgroup_name}',
               './scripts/start_server.sh', '--baseline', self.config.baseline]

        try:
            subprocess.run(
                cmd,
                cwd=self.root_dir,
                check=True,
                capture_output=True,
                text=True,
                timeout=30
            )
            time.sleep(5)
            print(f"  PostgreSQL started successfully in cgroup '{cgroup_name}'")
        except subprocess.TimeoutExpired:
            print("  Error: Timeout starting server")
            sys.exit(1)
        except subprocess.CalledProcessError as e:
            print(f"  Error starting server (exit code {e.returncode})")
            if e.stdout:
                print(f"  stdout: {e.stdout}")
            if e.stderr:
                print(f"  stderr: {e.stderr}")
            sys.exit(1)

        # VISTA requires additional initialization
        if self.config.baseline == "vista":
            self._init_vista_pg()

    def _init_vista_pg(self):
        """Initialize VISTA PostgreSQL with warmup query"""
        # Use specified port or default to 5004
        vista_pg_port = self.config.port if self.config.port else 5004
        cgroup_name = BenchmarkConfig.PG_CGROUPS[self.config.baseline]

        print(f"  Initializing VISTA PostgreSQL (cgroup '{cgroup_name}', port {vista_pg_port})...")

        # VISTA initialization queries
        init_queries = """
CREATE OR REPLACE FUNCTION vista_add_all_tables_to_bitmap()
RETURNS integer
AS 'vista_udf', 'vista_add_all_tables_to_bitmap'
LANGUAGE C STRICT;

SELECT vista_add_all_tables_to_bitmap();

BEGIN;
SELECT 1;
COMMIT;
"""

        # Retry connection up to 10 times with 1 second delay
        max_retries = 10
        for attempt in range(max_retries):
            try:
                conn = psycopg2.connect(
                    host=self.config.host,
                    port=vista_pg_port,
                    dbname=self.config.dbname,
                    user=self.config.user,
                    password=self.config.password
                )
                cursor = conn.cursor()
                cursor.execute(init_queries)
                cursor.close()
                conn.close()
                print("  VISTA PostgreSQL initialized successfully")
                return
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    print(f"  Error: Failed to initialize VISTA PostgreSQL after {max_retries} attempts: {e}")
                    sys.exit(1)


class QueryParser:
    """Parses SQL queries from the benchmark file"""

    def __init__(self, query_file: str):
        self.query_file = query_file
        self.queries: Dict[str, str] = {}

    def parse(self) -> Dict[str, str]:
        """Parse queries from file, returning dict of query_name -> query_sql"""
        with open(self.query_file, 'r') as f:
            content = f.read()

        lines = content.split('\n')
        current_query_name = None
        current_query_lines = []

        for line in lines:
            stripped = line.strip()
            if stripped.startswith('-- Q') and len(stripped) <= 6:
                if current_query_name and current_query_lines:
                    query_sql = '\n'.join(current_query_lines).strip()
                    if query_sql:
                        self.queries[current_query_name] = query_sql

                current_query_name = stripped[3:].strip()
                current_query_lines = []
            elif current_query_name:
                current_query_lines.append(line)

        if current_query_name and current_query_lines:
            query_sql = '\n'.join(current_query_lines).strip()
            if query_sql:
                self.queries[current_query_name] = query_sql

        return self.queries


class DuckDBPostgresConnection:
    """Handles DuckDB CLI connection via subprocess"""

    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.process: Optional[subprocess.Popen] = None
        self.output_buffer = []
        self.output_lock = threading.Lock()
        self.output_event = threading.Event()
        self.reader_thread = None

        # Get the root directory (supplementary-material/) as absolute path
        root_dir = Path(__file__).parent.parent.parent.resolve()
        # Get the script directory (cold-warm/) for local files
        script_dir = Path(__file__).parent.resolve()

        # Set DuckDB path and init commands based on baseline
        if config.baseline == "vista":
            self.duckdb_path = str(root_dir / "vista/duckdb-ex/build/release/duckdb")
            init_template = str(script_dir / "duckdb_init/init_vista.sql")
            extension_path = str(root_dir / "vista/duckdb-ex/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension")
        else:  # duckdb-pg_ext
            self.duckdb_path = str(root_dir / "duckdb-pg_ext/duckdb-postgres/build/release/duckdb")
            init_template = str(script_dir / "duckdb_init/init_duckdb_ext.sql")
            extension_path = str(root_dir / "duckdb-pg_ext/duckdb-postgres/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension")

        # Read template and replace placeholder (keep in memory)
        with open(init_template, 'r') as f:
            init_content = f.read()
        self.init_commands = init_content.replace('{EXTENSION_PATH}', extension_path)

    def connect(self):
        """Start DuckDB CLI process with appropriate cgroup"""
        try:
            # Get cgroup for this baseline
            cgroup_name = BenchmarkConfig.DUCKDB_CGROUPS[self.config.baseline]

            print(f"    Starting DuckDB CLI in cgroup '{cgroup_name}'...")

            # Build base command (no -init, we'll send commands via stdin)
            duckdb_cmd = [self.duckdb_path, "-unsigned"]

            # Wrap with cgroup
            cmd = ['cgexec', '-g', f'cpu,cpuset,memory:{cgroup_name}'] + duckdb_cmd

            self.process = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )

            self.reader_thread = threading.Thread(target=self._read_output, daemon=True)
            self.reader_thread.start()

            time.sleep(2)
            print(f"    Started DuckDB CLI (PID: {self.process.pid})")

            # Send init commands via stdin
            print(f"    Sending initialization commands...")
            self.process.stdin.write(self.init_commands)
            self.process.stdin.flush()
            time.sleep(3)  # Wait for init to complete
        except Exception as e:
            print(f"    Failed to start DuckDB CLI: {e}")
            sys.exit(1)

    def _read_output(self):
        """Read output from subprocess continuously"""
        if self.process and self.process.stdout:
            for line in iter(self.process.stdout.readline, ''):
                print(line)
                if not line:
                    break
                with self.output_lock:
                    self.output_buffer.append(line)
                self.output_event.set()

    def disconnect(self):
        """Close DuckDB CLI process"""
        if self.process:
            try:
                self.process.stdin.write(".quit\n")
                self.process.stdin.flush()
                self.process.wait(timeout=5)
            except:
                self.process.terminate()
                self.process.wait(timeout=2)
            finally:
                if self.process.poll() is None:
                    self.process.kill()

    def execute_query(self, query_name: str, query_sql: str) -> Tuple[float, str]:
        """Execute a query via stdin and measure execution time"""
        if not self.process or self.process.poll() is not None:
            return -1.0, "error"

        try:
            with self.output_lock:
                self.output_buffer.clear()
            self.output_event.clear()

            commands = ".timer on\n" + query_sql + "\n.timer off\n"

            self.process.stdin.write(commands)
            self.process.stdin.flush()

            execution_time, status = self._wait_for_completion(query_name)

            if status == "error":
                return -1.0, "error"
            elif execution_time < 0:
                return -1.0, "timeout"

            return execution_time, "success"

        except Exception as e:
            print(f"    Error executing query {query_name}: {e}")
            return -1.0, "error"

    def _wait_for_completion(self, query_name: str, timeout: Optional[int] = None) -> Tuple[float, str]:
        """Wait for query completion and extract execution time"""
        if timeout is None:
            timeout = self.config.timeout

        start_wait = time.time()
        execution_time = -1.0

        timer_pattern = re.compile(r'Run Time.*?:\s*(?:real\s+)?([\d.]+)')
        error_pattern = re.compile(r'(Error|ERROR|Exception):', re.IGNORECASE)

        while time.time() - start_wait < timeout:
            remaining_time = timeout - (time.time() - start_wait)
            if remaining_time <= 0:
                break

            self.output_event.wait(timeout=min(remaining_time, 0.1))

            with self.output_lock:
                buffer_copy = self.output_buffer.copy()
                self.output_buffer.clear()

            self.output_event.clear()

            for line in buffer_copy:
                if error_pattern.search(line):
                    print(f"    Query error: {line.strip()}")
                    return -1.0, "error"

                match = timer_pattern.search(line)
                if match:
                    execution_time = float(match.group(1))
                    return execution_time, "success"

        return -1.0, "timeout"


class DatabaseConnection:
    """Handles database connection and query execution"""

    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.conn = None
        self.cursor = None

    def connect(self):
        """Establish database connection"""
        # Get cgroup where PostgreSQL is running
        cgroup_name = BenchmarkConfig.PG_CGROUPS[self.config.baseline]
        print(f"    Connecting to PostgreSQL (running in cgroup '{cgroup_name}')...")

        try:
            self.conn = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                dbname=self.config.dbname,
                user=self.config.user,
                password=self.config.password
            )
            self.cursor = self.conn.cursor()

            timeout_ms = self.config.timeout * 1000
            self.cursor.execute(f"SET statement_timeout = {timeout_ms}")
            self.conn.commit()

            print(f"    Connected to port {self.config.port}")
        except Exception as e:
            print(f"    Failed to connect: {e}")
            sys.exit(1)

    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def execute_query(self, query_name: str, query_sql: str) -> Tuple[float, str]:
        """Execute a query and return (execution time, status)"""
        if self.config.baseline == "postgres-pgd":
            full_query = "SET duckdb.force_execution = true;\n" + query_sql
        else:
            full_query = query_sql

        try:
            start_time = time.time()
            self.cursor.execute(full_query)
            results = self.cursor.fetchall()
            end_time = time.time()

            if self.config.show_result:
                print(f"\n--- Results for {query_name} ---")
                if results:
                    col_names = [desc[0] for desc in self.cursor.description]
                    print(" | ".join(col_names))
                    print("-" * 80)
                    for row in results:
                        print(" | ".join(str(val) for val in row))
                else:
                    print("No results")
                print("--- End ---\n")

            self.conn.commit()

            execution_time = end_time - start_time
            return execution_time, "success"
        except psycopg2.errors.QueryCanceled:
            self.conn.rollback()
            return -1.0, "timeout"
        except Exception as e:
            print(f"    Error: {e}")
            self.conn.rollback()
            return -1.0, "error"


class BenchmarkExecutor:
    """Main benchmark execution logic"""

    def __init__(self, config: BenchmarkConfig, queries: Dict[str, str]):
        self.config = config
        self.queries = queries
        self.server_manager = ServerManager(config)
        self.results: List[Dict] = []

        if self.config.output:
            self.output_filename = self.config.output
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.output_filename = f"benchmark_cold_{self.config.baseline}_{timestamp}.json"

    def run(self):
        """Execute benchmark for specified iterations"""
        random.seed(7355608)

        # Copy fresh dataset from source
        self.server_manager.copy_dataset()

        # Setup PostgreSQL config once at the beginning
        self.server_manager.setup_config()

        query_names = [q for q in self.queries.keys() if q not in self.config.excluded_queries]

        if self.config.excluded_queries:
            excluded_list = sorted(self.config.excluded_queries)
            print(f"Excluding queries: {', '.join(excluded_list)}")

        print(f"\nStarting cold cache benchmark for {self.config.iterations} iterations")
        print(f"Total queries per iteration: {len(query_names)}")
        print(f"Each query will: stop server → drop cache → start server → execute\n")

        for iteration in range(1, self.config.iterations + 1):
            print(f"\n{'='*60}")
            print(f"Iteration {iteration}/{self.config.iterations}")
            print(f"{'='*60}")

            if not self.config.no_random:
                current_query_names = query_names.copy()
                random.shuffle(current_query_names)
            else:
                current_query_names = sorted(query_names, key=lambda x: int(x[1:]))

            for idx, query_name in enumerate(current_query_names, 1):
                query_sql = self.queries[query_name]

                print(f"\n[{idx}/{len(current_query_names)}] {query_name} (repeat {self.config.repeat}x)")

                # Stop, drop cache, start (every query gets fresh start)
                self.server_manager.stop_server()
                self.server_manager.drop_cache()
                self.server_manager.start_server()

                # Connect once
                if self.config.baseline in ["duckdb-pg_ext", "vista"]:
                    db = DuckDBPostgresConnection(self.config)
                else:
                    db = DatabaseConnection(self.config)

                db.connect()

                # Execute query repeat times
                execution_times = []
                statuses = []
                timestamps = []

                for rep in range(self.config.repeat):
                    cache_state = "cold" if rep == 0 else "warm"
                    print(f"  Executing ({cache_state}, {rep+1}/{self.config.repeat})...", end=" ")

                    exec_time, status = db.execute_query(query_name, query_sql)
                    execution_times.append(exec_time if status == "success" else -1)
                    statuses.append(status)
                    timestamps.append(datetime.now().isoformat())

                    if status == "success":
                        print(f"{exec_time:.3f}s")
                    elif status == "timeout":
                        print("TIMEOUT")
                    else:
                        print("FAILED")

                db.disconnect()

                # Calculate average of successful executions
                successful_times = [t for t, s in zip(execution_times, statuses) if s == "success"]
                avg_time = sum(successful_times) / len(successful_times) if successful_times else -1

                self.results.append({
                    "iteration": iteration,
                    "query": query_name,
                    "execution_times": execution_times,
                    "statuses": statuses,
                    "timestamps": timestamps,
                    "avg_time": avg_time
                })

                self.export_results_incremental()

            # Calculate geometric mean for iteration using avg_time
            iteration_times = [r["avg_time"] for r in self.results
                             if r["iteration"] == iteration and r["avg_time"] > 0]
            if iteration_times:
                geomean = geometric_mean(iteration_times)
                print(f"\nIteration {iteration} Geometric Mean: {geomean:.3f}s")

        print("\n\nStopping server...")
        self.server_manager.stop_server()

        print(f"\nBenchmark completed. Total executions: {len(self.results)}")

    def export_results_incremental(self):
        """Export current results to JSON file"""
        output = {
            "metadata": {
                "baseline": self.config.baseline,
                "port": self.config.port,
                "iterations": self.config.iterations,
                "repeat": self.config.repeat,
                "mode": "cold_cache",
                "total_executions": len(self.results),
                "timestamp": datetime.now().isoformat()
            },
            "results": self.results
        }

        with open(self.output_filename, 'w') as f:
            json.dump(output, f, indent=2)

    def export_results(self):
        """Export final results"""
        self.export_results_incremental()
        print(f"Results exported to {self.output_filename}")


def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description="CH-Benchmark OLAP Query Runner (Cold Cache Mode)"
    )
    parser.add_argument(
        "--baseline",
        required=True,
        choices=BenchmarkConfig.VALID_BASELINES,
        help="Baseline system to test"
    )
    parser.add_argument(
        "--port",
        type=int,
        help="Override default port number"
    )
    parser.add_argument(
        "--iter",
        type=int,
        required=True,
        help="Number of iterations (REQUIRED)"
    )
    parser.add_argument(
        "--show-result",
        action="store_true",
        help="Show query results"
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output JSON file path"
    )
    parser.add_argument(
        "--init",
        type=str,
        help="SQL file with initialization queries (for DuckDB)"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=1800,
        help="Query timeout in seconds (default: 1800)"
    )
    parser.add_argument(
        "--excluding",
        type=str,
        help="Comma-separated query numbers to exclude (e.g., '5,10,21')"
    )
    parser.add_argument(
        "--no-random",
        action="store_true",
        help="Execute queries in order (Q1, Q2, ...) instead of randomizing"
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="Number of times to repeat each query without restarting server (default: 1)"
    )
    parser.add_argument(
        "--data",
        type=str,
        required=True,
        help="Source dataset directory to copy (REQUIRED)"
    )

    return parser.parse_args()


def main():
    """Main entry point"""
    args = parse_arguments()

    print("=" * 60)
    print("CH-Benchmark OLAP Query Runner (Cold Cache Mode)")
    print("=" * 60)
    print("\nIMPORTANT: This script requires sudo for cache drop.")
    print("Run 'sudo -v' before executing this script.\n")

    try:
        config = BenchmarkConfig(
            baseline=args.baseline,
            port=args.port,
            show_result=args.show_result,
            iterations=args.iter,
            output=args.output,
            init_file=args.init,
            timeout=args.timeout,
            excluding=args.excluding,
            no_random=args.no_random,
            repeat=args.repeat,
            data_source=args.data
        )

        query_file = Path(__file__).parent.resolve() / "ch-benchmark-queries.sql"
        parser = QueryParser(str(query_file))
        queries = parser.parse()

        if not queries:
            print("Error: No queries found")
            sys.exit(1)

        print(f"Loaded {len(queries)} queries")

        executor = BenchmarkExecutor(config, queries)
        executor.run()
        executor.export_results()

    except (ValueError, NotImplementedError) as e:
        print(f"Error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
