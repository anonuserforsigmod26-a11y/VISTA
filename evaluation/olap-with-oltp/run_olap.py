#!/usr/bin/env python3
"""
CH-Benchmark OLAP Query Runner
Executes TPC-H style queries and collects execution time metrics
"""

import argparse
import os
import sys
import psycopg2
import time
import json
import subprocess
import threading
import re
import signal
from datetime import datetime
from typing import List, Dict, Tuple, Optional
from pathlib import Path
from statistics import geometric_mean


class BenchmarkConfig:
    """Configuration for benchmark execution"""

    VALID_BASELINES = ["postgres_only", "pg_duckdb", "duckdb-postgres", "vista"]
    BASELINE_PORTS = {
        "postgres_only": 5003,
        "pg_duckdb": 5002,
        "duckdb-postgres": None,  # Uses DuckDB CLI directly
        "vista": None,  # Uses DuckDB CLI directly
    }

    def __init__(self, baseline: str, port: int = None, show_result: bool = False, iterations: int = None, output: str = None, init_file: str = None, timeout: int = 1800, excluding: str = None):
        self.baseline = baseline
        self.show_result = show_result
        self.iterations = iterations
        self.output = output
        self.init_file = init_file
        self.timeout = timeout

        # Parse excluding queries
        self.excluded_queries = set()
        if excluding:
            # Parse comma-separated numbers and convert to query names
            try:
                query_nums = [num.strip() for num in excluding.split(',')]
                self.excluded_queries = {f"Q{num}" for num in query_nums}
            except Exception as e:
                raise ValueError(f"Invalid --excluding format: {excluding}. Use comma-separated numbers like '5,10,21'")

        # Validate baseline
        if baseline not in self.VALID_BASELINES:
            raise ValueError(f"Invalid baseline: {baseline}. Must be one of {self.VALID_BASELINES}")

        # Check if baseline is implemented
        if baseline not in ["postgres_only", "pg_duckdb", "duckdb-postgres", "vista"]:
            raise NotImplementedError(f"Baseline '{baseline}' is not yet implemented")

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


class QueryParser:
    """Parses SQL queries from the benchmark file"""

    def __init__(self, query_file: str):
        self.query_file = query_file
        self.queries: Dict[str, str] = {}

    def parse(self) -> Dict[str, str]:
        """Parse queries from file, returning dict of query_name -> query_sql"""
        with open(self.query_file, 'r') as f:
            content = f.read()

        # Split by query comments (-- Q1, -- Q2, etc.)
        lines = content.split('\n')
        current_query_name = None
        current_query_lines = []

        for line in lines:
            # Check if this is a query header comment
            stripped = line.strip()
            if stripped.startswith('-- Q') and len(stripped) <= 6:
                # Save previous query if exists
                if current_query_name and current_query_lines:
                    query_sql = '\n'.join(current_query_lines).strip()
                    if query_sql:
                        self.queries[current_query_name] = query_sql

                # Start new query
                current_query_name = stripped[3:].strip()  # Extract "Q1", "Q2", etc.
                current_query_lines = []
            elif current_query_name:
                # Add line to current query
                current_query_lines.append(line)

        # Don't forget the last query
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
        self.output_event = threading.Event()  # Signal when new output arrives
        self.reader_thread = None

        # Get paths
        script_dir = Path(__file__).parent
        root_dir = script_dir.parent.parent  # supplementary-material

        # Set DuckDB path and init file based on baseline
        if config.baseline == "vista":
            self.duckdb_path = str(root_dir / "vista/duckdb-ex/build/release/duckdb")
            init_template = str(script_dir / "duckdb_init/init_vista.sql")
            extension_path = str(root_dir / "vista/duckdb-ex/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension")
        else:  # duckdb-postgres
            self.duckdb_path = str(root_dir / "duckdb-pg_ext/duckdb-postgres/build/release/duckdb")
            init_template = str(script_dir / "duckdb_init/init_duckdb_ext.sql")
            extension_path = str(root_dir / "duckdb-pg_ext/duckdb-postgres/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension")

        # Read template and replace placeholder
        with open(init_template, 'r') as f:
            init_content = f.read()
        self.init_commands = init_content.replace('{EXTENSION_PATH}', extension_path)
        self.init_file = init_template  # Keep for reference

    def connect(self):
        """Start DuckDB CLI process"""
        try:
            # Build command (no -init, we'll send commands via stdin)
            cmd = [self.duckdb_path, "-unsigned"]

            # Wrap with cgexec based on baseline
            if self.config.baseline == "vista":
                cgroup = "vista_nohp"
            else:
                cgroup = "vista_base"
            cmd = ["cgexec", "-g", f"cpu,cpuset,memory:{cgroup}"] + cmd

            # Start subprocess
            self.process = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )

            # Start output reader thread
            self.reader_thread = threading.Thread(target=self._read_output, daemon=True)
            self.reader_thread.start()

            # Wait a moment for DuckDB to start
            time.sleep(2)

            print(f"Started DuckDB CLI process (PID: {self.process.pid})")
            print(f"Running under cgroup: {cgroup}")

            # Send init commands via stdin
            print(f"Sending initialization commands...")
            self.process.stdin.write(self.init_commands)
            self.process.stdin.flush()
            time.sleep(3)  # Wait for init to complete
        except Exception as e:
            print(f"Failed to start DuckDB CLI: {e}")
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
                self.output_event.set()  # Signal that new output arrived

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

    def execute_init_queries(self, init_file: str):
        """Init queries are handled via -init flag, so this is a no-op"""
        pass

    def execute_query(self, query_name: str, query_sql: str) -> Tuple[float, str]:
        """Execute a query via stdin and measure execution time"""
        if not self.process or self.process.poll() is not None:
            return -1.0, "error"

        try:
            # Clear output buffer and reset event
            with self.output_lock:
                self.output_buffer.clear()
            self.output_event.clear()

            # Send query with timing commands
            # Use .timer on to measure query execution time
            commands = ".timer on\n" + query_sql + "\n.timer off\n"

            start_time = time.time()
            self.process.stdin.write(commands)
            self.process.stdin.flush()

            # Wait for query completion by detecting the prompt or timer output
            execution_time, status = self._wait_for_completion(query_name)

            if status == "error":
                # Query error occurred
                return -1.0, "error"
            elif execution_time < 0:
                # Timeout occurred - kill and restart the process
                print("  Timeout occurred - restarting DuckDB process...")
                self._restart_process()
                return -1.0, "timeout"

            return execution_time, "success"

        except Exception as e:
            print(f"Error executing query {query_name}: {e}")
            return -1.0, "error"

    def _cancel_query(self):
        """Cancel running query by sending SIGINT to DuckDB process

        When you press Ctrl+C in a terminal, the kernel sends SIGINT to the
        foreground process group. DuckDB CLI handles this by canceling the
        current query but staying alive. We replicate this behavior.
        If the process dies, restart it.
        """
        if self.process and self.process.poll() is None:
            try:
                print("  Cancelling query...")

                # Clear output buffer before sending signal
                with self.output_lock:
                    self.output_buffer.clear()
                self.output_event.clear()

                # Send SIGINT directly to the DuckDB process
                # This mimics what happens when you press Ctrl+C in terminal
                import os
                os.kill(self.process.pid, signal.SIGINT)

                # Wait for cancellation confirmation in output (max 10 seconds)
                start_wait = time.time()
                query_cancelled = False

                while time.time() - start_wait < 10.0:
                    # Wait for new output
                    self.output_event.wait(timeout=0.1)

                    with self.output_lock:
                        buffer_copy = self.output_buffer.copy()
                        self.output_buffer.clear()

                    self.output_event.clear()

                    # Look for prompt or completion indicators
                    for line in buffer_copy:
                        line_lower = line.lower()
                        # DuckDB might print "interrupted" or return to prompt
                        if 'interrupt' in line_lower or 'D ' in line or 'cancelled' in line_lower:
                            query_cancelled = True
                            break

                    if query_cancelled:
                        break

                    # Check if process died
                    if self.process.poll() is not None:
                        print("  DuckDB process died - restarting...")
                        self._restart_process()
                        return

                if query_cancelled:
                    print("  Query cancelled successfully")
                else:
                    print("  Query cancellation sent (no confirmation received)")

                # Verify process is still alive
                time.sleep(0.2)
                if self.process.poll() is None:
                    print("  DuckDB shell is still running")
                else:
                    print("  DuckDB shell died - restarting...")
                    self._restart_process()

            except Exception as e:
                print(f"  Error cancelling query: {e}")

    def _restart_process(self):
        """Restart the DuckDB CLI process"""
        try:
            # Kill the old process if it's still running
            if self.process and self.process.poll() is None:
                try:
                    self.process.terminate()
                    self.process.wait(timeout=2)
                except:
                    self.process.kill()
                    self.process.wait(timeout=1)

            # Clear output buffer
            with self.output_lock:
                self.output_buffer.clear()
            self.output_event.clear()

            # Build command (no -init, we'll send commands via stdin)
            cmd = [self.duckdb_path, "-unsigned"]

            # Wrap with cgexec based on baseline
            if self.config.baseline == "vista":
                cgroup = "vista_nohp"
            else:
                cgroup = "vista_base"
            cmd = ["cgexec", "-g", f"cpu,cpuset,memory:{cgroup}"] + cmd

            # Start new subprocess
            self.process = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )

            # Start new output reader thread
            self.reader_thread = threading.Thread(target=self._read_output, daemon=True)
            self.reader_thread.start()

            # Wait for DuckDB to start
            time.sleep(2)

            print(f"  DuckDB restarted (PID: {self.process.pid})")

            # Send init commands via stdin
            print(f"  Sending initialization commands...")
            self.process.stdin.write(self.init_commands)
            self.process.stdin.flush()
            time.sleep(3)  # Wait for init to complete

        except Exception as e:
            print(f"  ERROR: Failed to restart DuckDB: {e}")
            raise

    def _wait_for_completion(self, query_name: str, timeout: Optional[int] = None) -> Tuple[float, str]:
        """Wait for query completion and extract execution time
        Returns: (execution_time, status) where status is 'success', 'error', or 'timeout'
        """
        if timeout is None:
            timeout = self.config.timeout

        start_wait = time.time()
        execution_time = -1.0

        # Pattern to match DuckDB's timer output
        # Matches "Run Time (s): real X.XXX user Y.YYY sys Z.ZZZ" and extracts real time
        # Also matches older format "Run Time (s): X.XXX" for backward compatibility
        timer_pattern = re.compile(r'Run Time.*?:\s*(?:real\s+)?([\d.]+)')
        # Pattern to match DuckDB errors: "Error:", "ERROR:", "Binder Error:", "Parser Error:", etc.
        error_pattern = re.compile(r'(Error|ERROR|Exception):', re.IGNORECASE)

        while time.time() - start_wait < timeout:
            # Wait for new output with timeout
            remaining_time = timeout - (time.time() - start_wait)
            if remaining_time <= 0:
                break

            # Wait for event (blocking, but with timeout)
            # Returns immediately when new output arrives
            self.output_event.wait(timeout=min(remaining_time, 0.1))

            # Process and consume buffer lines
            with self.output_lock:
                buffer_copy = self.output_buffer.copy()
                self.output_buffer.clear()  # Clear after consuming

            self.output_event.clear()

            # Look for errors or timer output in the consumed lines
            for line in buffer_copy:
                # Check for errors first
                if error_pattern.search(line):
                    print(f"  Query error detected: {line.strip()}")
                    return -1.0, "error"

                # Check for timer output
                match = timer_pattern.search(line)
                if match:
                    execution_time = float(match.group(1))
                    return execution_time, "success"

        # Timeout occurred
        return -1.0, "timeout"


class DatabaseConnection:
    """Handles database connection and query execution"""

    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.conn = None
        self.cursor = None

    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                dbname=self.config.dbname,
                user=self.config.user,
                password=self.config.password
            )
            self.cursor = self.conn.cursor()

            # Set statement timeout (in milliseconds)
            timeout_ms = self.config.timeout * 1000
            self.cursor.execute(f"SET statement_timeout = {timeout_ms}")
            self.conn.commit()

            print(f"Connected to database on port {self.config.port}")
            print(f"Query timeout set to {self.config.timeout} seconds")
        except Exception as e:
            print(f"Failed to connect to database: {e}")
            sys.exit(1)

    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def execute_init_queries(self, init_file: str):
        """Execute initialization queries from a SQL file once"""
        try:
            with open(init_file, 'r') as f:
                init_sql = f.read()

            print(f"Executing initialization queries from {init_file}...")
            self.cursor.execute(init_sql)
            self.conn.commit()
            print("Initialization queries completed successfully")
        except FileNotFoundError:
            print(f"Error: Init file not found: {init_file}")
            sys.exit(1)
        except Exception as e:
            print(f"Error executing init queries: {e}")
            self.conn.rollback()
            sys.exit(1)

    def execute_query(self, query_name: str, query_sql: str) -> Tuple[float, str]:
        """Execute a query and return (execution time in seconds, status)
        Status can be: 'success', 'timeout', 'error'
        """
        # Prepare query with pg_duckdb prefix if needed
        if self.config.baseline == "pg_duckdb":
            full_query = "SET duckdb.force_execution = true;\n" + query_sql
        else:
            full_query = query_sql

        try:
            start_time = time.time()
            self.cursor.execute(full_query)
            # Fetch results to ensure query completes
            results = self.cursor.fetchall()
            end_time = time.time()

            # Show results if requested
            if self.config.show_result:
                print(f"\n--- Results for {query_name} ---")
                if results:
                    # Print column names
                    col_names = [desc[0] for desc in self.cursor.description]
                    print(" | ".join(col_names))
                    print("-" * 80)
                    # Print rows
                    for row in results:
                        print(" | ".join(str(val) for val in row))
                else:
                    print("No results returned")
                print("--- End of results ---\n")

            self.conn.commit()

            execution_time = end_time - start_time
            return execution_time, "success"
        except psycopg2.errors.QueryCanceled as e:
            # Timeout occurred
            self.conn.rollback()
            return -1.0, "timeout"
        except Exception as e:
            print(f"Error executing query {query_name}: {e}")
            self.conn.rollback()
            return -1.0, "error"


class BenchmarkExecutor:
    """Main benchmark execution logic"""

    def __init__(self, config: BenchmarkConfig, queries: Dict[str, str]):
        self.config = config
        self.queries = queries

        # Select appropriate connection type
        if config.baseline in ["duckdb-postgres", "vista"]:
            self.db = DuckDBPostgresConnection(config)
        else:
            self.db = DatabaseConnection(config)

        self.results: List[Dict] = []
        self.iteration_geomeans: List[Dict] = []

        # Determine output filename upfront for incremental export
        if self.config.output:
            self.output_filename = self.config.output
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.output_filename = f"benchmark_results_{self.config.baseline}_{timestamp}.json"

    def run(self):
        """Execute benchmark for specified iterations"""
        self.db.connect()

        # Execute init queries if provided
        if self.config.init_file:
            self.db.execute_init_queries(self.config.init_file)

        # Filter excluded queries
        query_names = [q for q in self.queries.keys() if q not in self.config.excluded_queries]

        if self.config.excluded_queries:
            excluded_list = sorted(self.config.excluded_queries)
            print(f"Excluding queries: {', '.join(excluded_list)}")

        iteration = 0

        # Determine execution mode
        if self.config.iterations is None:
            print("Error: --iteration is required")
            sys.exit(1)

        print(f"Total queries per set: {len(query_names)}")

        # Warm-up phase: run queries once before starting actual iterations
        print(f"\n=== Warm-up Phase ===")
        query_names_sorted = sorted(query_names, key=lambda x: int(x[1:]))

        for query_name in query_names_sorted:
            query_sql = self.queries[query_name]
            print(f"Warming up {query_name}...", end=" ")

            exec_time, status = self.db.execute_query(query_name, query_sql)

            if status == "success":
                print(f"{exec_time:.3f}s")
            elif status == "timeout":
                print("TIMEOUT")
            else:
                print("FAILED")

        print(f"\nWarm-up completed. Starting benchmark for {self.config.iterations} iterations...")
        max_iterations = self.config.iterations

        while True:
            # Check termination condition
            if iteration >= max_iterations:
                break

            iteration += 1
            print(f"\n=== Iteration {iteration} ===")

            # Execute queries in natural order (Q1, Q2, ..., Q22)
            query_names_sorted = sorted(query_names, key=lambda x: int(x[1:]))

            # Track execution times for this iteration
            iteration_times = []

            for query_name in query_names_sorted:
                query_sql = self.queries[query_name]
                print(f"Executing {query_name}...", end=" ")

                exec_time, status = self.db.execute_query(query_name, query_sql)

                if status == "success":
                    print(f"{exec_time:.3f}s")
                    self.results.append({
                        "iteration": iteration,
                        "query": query_name,
                        "execution_time": exec_time,
                        "status": status,
                        "timestamp": datetime.now().isoformat()
                    })
                    iteration_times.append(exec_time)
                elif status == "timeout":
                    print("TIMEOUT")
                    self.results.append({
                        "iteration": iteration,
                        "query": query_name,
                        "execution_time": -1,
                        "status": status,
                        "timestamp": datetime.now().isoformat()
                    })
                else:
                    print("FAILED")
                    self.results.append({
                        "iteration": iteration,
                        "query": query_name,
                        "execution_time": -1,
                        "status": status,
                        "timestamp": datetime.now().isoformat()
                    })

                # Export results incrementally after each query
                self.export_results_incremental()

            # Calculate and print geometric mean for this iteration
            if iteration_times:
                geomean = geometric_mean(iteration_times)
                print(f"Iteration {iteration} Geometric Mean: {geomean:.3f}s")
                self.iteration_geomeans.append({
                    "iteration": iteration,
                    "geometric_mean": geomean,
                    "successful_queries": len(iteration_times)
                })
            else:
                print(f"Iteration {iteration}: No successful queries")

        self.db.disconnect()
        print(f"\nBenchmark completed. Total executions: {len(self.results)}")

    def calculate_throughput_metrics(self) -> Dict:
        """Calculate QPS, QPM, QPH"""
        if len(self.results) == 0:
            return {}

        # Filter out error queries for throughput calculation
        successful_results = [r for r in self.results if r["status"] != "error"]

        if len(successful_results) == 0:
            return {}

        # Get actual elapsed time from first to last query
        timestamps = [datetime.fromisoformat(r["timestamp"]) for r in successful_results]
        if len(timestamps) < 2:
            return {}

        elapsed_seconds = (timestamps[-1] - timestamps[0]).total_seconds()
        if elapsed_seconds == 0:
            return {}

        total_queries = len(successful_results)

        return {
            "qps": total_queries / elapsed_seconds,
            "qpm": (total_queries / elapsed_seconds) * 60,
            "qph": (total_queries / elapsed_seconds) * 3600,
            "elapsed_seconds": elapsed_seconds,
            "total_queries": total_queries,
            "error_count": len([r for r in self.results if r["status"] == "error"])
        }

    def export_results_incremental(self):
        """Export current results to JSON file (called after each query)"""
        throughput = self.calculate_throughput_metrics()

        output = {
            "metadata": {
                "baseline": self.config.baseline,
                "port": self.config.port,
                "iterations": self.config.iterations,
                "total_executions": len(self.results),
                "timestamp": datetime.now().isoformat()
            },
            "results": self.results,
            "iteration_geomeans": self.iteration_geomeans
        }

        if throughput:
            output["throughput"] = throughput

        with open(self.output_filename, 'w') as f:
            json.dump(output, f, indent=2)

    def export_results(self):
        """Export results to JSON file with timestamp"""
        throughput = self.calculate_throughput_metrics()

        output = {
            "metadata": {
                "baseline": self.config.baseline,
                "port": self.config.port,
                "iterations": self.config.iterations,
                "total_executions": len(self.results),
                "timestamp": datetime.now().isoformat()
            },
            "results": self.results,
            "iteration_geomeans": self.iteration_geomeans
        }

        if throughput:
            output["throughput"] = throughput
            print(f"\nThroughput Metrics:")
            print(f"  QPS (Queries Per Second):  {throughput['qps']:.2f}")
            print(f"  QPM (Queries Per Minute):  {throughput['qpm']:.2f}")
            print(f"  QPH (Queries Per Hour):    {throughput['qph']:.2f}")
            print(f"  Elapsed Time: {throughput['elapsed_seconds']:.2f}s")
            print(f"  Successful Queries: {throughput['total_queries']}")
            if throughput['error_count'] > 0:
                print(f"  Error Queries (excluded from throughput): {throughput['error_count']}")

        with open(self.output_filename, 'w') as f:
            json.dump(output, f, indent=2)

        print(f"\nResults exported to {self.output_filename}")


def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(description="CH-Benchmark OLAP Query Runner")
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
        "--show-result",
        action="store_true",
        help="Show query results on screen instead of discarding them"
    )
    parser.add_argument(
        "--iteration",
        type=int,
        required=True,
        help="Number of iterations to run"
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output JSON file path (default: auto-generated with timestamp)"
    )
    parser.add_argument(
        "--init",
        type=str,
        help="SQL file with initialization queries to run once before benchmark"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=1800,
        help="Query timeout in seconds (default: 1800 = 30 minutes)"
    )
    parser.add_argument(
        "--excluding",
        type=str,
        help="Comma-separated query numbers to exclude (e.g., '5,10,21' to exclude Q5, Q10, Q21)"
    )

    return parser.parse_args()


def main():
    """Main entry point"""
    # Parse arguments
    args = parse_arguments()

    try:
        # Create configuration
        config = BenchmarkConfig(
            baseline=args.baseline,
            port=args.port,
            show_result=args.show_result,
            iterations=args.iteration,
            output=args.output,
            init_file=args.init,
            timeout=args.timeout,
            excluding=args.excluding
        )

        # Parse queries
        query_file = Path(__file__).parent / "ch-benchmark-queries.sql"
        parser = QueryParser(str(query_file))
        queries = parser.parse()

        if not queries:
            print("Error: No queries found in query file")
            sys.exit(1)

        print(f"Loaded {len(queries)} queries")

        # Execute benchmark
        executor = BenchmarkExecutor(config, queries)
        executor.run()

        # Export results
        executor.export_results()

    except (ValueError, NotImplementedError) as e:
        print(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
