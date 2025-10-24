#!/usr/bin/env python3
import argparse
import subprocess
import os
import sys
from pathlib import Path
from typing import Optional


class TPCCConfig:
    """Manages TPC-C configuration file modification."""

    def __init__(self, tpcc_tcl_path: str):
        self.tpcc_tcl_path = tpcc_tcl_path

    def update_config(self, workers: int, port: int) -> None:
        """Update TPC-C configuration file with worker count and port."""
        # Update port (line 12)
        subprocess.run([
            'sed', '-i',
            f's/^diset connection pg_port.*/diset connection pg_port {port}/',
            self.tpcc_tcl_path
        ], check=True)

        # Update VU count (line 29)
        subprocess.run([
            'sed', '-i',
            f's/^vuset vu.*/vuset vu {workers}/',
            self.tpcc_tcl_path
        ], check=True)


class TPCCRunner:
    """Manages TPC-C workload execution."""

    def __init__(self, hammerdb_dir: str, tpcc_script: str):
        self.hammerdb_dir = hammerdb_dir
        self.tpcc_script = tpcc_script

    def run(self, output: Optional[str] = None) -> subprocess.Popen:
        """Execute TPC-C workload in HammerDB."""
        cmd = ['./hammerdbcli', 'auto', self.tpcc_script]

        if output:
            # Redirect stdout and stderr to output file
            output_file = open(output, 'w')
            process = subprocess.Popen(
                cmd,
                cwd=self.hammerdb_dir,
                stdout=output_file,
                stderr=subprocess.STDOUT
            )
            # Store file handle to close later
            process._output_file = output_file
        else:
            process = subprocess.Popen(
                cmd,
                cwd=self.hammerdb_dir
            )
        return process


class OLAPRunner:
    """Manages OLAP workload execution."""

    def __init__(self, olap_dir: str):
        self.olap_dir = olap_dir

    def run(self, iteration: int, baseline: str, excluding: str = '', output: Optional[str] = None) -> subprocess.Popen:
        """Execute OLAP workload."""
        if baseline == 'pgonly':
            baseline = 'postgres_only'
        elif baseline == 'duckdb_ext':
            baseline = 'duckdb-postgres'
        elif baseline == 'pg_duckdb':
            baseline = 'pg_duckdb'
        elif baseline == 'vista':
            baseline = 'vista'
        else:
            raise ValueError(f"Unknown baseline: {baseline}")

        cmd = ['./run_olap.py', '--baseline', baseline, '--iteration', str(iteration)]
        if excluding:
            cmd.extend(['--excluding', excluding])
        if output:
            cmd.extend(['--output', output])
        process = subprocess.Popen(
            cmd,
            cwd=self.olap_dir
        )
        return process


class HTAPBenchmark:
    """Orchestrates HTAP benchmark execution."""

    # Baseline to port mapping
    BASELINE_PORTS = {
        'pgonly': 5003,
        'duckdb_ext': 5001,
        'pg_duckdb': 5002,
        'vista': 5004
    }

    # Baseline to excluding queries mapping
    BASELINE_EXCLUDING = {
        'pgonly': '',
        'duckdb_ext': '5,21',
        'pg_duckdb': '5,21',
        'vista': '21'
    }

    def __init__(self, args):
        self.iteration = args.iteration
        self.tpcc_workers = args.tpcc_worker
        self.output = args.output
        self.baseline = args.baseline

        # Get port and excluding queries for the baseline
        self.port = self.BASELINE_PORTS[self.baseline]
        self.excluding = self.BASELINE_EXCLUDING[self.baseline]

        # Resolve paths
        script_dir = Path(__file__).parent
        base_dir = script_dir.parent.parent  # supplementary-material
        self.tpcc_tcl_path = str(script_dir / 'tpcc.tcl')
        self.hammerdb_dir = str(base_dir / 'citus-benchmark' / 'HammerDB-4.4')
        self.olap_dir = str(script_dir)

        # Initialize components
        self.tpcc_config = TPCCConfig(self.tpcc_tcl_path)
        self.tpcc_runner = TPCCRunner(
            self.hammerdb_dir,
            self.tpcc_tcl_path
        )
        self.olap_runner = OLAPRunner(self.olap_dir)

    def run(self) -> int:
        """Execute HTAP benchmark."""
        try:
            tpcc_process = None

            # Start TPC-C workload only if workers > 0
            if self.tpcc_workers > 0:
                # Update TPC-C configuration
                print(f"Updating TPC-C config: baseline={self.baseline}, port={self.port}, workers={self.tpcc_workers}")
                self.tpcc_config.update_config(self.tpcc_workers, self.port)

                # Start TPC-C workload
                print("Starting TPC-C workload...")
                tpcc_process = self.tpcc_runner.run()
            else:
                print("Skipping TPC-C workload (workers=0)")

            # Start OLAP workload
            print("Starting OLAP workload...")
            if self.excluding:
                print(f"Excluding queries: {self.excluding}")
            if self.output:
                print(f"OLAP output will be saved to: {self.output}")
            olap_process = self.olap_runner.run(iteration=self.iteration, baseline=self.baseline, excluding=self.excluding, output=self.output)

            # Wait for OLAP to complete
            print("Waiting for OLAP workload to complete...")
            olap_process.wait()

            # Terminate TPC-C if it's still running
            if tpcc_process and tpcc_process.poll() is None:
                print("OLAP completed. Terminating TPC-C workload...")
                tpcc_process.terminate()
                try:
                    tpcc_process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    print("TPC-C didn't terminate gracefully, killing...")
                    tpcc_process.kill()
                    tpcc_process.wait()

            # Check results
            return_codes = []

            if tpcc_process:
                if tpcc_process.returncode != 0 and tpcc_process.returncode != -15:  # -15 is SIGTERM
                    print(f"TPC-C failed with return code {tpcc_process.returncode}", file=sys.stderr)
                    return_codes.append(tpcc_process.returncode)
                else:
                    print("TPC-C completed successfully")
                    return_codes.append(0)

            if olap_process.returncode != 0:
                print(f"OLAP failed with return code {olap_process.returncode}", file=sys.stderr)
                return_codes.append(olap_process.returncode)
            else:
                print("OLAP completed successfully")
                return_codes.append(0)

            return max(return_codes) if return_codes else 0

        except Exception as e:
            print(f"Error during benchmark execution: {e}", file=sys.stderr)
            return 1


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Run HTAP benchmark with TPC-C and OLAP workloads'
    )
    parser.add_argument(
        '--baseline',
        type=str,
        required=True,
        choices=['pgonly', 'duckdb_ext', 'pg_duckdb', 'vista'],
        help='Baseline to use (pgonly=5003, duckdb_ext=5001, pg_duckdb=5002, vista=5004)'
    )
    parser.add_argument(
        '--iteration',
        type=int,
        required=True,
        help='Number of iterations to run'
    )
    parser.add_argument(
        '--tpcc-worker',
        type=int,
        required=True,
        help='Number of TPC-C workers'
    )
    parser.add_argument(
        '--output',
        type=str,
        help='Output file path for OLAP results (JSON format)'
    )
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    benchmark = HTAPBenchmark(args)
    return benchmark.run()


if __name__ == '__main__':
    sys.exit(main())
