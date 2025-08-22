#!/usr/bin/env python3
"""
Test runner script for the TimescaleDB CRUD system.

This script provides convenient ways to run different types of tests:
- Unit tests (with mocks)
- Integration tests (requires running TimescaleDB)
- All tests
"""

import os
import sys
import subprocess
import argparse
from pathlib import Path


def run_command(cmd: list[str], cwd: Path = None) -> int:
    """Run a command and return the exit code."""
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd)
    return result.returncode


def check_docker_compose():
    """Check if Docker Compose is available and TimescaleDB is running."""
    try:
        result = subprocess.run(
            ["docker", "compose", "ps", "timescaledb"], capture_output=True, text=True
        )
        return "Up" in result.stdout
    except subprocess.CalledProcessError:
        return False


def main():
    """Main test runner function."""
    parser = argparse.ArgumentParser(
        description="Run tests for TimescaleDB CRUD system"
    )
    parser.add_argument(
        "test_type", choices=["unit", "integration", "all"], help="Type of tests to run"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Run tests in verbose mode"
    )
    parser.add_argument(
        "--coverage", action="store_true", help="Run tests with coverage report"
    )
    parser.add_argument(
        "--start-db",
        action="store_true",
        help="Start TimescaleDB using Docker Compose before running integration tests",
    )

    args = parser.parse_args()

    # Get project root directory
    project_root = Path(__file__).parent.parent

    # Base pytest command
    pytest_cmd = ["python", "-m", "pytest"]

    if args.verbose:
        pytest_cmd.append("-v")

    if args.coverage:
        pytest_cmd.extend(["--cov=tsdb", "--cov-report=html", "--cov-report=term"])

    # Set environment variables
    env = os.environ.copy()
    env["PYTHONPATH"] = str(project_root)

    exit_code = 0

    if args.test_type in ["unit", "all"]:
        print("=" * 60)
        print("Running Unit Tests")
        print("=" * 60)

        unit_cmd = pytest_cmd + ["tests/unit/"]
        result = subprocess.run(unit_cmd, cwd=project_root, env=env)
        exit_code = max(exit_code, result.returncode)

    if args.test_type in ["integration", "all"]:
        print("=" * 60)
        print("Running Integration Tests")
        print("=" * 60)

        # Check if we should start the database
        if args.start_db:
            print("Starting TimescaleDB with Docker Compose...")
            start_result = run_command(
                ["docker", "compose", "up", "-d", "timescaledb"], cwd=project_root
            )
            if start_result != 0:
                print("Failed to start TimescaleDB")
                return 1

            # Wait for database to be ready
            print("Waiting for TimescaleDB to be ready...")
            import time

            for _ in range(30):  # Wait up to 30 seconds
                if check_docker_compose():
                    break
                time.sleep(1)
            else:
                print("TimescaleDB did not start in time")
                return 1

        # Check if TimescaleDB is running
        if not check_docker_compose():
            print("Warning: TimescaleDB is not running. Integration tests may fail.")
            print("Start TimescaleDB with: docker compose up -d timescaledb")
            print("Or use --start-db flag to start it automatically.")

        # Set test database URL
        env["TEST_TSDB_DATABASE_URI"] = (
            "postgresql://test_user:test_password@localhost:5432/tsdb"
        )

        integration_cmd = pytest_cmd + ["tests/integration/"]
        result = subprocess.run(integration_cmd, cwd=project_root, env=env)
        exit_code = max(exit_code, result.returncode)

    if exit_code == 0:
        print("=" * 60)
        print("All tests passed! ✅")
        print("=" * 60)
    else:
        print("=" * 60)
        print("Some tests failed! ❌")
        print("=" * 60)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
