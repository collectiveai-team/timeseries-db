"""
Bulk IO Adapter Example and Performance Benchmark

This script demonstrates how to use the BulkIOAdapter for both DuckDB and
TimescaleDB and provides a basic performance benchmark for write and read
operations.

The BulkIOAdapter provides a unified, high-performance interface for bulk
data operations across different database backends, leveraging native features
like DuckDB's Arrow integration and TimescaleDB's COPY command.

To run the TimescaleDB example, ensure the database is running and the
`TIMESCALE_DSN` environment variable is set correctly. For example:

export TIMESCALE_DSN="postgresql://tsdb_user:tsdb_password@localhost:5432/tsdb"

"""

import datetime
import os
import random
import time
from typing import Iterator

import duckdb
import psycopg
from pydantic import BaseModel

from tsdb.io import get_bulk_io_adapter


# --- 1. Define a Pydantic Model ---
class SensorData(BaseModel):
    ts: datetime.datetime
    sensor_id: int
    value: float


# --- 2. Helper function to generate sample data ---
def generate_sensor_data(n: int) -> Iterator[SensorData]:
    """Generate n sample SensorData records."""
    for i in range(n):
        yield SensorData(
            ts=datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(seconds=i),
            sensor_id=random.randint(1, 10),
            value=random.random() * 100,
        )


# --- 3. DuckDB Example & Benchmark ---
def run_duckdb_example(num_records: int = 250_000):
    """Demonstrates bulk IO with an in-memory DuckDB database."""
    print("--- Running DuckDB Bulk IO Example & Benchmark ---")
    con = duckdb.connect(":memory:")

    # Create table
    con.execute(
        """CREATE TABLE sensor_data (
            ts TIMESTAMP,
            sensor_id INTEGER,
            value DOUBLE
        );"""
    )

    # Get adapter
    adapter = get_bulk_io_adapter(con, SensorData)

    # --- Write Benchmark ---
    print(f"Writing {num_records:,} records to DuckDB...")
    data_to_write = generate_sensor_data(num_records)
    start_time = time.perf_counter()
    adapter.write_bulk(table_name="sensor_data", data=data_to_write, batch_size=50_000)
    end_time = time.perf_counter()
    write_duration = end_time - start_time
    records_per_sec = num_records / write_duration
    print(
        f"Write complete in {write_duration:.2f}s ({records_per_sec:,.0f} records/sec)."
    )

    # Verify count
    count = con.execute("SELECT COUNT(*) FROM sensor_data").fetchone()[0]
    print(f"Total records in DuckDB: {count:,}")

    # --- Read Benchmark ---
    print("Reading records from DuckDB...")
    start_time = time.perf_counter()
    read_count = 0
    for _ in adapter.read_iter("SELECT * FROM sensor_data"):
        read_count += 1
    end_time = time.perf_counter()
    read_duration = end_time - start_time
    records_per_sec = read_count / read_duration
    print(
        f"Successfully read {read_count:,} records in {read_duration:.2f}s ({records_per_sec:,.0f} records/sec)."
    )

    con.close()
    print("--- DuckDB Example Complete ---\n")


# --- 4. TimescaleDB/Postgres Example & Benchmark ---
def run_timescaledb_example(num_records: int = 250_000):
    """Demonstrates bulk IO with TimescaleDB."""
    print("--- Running TimescaleDB Bulk IO Example & Benchmark ---")
    dsn = os.environ.get(
        "TIMESCALE_DSN", "postgresql://tsdb_user:tsdb_password@localhost:5432/tsdb"
    )
    try:
        with psycopg.connect(dsn) as con:
            with con.cursor() as cur:
                # Create table
                cur.execute("DROP TABLE IF EXISTS sensor_data;")
                cur.execute(
                    """CREATE TABLE sensor_data (
                        ts TIMESTAMPTZ NOT NULL,
                        sensor_id INTEGER,
                        value DOUBLE PRECISION
                    );"""
                )
                # Optional: Create hypertable for better performance
                try:
                    cur.execute("SELECT create_hypertable('sensor_data', 'ts');")
                    print("Successfully created TimescaleDB hypertable.")
                except psycopg.errors.DuplicateTable:
                    pass  # Hypertable already exists
                except psycopg.errors.lookup("58P01"):  # undefined_file
                    print(
                        "TimescaleDB extension not found. Proceeding with standard table."
                    )

            # Get adapter
            adapter = get_bulk_io_adapter(con, SensorData)

            # --- Write Benchmark ---
            print(f"Writing {num_records:,} records to TimescaleDB...")
            data_to_write = generate_sensor_data(num_records)
            start_time = time.perf_counter()
            adapter.write_bulk(
                table_name="sensor_data", data=data_to_write, batch_size=50_000
            )
            end_time = time.perf_counter()
            write_duration = end_time - start_time
            records_per_sec = num_records / write_duration
            print(
                f"Write complete in {write_duration:.2f}s ({records_per_sec:,.0f} records/sec)."
            )

            # Verify count
            with con.cursor() as cur:
                count = cur.execute("SELECT COUNT(*) FROM sensor_data").fetchone()[0]
                print(f"Total records in TimescaleDB: {count:,}")

            # --- Read Benchmark ---
            print("Reading records from TimescaleDB...")
            start_time = time.perf_counter()
            read_count = 0
            for _ in adapter.read_iter(
                "SELECT * FROM sensor_data ORDER BY ts DESC", itersize=10_000
            ):
                read_count += 1
            end_time = time.perf_counter()
            read_duration = end_time - start_time
            records_per_sec = read_count / read_duration
            print(
                f"Successfully read {read_count:,} records in {read_duration:.2f}s ({records_per_sec:,.0f} records/sec)."
            )

    except psycopg.OperationalError as e:
        print(f"Could not connect to TimescaleDB: {e}")
        print("Please ensure the database is running and the DSN is correct.")
        print("(You can set the TIMESCALE_DSN environment variable)")
    finally:
        print("--- TimescaleDB Example Complete ---\n")


if __name__ == "__main__":
    RECORDS_TO_BENCHMARK = int(os.environ.get("BENCHMARK_RECORDS", "250000"))
    run_duckdb_example(RECORDS_TO_BENCHMARK)
    run_timescaledb_example(RECORDS_TO_BENCHMARK)
