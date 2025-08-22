import datetime
import os
import random
from typing import Iterator

import duckdb
import psycopg
import pytest
from pydantic import BaseModel

from tsdb.io import get_bulk_io_adapter


# --- 1. Model and Test Data Generation ---
class SensorData(BaseModel):
    ts: datetime.datetime
    sensor_id: int
    value: float


def generate_sensor_data(n: int) -> Iterator[SensorData]:
    """Generate n sample SensorData records."""
    for i in range(n):
        yield SensorData(
            ts=datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(seconds=i),
            sensor_id=random.randint(1, 10),
            value=random.random() * 100,
        )


# --- 2. Fixtures ---
@pytest.fixture(scope="module")
def duckdb_conn():
    """Fixture for an in-memory DuckDB connection."""
    con = duckdb.connect(":memory:")
    con.execute(
        """CREATE TABLE sensor_data (
            ts TIMESTAMP,
            sensor_id INTEGER,
            value DOUBLE
        );"""
    )
    yield con
    con.close()


@pytest.fixture(scope="module")
def timescaledb_conn():
    """Fixture for a TimescaleDB connection."""
    dsn = os.environ.get(
        "TIMESCALE_DSN", "postgresql://postgres:password@localhost:5432/testdb"
    )
    try:
        with psycopg.connect(dsn) as con:
            with con.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS sensor_data;")
                cur.execute(
                    """CREATE TABLE sensor_data (
                        ts TIMESTAMPTZ NOT NULL,
                        sensor_id INTEGER,
                        value DOUBLE PRECISION
                    );"""
                )
            yield con
    except psycopg.OperationalError:
        pytest.skip("TimescaleDB not available")


# --- 3. Tests ---


def test_get_adapter_factory(duckdb_conn, timescaledb_conn):
    """Test that the get_bulk_io_adapter factory returns the correct adapter."""
    from tsdb.io.duckdb import DuckDBBulkIOAdapter
    from tsdb.io.timescaledb import TimescaleDBBulkIOAdapter

    duckdb_adapter = get_bulk_io_adapter(duckdb_conn, SensorData)
    assert isinstance(duckdb_adapter, DuckDBBulkIOAdapter)

    timescaledb_adapter = get_bulk_io_adapter(timescaledb_conn, SensorData)
    assert isinstance(timescaledb_adapter, TimescaleDBBulkIOAdapter)

    with pytest.raises(NotImplementedError):
        get_bulk_io_adapter("not a connection", SensorData)


@pytest.mark.parametrize("db_fixture", ["duckdb_conn", "timescaledb_conn"])
def test_bulk_io_write_and_read(db_fixture, request):
    """Test bulk write and iterative read for both databases."""
    conn = request.getfixturevalue(db_fixture)
    adapter = get_bulk_io_adapter(conn, SensorData)
    table_name = "sensor_data"
    num_records = 10_000

    # Write data
    data_to_write = list(generate_sensor_data(num_records))
    adapter.write_bulk(table_name=table_name, data=data_to_write, batch_size=2_500)

    # Verify count
    with conn.cursor() as cur:
        count = cur.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        assert count == num_records

    # Read and verify data
    read_records = list(adapter.read_iter(f"SELECT * FROM {table_name}"))
    assert len(read_records) == num_records
    assert all(isinstance(r, SensorData) for r in read_records)

    # Clean up
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {table_name}")
