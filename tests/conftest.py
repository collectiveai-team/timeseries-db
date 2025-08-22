"""
Test configuration and fixtures for the TimescaleDB CRUD system.
"""

import pytest
from datetime import datetime
from typing import Generator
from pydantic import BaseModel

from tsdb.decorators.pydantic_decorator import db_crud
from tsdb.connectors.duckdb import DuckDBConnector


@pytest.fixture(scope="function")
def duckdb_session() -> Generator[DuckDBConnector, None, None]:
    """Create a DuckDB session for testing."""
    connector = DuckDBConnector(model=BaseModel, config={"db_path": ":memory:"})
    connector.connect()
    yield connector
    connector.disconnect()


@pytest.fixture
def sample_timeseries_model():
    """Create a sample Pydantic model for testing with DuckDB."""

    @db_crud(
        db_type="duckdb",
        table_name="sensor_data",
        time_column="timestamp",
    )
    class SensorData(BaseModel):
        id: int | None = None
        sensor_id: str
        temperature: float
        humidity: float
        timestamp: datetime
        location: str | None = None

    # The decorator replaces the class, so we need to get the connector from the new class
    connector = SensorData._get_connector()
    # Manually create the table for the test session
    connector.create_table()

    yield SensorData

    # Cleanup: drop the table
    try:
        conn = connector._get_connection()
        conn.execute(f"DROP TABLE IF EXISTS {connector._get_table_name()}")
    except Exception:
        pass  # Ignore cleanup errors


@pytest.fixture
def sample_data():
    """Sample data for testing."""
    return {
        "sensor_id": "sensor_001",
        "temperature": 23.5,
        "humidity": 65.2,
        "timestamp": datetime(2024, 1, 1, 12, 0, 0),
        "location": "Room A",
    }
