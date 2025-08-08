"""
Test configuration and fixtures for the TimescaleDB CRUD system.
"""

import pytest
from datetime import datetime
from typing import Generator
from unittest.mock import Mock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool
from pydantic import BaseModel

from tsdb.decorators.pydantic_decorator import Base, timescale_crud


@pytest.fixture(scope="session")
def in_memory_engine():
    """Create an in-memory SQLite engine for testing."""
    engine = create_engine(
        "sqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
        echo=False,
    )
    return engine


@pytest.fixture(scope="function")
def db_session(in_memory_engine) -> Generator[Session, None, None]:
    """Create a database session for testing."""
    # Create all tables
    Base.metadata.create_all(in_memory_engine)

    # Create session
    SessionLocal = sessionmaker(
        autocommit=False, autoflush=False, bind=in_memory_engine
    )
    session = SessionLocal()

    try:
        yield session
    finally:
        session.close()
        # Clean up tables
        Base.metadata.drop_all(in_memory_engine)


@pytest.fixture
def mock_session():
    """Create a mock SQLAlchemy session."""
    session = Mock(spec=Session)
    session.add = Mock()
    session.commit = Mock()
    session.rollback = Mock()
    session.refresh = Mock()
    session.close = Mock()
    session.execute = Mock()
    session.scalars = Mock()
    session.get = Mock()
    return session


@pytest.fixture
def sample_timeseries_model():
    """Create a sample Pydantic model for testing."""

    @timescale_crud(
        table_name="sensor_data",
        time_column="timestamp",
        create_hypertable=True,
        chunk_time_interval="1 hour",
    )
    class SensorData(BaseModel):
        id: int | None = None
        sensor_id: str
        temperature: float
        humidity: float
        timestamp: datetime
        location: str | None = None

    return SensorData


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


@pytest.fixture
def mock_sql_model():
    """Create a mock SQLAlchemy model instance."""
    mock_instance = Mock()
    mock_instance.id = 1
    mock_instance.sensor_id = "sensor_001"
    mock_instance.temperature = 23.5
    mock_instance.humidity = 65.2
    mock_instance.timestamp = datetime(2024, 1, 1, 12, 0, 0)
    mock_instance.location = "Room A"
    mock_instance.created_at = datetime(2024, 1, 1, 12, 0, 0)
    mock_instance.updated_at = datetime(2024, 1, 1, 12, 0, 0)
    return mock_instance


@pytest.fixture
def mock_engine():
    """Create a mock SQLAlchemy engine."""
    engine = Mock()
    engine.connect = Mock()
    engine.execute = Mock()
    return engine


class MockResult:
    """Mock SQLAlchemy result object."""

    def __init__(self, data=None):
        self.data = data or []

    def scalars(self):
        return MockScalars(self.data)

    def first(self):
        return self.data[0] if self.data else None

    def all(self):
        return self.data


class MockScalars:
    """Mock SQLAlchemy scalars object."""

    def __init__(self, data):
        self.data = data

    def all(self):
        return self.data

    def first(self):
        return self.data[0] if self.data else None


@pytest.fixture
def mock_result():
    """Create a mock SQLAlchemy result."""
    return MockResult
