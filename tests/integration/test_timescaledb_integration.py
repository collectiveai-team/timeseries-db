"""
Integration tests for TimescaleDB CRUD operations.

These tests require a running TimescaleDB instance and test the full integration
with the database. They can be run against the Docker Compose setup.
"""

import pytest
import os
from datetime import datetime, timedelta
from typing import Optional

from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from tsdb.decoreators.pydantic_decorator import timescale_crud


# Skip integration tests if no database URL is provided
DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL", "postgresql://test_user:test_password@localhost:5432/tsdb"
)
SKIP_INTEGRATION = os.getenv("SKIP_INTEGRATION_TESTS", "false").lower() == "true"


@pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled")
class TestTimescaleDBIntegration:
    """Integration tests for TimescaleDB operations."""

    @pytest.fixture(scope="class")
    def db_engine(self):
        """Create database engine for integration tests."""
        try:
            engine = create_engine(DATABASE_URL, echo=False)
            # Test connection
            with engine.connect() as conn:
                conn.execute("SELECT 1")
            return engine
        except Exception as e:
            pytest.skip(f"Cannot connect to test database: {e}")

    @pytest.fixture(scope="class")
    def sensor_model(self, db_engine):
        """Create sensor model with TimescaleDB integration."""

        @timescale_crud(
            table_name="integration_sensor_data",
            time_column="timestamp",
            create_hypertable=True,
            chunk_time_interval="1 hour",
        )
        class SensorData(BaseModel):
            id: Optional[int] = None
            sensor_id: str
            temperature: float
            humidity: float
            timestamp: datetime
            location: Optional[str] = None

        # Initialize database
        SensorData.init_db(db_engine)

        # Create session
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db_engine)
        session = SessionLocal()
        SensorData.set_session(session)

        yield SensorData

        # Cleanup
        session.close()
        # Drop test table
        try:
            with db_engine.connect() as conn:
                conn.execute("DROP TABLE IF EXISTS integration_sensor_data CASCADE")
                conn.commit()
        except Exception:
            pass  # Ignore cleanup errors

    def test_create_and_retrieve_record(self, sensor_model):
        """Test creating and retrieving a record."""
        # Create test data
        test_data = {
            "sensor_id": "sensor_integration_001",
            "temperature": 23.5,
            "humidity": 65.2,
            "timestamp": datetime.now(),
            "location": "Integration Test Room",
        }

        # Create record
        created_record = sensor_model.create(test_data)
        assert created_record.id is not None
        assert created_record.sensor_id == test_data["sensor_id"]
        assert created_record.temperature == test_data["temperature"]

        # Retrieve record
        retrieved_record = sensor_model.get_by_id(created_record.id)
        assert retrieved_record is not None
        assert retrieved_record.sensor_id == test_data["sensor_id"]
        assert retrieved_record.temperature == test_data["temperature"]

    def test_list_records_with_pagination(self, sensor_model):
        """Test listing records with pagination."""
        # Create multiple test records
        base_time = datetime.now()
        test_records = []

        for i in range(5):
            test_data = {
                "sensor_id": f"sensor_list_{i:03d}",
                "temperature": 20.0 + i,
                "humidity": 60.0 + i,
                "timestamp": base_time + timedelta(minutes=i),
                "location": f"Room {i}",
            }
            created = sensor_model.create(test_data)
            test_records.append(created)

        # Test listing with limit
        records = sensor_model.list(limit=3, offset=0)
        assert len(records) <= 3

        # Test listing with offset
        records_offset = sensor_model.list(limit=3, offset=2)
        assert len(records_offset) <= 3

        # Test filtering
        filtered_records = sensor_model.list(filters={"sensor_id": "sensor_list_001"})
        assert len(filtered_records) >= 1
        assert all(r.sensor_id == "sensor_list_001" for r in filtered_records)

    def test_update_record(self, sensor_model):
        """Test updating a record."""
        # Create test record
        test_data = {
            "sensor_id": "sensor_update_001",
            "temperature": 23.5,
            "humidity": 65.2,
            "timestamp": datetime.now(),
            "location": "Update Test Room",
        }

        created_record = sensor_model.create(test_data)
        original_updated_at = created_record.updated_at

        # Update record
        update_data = {"temperature": 25.0, "humidity": 70.0}
        updated_record = sensor_model.update(created_record.id, update_data)

        assert updated_record.temperature == 25.0
        assert updated_record.humidity == 70.0
        assert updated_record.updated_at > original_updated_at

        # Verify update persisted
        retrieved_record = sensor_model.get_by_id(created_record.id)
        assert retrieved_record.temperature == 25.0
        assert retrieved_record.humidity == 70.0

    def test_delete_record(self, sensor_model):
        """Test deleting a record."""
        # Create test record
        test_data = {
            "sensor_id": "sensor_delete_001",
            "temperature": 23.5,
            "humidity": 65.2,
            "timestamp": datetime.now(),
            "location": "Delete Test Room",
        }

        created_record = sensor_model.create(test_data)
        record_id = created_record.id

        # Delete record
        result = sensor_model.delete(record_id, hard_delete=True)
        assert result is True

        # Verify record is deleted
        deleted_record = sensor_model.get_by_id(record_id)
        assert deleted_record is None

    def test_count_records(self, sensor_model):
        """Test counting records."""
        # Get initial count
        initial_count = sensor_model.count()

        # Create test records
        base_time = datetime.now()
        for i in range(3):
            test_data = {
                "sensor_id": f"sensor_count_{i:03d}",
                "temperature": 20.0 + i,
                "humidity": 60.0 + i,
                "timestamp": base_time + timedelta(minutes=i),
                "location": f"Count Room {i}",
            }
            sensor_model.create(test_data)

        # Test total count
        new_count = sensor_model.count()
        assert new_count >= initial_count + 3

        # Test filtered count
        filtered_count = sensor_model.count(filters={"location": "Count Room 1"})
        assert filtered_count >= 1

    def test_timeseries_queries(self, sensor_model):
        """Test time-series specific queries."""
        # Create time-series data
        base_time = datetime.now() - timedelta(hours=2)

        for i in range(10):
            test_data = {
                "sensor_id": "sensor_timeseries_001",
                "temperature": 20.0 + (i * 0.5),
                "humidity": 60.0 + (i * 2),
                "timestamp": base_time + timedelta(minutes=i * 10),
                "location": "Timeseries Test Room",
            }
            sensor_model.create(test_data)

        # Test ordering by timestamp
        records = sensor_model.list(
            filters={"sensor_id": "sensor_timeseries_001"},
            order_by="timestamp",
            order_desc=False,
            limit=5,
        )

        assert len(records) <= 5
        # Verify ordering
        if len(records) > 1:
            for i in range(1, len(records)):
                assert records[i].timestamp >= records[i - 1].timestamp


@pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled")
class TestTimescaleDBHypertables:
    """Test TimescaleDB hypertable functionality."""

    @pytest.fixture(scope="class")
    def db_engine(self):
        """Create database engine for hypertable tests."""
        try:
            engine = create_engine(DATABASE_URL, echo=False)
            # Test connection and TimescaleDB extension
            with engine.connect() as conn:
                result = conn.execute(
                    "SELECT extname FROM pg_extension WHERE extname = 'timescaledb'"
                )
                if not result.fetchone():
                    pytest.skip("TimescaleDB extension not available")
            return engine
        except Exception as e:
            pytest.skip(
                f"Cannot connect to test database or TimescaleDB not available: {e}"
            )

    def test_hypertable_creation(self, db_engine):
        """Test that hypertables are created correctly."""

        @timescale_crud(
            table_name="hypertable_test",
            time_column="timestamp",
            create_hypertable=True,
            chunk_time_interval="1 hour",
        )
        class HypertableTest(BaseModel):
            id: Optional[int] = None
            value: float
            timestamp: datetime

        # Initialize database
        HypertableTest.init_db(db_engine)

        # Check if hypertable was created
        with db_engine.connect() as conn:
            result = conn.execute("""
                SELECT table_name 
                FROM _timescaledb_catalog.hypertable 
                WHERE table_name = 'hypertable_test'
            """)
            hypertable = result.fetchone()
            assert hypertable is not None

        # Cleanup
        try:
            with db_engine.connect() as conn:
                conn.execute("DROP TABLE IF EXISTS hypertable_test CASCADE")
                conn.commit()
        except Exception:
            pass

    def test_chunk_creation(self, db_engine):
        """Test that chunks are created for time-series data."""

        @timescale_crud(
            table_name="chunk_test",
            time_column="timestamp",
            create_hypertable=True,
            chunk_time_interval="1 hour",
        )
        class ChunkTest(BaseModel):
            id: Optional[int] = None
            value: float
            timestamp: datetime

        # Initialize database
        ChunkTest.init_db(db_engine)

        # Create session
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db_engine)
        session = SessionLocal()
        ChunkTest.set_session(session)

        try:
            # Insert data across multiple time periods
            base_time = datetime.now() - timedelta(hours=3)
            for i in range(5):
                test_data = {
                    "value": float(i),
                    "timestamp": base_time + timedelta(hours=i),
                }
                ChunkTest.create(test_data)

            # Check that chunks were created
            with db_engine.connect() as conn:
                result = conn.execute("""
                    SELECT COUNT(*) 
                    FROM _timescaledb_catalog.chunk c
                    JOIN _timescaledb_catalog.hypertable h ON c.hypertable_id = h.id
                    WHERE h.table_name = 'chunk_test'
                """)
                chunk_count = result.scalar()
                assert chunk_count > 0

        finally:
            session.close()
            # Cleanup
            try:
                with db_engine.connect() as conn:
                    conn.execute("DROP TABLE IF EXISTS chunk_test CASCADE")
                    conn.commit()
            except Exception:
                pass
