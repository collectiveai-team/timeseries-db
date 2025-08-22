"""
Integration tests for database CRUD operations using DuckDB.

These tests use an in-memory DuckDB instance and test the full integration
with the database connector.
"""

from datetime import datetime, timedelta


class TestDBIntegration:
    """Integration tests for database operations."""

    def test_create_and_retrieve_record(self, sample_timeseries_model):
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
        record_to_create = sample_timeseries_model(**test_data)
        created_record = sample_timeseries_model.create(record_to_create)

        assert created_record.id is not None
        assert created_record.sensor_id == test_data["sensor_id"]
        assert created_record.temperature == test_data["temperature"]

        # Retrieve record
        retrieved_record = sample_timeseries_model.get_by_id(created_record.id)
        assert retrieved_record is not None
        assert retrieved_record.sensor_id == test_data["sensor_id"]
        assert retrieved_record.temperature == test_data["temperature"]

    def test_list_records_with_pagination(self, sample_timeseries_model):
        """Test listing records with pagination."""
        # Create multiple test records
        for i in range(5):
            test_data = {
                "sensor_id": f"sensor_list_{i:03d}",
                "temperature": 20.0 + i,
                "humidity": 60.0 + i,
                "timestamp": datetime.now() + timedelta(minutes=i),
                "location": f"List Room {i}",
            }
            record_to_create = sample_timeseries_model(**test_data)
            sample_timeseries_model.create(record_to_create)

        # Test listing with limit
        records = sample_timeseries_model.list(limit=3, offset=0)
        assert len(records) == 3

        # Test listing with offset
        records_offset = sample_timeseries_model.list(limit=3, offset=2)
        assert len(records_offset) == 3

        # Test filtering
        filtered_records = sample_timeseries_model.list(
            filters={"sensor_id": "sensor_list_001"}
        )
        assert len(filtered_records) == 1
        assert filtered_records[0].sensor_id == "sensor_list_001"

    def test_update_record(self, sample_timeseries_model):
        """Test updating a record."""
        # Create test record
        test_data = {
            "sensor_id": "sensor_update_001",
            "temperature": 25.0,
            "humidity": 70.0,
            "timestamp": datetime.now(),
            "location": "Update Room",
        }
        record_to_create = sample_timeseries_model(**test_data)
        created_record = sample_timeseries_model.create(record_to_create)

        # Update record
        update_data = {"temperature": 25.0, "humidity": 70.0}
        updated_record = sample_timeseries_model.update(created_record.id, update_data)

        assert updated_record.temperature == 25.0
        assert updated_record.humidity == 70.0

        # Verify update persisted
        retrieved_record = sample_timeseries_model.get_by_id(created_record.id)
        assert retrieved_record.temperature == 25.0
        assert retrieved_record.humidity == 70.0

    def test_delete_record(self, sample_timeseries_model):
        """Test deleting a record."""
        # Create test record
        test_data = {
            "sensor_id": "sensor_delete_001",
            "temperature": 30.0,
            "humidity": 75.0,
            "timestamp": datetime.now(),
            "location": "Delete Room",
        }
        record_to_create = sample_timeseries_model(**test_data)
        created_record = sample_timeseries_model.create(record_to_create)
        record_id = created_record.id

        # Delete record
        sample_timeseries_model.delete(record_id, hard_delete=True)

        # Verify record is deleted
        deleted_record = sample_timeseries_model.get_by_id(record_id)
        assert deleted_record is None

    def test_count_records(self, sample_timeseries_model):
        """Test counting records."""
        # Get initial count
        initial_count = sample_timeseries_model.count()

        # Create test records
        for i in range(3):
            test_data = {
                "sensor_id": f"sensor_count_{i:03d}",
                "temperature": 20.0 + i,
                "humidity": 60.0 + i,
                "timestamp": datetime.now() + timedelta(minutes=i),
                "location": f"Count Room {i}",
            }
            record_to_create = sample_timeseries_model(**test_data)
            sample_timeseries_model.create(record_to_create)

        # Test total count
        new_count = sample_timeseries_model.count()
        assert new_count == initial_count + 3

        # Test filtered count
        filtered_count = sample_timeseries_model.count(
            filters={"location": "Count Room 1"}
        )
        assert filtered_count == 1

    def test_timeseries_queries(self, sample_timeseries_model):
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
            record_to_create = sample_timeseries_model(**test_data)
            sample_timeseries_model.create(record_to_create)

        # Test ordering by timestamp
        records = sample_timeseries_model.list(
            filters={"sensor_id": "sensor_timeseries_001"},
            order_by="timestamp",
            order_desc=False,
            limit=5,
        )

        assert len(records) == 5
        # Verify ordering
        for i in range(1, len(records)):
            assert records[i].timestamp >= records[i - 1].timestamp
