"""
Unit tests for the Pydantic TimescaleDB CRUD decorator.
"""

import pytest
import uuid
from datetime import datetime
from unittest.mock import Mock
from pydantic import BaseModel
from typing import Optional

from tsdb.decorators.pydantic_decorator import timescale_crud
from tsdb.crud import CRUDConfig, CRUDError


class TestCRUDConfig:
    """Test CRUDConfig class."""

    def test_crud_config_creation(self):
        """Test creating a CRUD configuration."""
        config = CRUDConfig(
            table_name="test_table", primary_key="id", time_column="timestamp"
        )

        assert config.table_name == "test_table"
        assert config.primary_key == "id"
        assert config.time_column == "timestamp"
        assert config.enable_soft_delete is False
        assert config.enable_audit is True


class TestTimescaleCRUDDecorator:
    """Test the timescale_crud decorator."""

    def test_decorator_creates_enhanced_model(self):
        """Test that the decorator creates an enhanced model with CRUD operations."""

        @timescale_crud(table_name="sensor_data", time_column="timestamp")
        class SensorData(BaseModel):
            id: Optional[int] = None
            sensor_id: str
            temperature: float
            timestamp: datetime

        # Check that CRUD methods are added
        assert hasattr(SensorData, "create")
        assert hasattr(SensorData, "get_by_id")
        assert hasattr(SensorData, "update")
        assert hasattr(SensorData, "delete")
        assert hasattr(SensorData, "list")
        assert hasattr(SensorData, "count")


@pytest.fixture
def sample_data():
    """Sample data for testing."""
    return {
        "sensor_id": "sensor_001",
        "temperature": 23.5,
        "humidity": 65.2,
        "timestamp": datetime.now(),
        "location": "Room A",
    }


class TestDecoratorFunctionality:
    """Test the decorator's core functionality without complex SQLAlchemy mocking."""

    @pytest.fixture
    def sensor_model(self):
        """Create a sensor model for testing."""
        # Use a unique class name for each test to avoid SQLAlchemy registry conflicts
        unique_id = str(uuid.uuid4()).replace("-", "")[:8]
        table_name = f"sensor_data_{unique_id}"

        @timescale_crud(table_name=table_name, time_column="timestamp")
        class SensorData(BaseModel):
            id: Optional[int] = None
            sensor_id: str
            temperature: float
            humidity: float
            timestamp: datetime
            location: Optional[str] = None

        return SensorData

    def test_decorator_adds_crud_methods(self, sensor_model):
        """Test that the decorator adds CRUD methods to the model."""
        # Verify that all CRUD methods are present
        assert hasattr(sensor_model, "create")
        assert hasattr(sensor_model, "get_by_id")
        assert hasattr(sensor_model, "update")
        assert hasattr(sensor_model, "delete")
        assert hasattr(sensor_model, "list")
        assert hasattr(sensor_model, "count")

        # Verify methods are callable
        assert callable(sensor_model.create)
        assert callable(sensor_model.get_by_id)
        assert callable(sensor_model.update)
        assert callable(sensor_model.delete)
        assert callable(sensor_model.list)
        assert callable(sensor_model.count)

    def test_decorator_adds_session_management(self, sensor_model):
        """Test that the decorator adds session management methods."""
        # Verify session management methods are present
        assert hasattr(sensor_model, "set_session")
        assert hasattr(sensor_model, "_get_session")
        assert callable(sensor_model.set_session)
        assert callable(sensor_model._get_session)

        # Test setting a session
        mock_session = Mock()
        sensor_model.set_session(mock_session)

        # Verify the session was set
        retrieved_session = sensor_model._get_session()
        assert retrieved_session is mock_session

    def test_decorator_adds_configuration_methods(self, sensor_model):
        """Test that the decorator adds configuration access methods."""
        # Verify configuration methods are present
        assert hasattr(sensor_model, "_get_config")
        assert hasattr(sensor_model, "_get_sql_model")
        assert callable(sensor_model._get_config)
        assert callable(sensor_model._get_sql_model)

        # Test getting configuration
        config = sensor_model._get_config()
        assert config is not None
        assert hasattr(config, "table_name")
        assert hasattr(config, "time_column")
        assert config.time_column == "timestamp"

    def test_decorator_preserves_pydantic_functionality(
        self, sensor_model, sample_data
    ):
        """Test that the decorator preserves original Pydantic model functionality."""
        # Test creating a Pydantic instance
        instance = sensor_model(**sample_data)

        # Verify Pydantic functionality works
        assert instance.sensor_id == sample_data["sensor_id"]
        assert instance.temperature == sample_data["temperature"]
        assert instance.humidity == sample_data["humidity"]
        assert instance.timestamp == sample_data["timestamp"]
        assert instance.location == sample_data["location"]

        # Test model validation
        assert isinstance(instance, BaseModel)
        assert isinstance(instance, sensor_model)

        # Test serialization
        data_dict = instance.model_dump()
        assert "sensor_id" in data_dict
        assert "temperature" in data_dict

    def test_decorator_creates_sql_model(self, sensor_model):
        """Test that the decorator creates a corresponding SQLAlchemy model."""
        # Get the SQL model
        sql_model = sensor_model._get_sql_model()

        # Verify it's a class (SQLAlchemy model)
        assert sql_model is not None
        assert hasattr(sql_model, "__table__")
        assert hasattr(sql_model, "__tablename__")

        # Verify it has the expected columns
        table = sql_model.__table__
        column_names = [col.name for col in table.columns]

        # Should have the basic columns from the Pydantic model
        expected_columns = [
            "id",
            "sensor_id",
            "temperature",
            "humidity",
            "timestamp",
            "location",
        ]
        for col in expected_columns:
            assert col in column_names

    def test_decorator_with_different_configurations(self):
        """Test that the decorator works with different configuration options."""

        # Test with soft delete enabled
        @timescale_crud(
            table_name="test_soft", time_column="created_at", enable_soft_delete=True
        )
        class SoftDeleteModel(BaseModel):
            id: Optional[int] = None
            name: str
            created_at: datetime

        config = SoftDeleteModel._get_config()
        assert config.enable_soft_delete is True
        assert config.time_column == "created_at"

        # Test with audit disabled
        @timescale_crud(
            table_name="test_no_audit", time_column="timestamp", enable_audit=False
        )
        class NoAuditModel(BaseModel):
            id: Optional[int] = None
            value: float
            timestamp: datetime

        config = NoAuditModel._get_config()
        assert config.enable_audit is False

    def test_decorator_error_handling(self, sensor_model):
        """Test that the decorator handles errors appropriately."""
        # Test that calling CRUD methods without a session raises appropriate errors
        with pytest.raises((CRUDError, AttributeError)):
            sensor_model.create(
                {
                    "sensor_id": "test",
                    "temperature": 20.0,
                    "humidity": 50.0,
                    "timestamp": datetime.now(),
                }
            )

        with pytest.raises((CRUDError, AttributeError)):
            sensor_model.get_by_id(1)

        with pytest.raises((CRUDError, AttributeError)):
            sensor_model.list()
