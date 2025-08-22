"""
Unit tests for the Pydantic TimescaleDB CRUD decorator.
"""

import pytest
import uuid
from datetime import datetime
from pydantic import BaseModel

from tsdb.decorators.pydantic_decorator import db_crud
from tsdb.crud.crud import CRUDConfig
from tsdb.crud.exceptions import CRUDError


class TestCRUDConfig:
    """Test CRUDConfig class."""

    def test_crud_config_creation(self):
        """Test creating a CRUD configuration."""
        config = CRUDConfig(
            db_type="timescaledb",
            table_name="test_table",
            primary_key="id",
            time_column="timestamp",
        )

        assert config.db_type == "timescaledb"
        assert config.table_name == "test_table"
        assert config.primary_key == "id"
        assert config.time_column == "timestamp"
        assert config.enable_soft_delete is False
        assert config.enable_audit is True


@pytest.fixture
def decorated_sensor_model():
    """Provides a decorated sensor model for testing."""
    unique_id = str(uuid.uuid4()).replace("-", "")[:8]
    table_name = f"sensor_data_{unique_id}"

    @db_crud(db_type="timescaledb", table_name=table_name, time_column="timestamp")
    class SensorData(BaseModel):
        id: int | None = None
        sensor_id: str
        temperature: float
        humidity: float
        timestamp: datetime
        location: str | None = None

    return SensorData


class TestDecoratorFunctionality:
    """Test the decorator's core functionality."""

    def test_decorator_enhances_model(self, decorated_sensor_model):
        """Test that the decorator adds expected functionality to the model."""
        # 1. Verify that all CRUD methods are present and callable
        for method_name in ["create", "get_by_id", "update", "delete", "list", "count"]:
            assert hasattr(decorated_sensor_model, method_name)
            assert callable(getattr(decorated_sensor_model, method_name))

        # 2. Verify connector management methods are present
        assert hasattr(decorated_sensor_model, "set_connector")
        assert hasattr(decorated_sensor_model, "_get_connector")
        assert callable(decorated_sensor_model.set_connector)
        assert callable(decorated_sensor_model._get_connector)

        # 3. Verify Pydantic functionality is preserved
        sample_data = {
            "sensor_id": "sensor_001",
            "temperature": 23.5,
            "humidity": 65.2,
            "timestamp": datetime.now(),
            "location": "Room A",
        }
        instance = decorated_sensor_model(**sample_data)
        assert instance.sensor_id == sample_data["sensor_id"]
        assert isinstance(instance, BaseModel)

    def test_decorator_with_different_configurations(self):
        """Test that the decorator works with different configuration options."""

        # Test with soft delete enabled
        @db_crud(
            db_type="timescaledb",
            table_name="test_soft",
            time_column="created_at",
            enable_soft_delete=True,
        )
        class SoftDeleteModel(BaseModel):
            id: int | None = None
            name: str
            created_at: datetime

        connector = SoftDeleteModel._get_connector()
        assert connector.config["enable_soft_delete"] is True

        # Test with audit disabled
        @db_crud(
            db_type="timescaledb",
            table_name="test_no_audit",
            time_column="timestamp",
            enable_audit=False,
        )
        class NoAuditModel(BaseModel):
            id: int | None = None
            value: float
            timestamp: datetime

        connector = NoAuditModel._get_connector()
        assert connector.config["enable_audit"] is False

    def test_error_on_missing_connector(self, decorated_sensor_model):
        """Test that CRUD methods raise an error if the connector is not set."""
        # Temporarily remove the connector to test the error case
        original_connector = decorated_sensor_model._get_connector()
        decorated_sensor_model._connector = None

        with pytest.raises(CRUDError, match="Database connector not configured"):
            decorated_sensor_model.get_by_id(1)

        # Restore the connector
        decorated_sensor_model.set_connector(original_connector)
