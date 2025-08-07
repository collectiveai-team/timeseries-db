"""
Mock API responses for local testing of the TimescaleDB CRUD system.
"""

from datetime import datetime
from typing import Dict, List, Any
from unittest.mock import Mock


class MockDatabaseResponses:
    """Mock database responses for testing."""
    
    @staticmethod
    def successful_create_response() -> Dict[str, Any]:
        """Mock successful create operation response."""
        return {
            "id": 1,
            "sensor_id": "sensor_001",
            "temperature": 23.5,
            "humidity": 65.2,
            "timestamp": datetime(2024, 1, 1, 12, 0, 0),
            "location": "Room A",
            "created_at": datetime(2024, 1, 1, 12, 0, 0),
            "updated_at": datetime(2024, 1, 1, 12, 0, 0)
        }
    
    @staticmethod
    def successful_list_response() -> List[Dict[str, Any]]:
        """Mock successful list operation response."""
        return [
            {
                "id": 1,
                "sensor_id": "sensor_001",
                "temperature": 23.5,
                "humidity": 65.2,
                "timestamp": datetime(2024, 1, 1, 12, 0, 0),
                "location": "Room A",
                "created_at": datetime(2024, 1, 1, 12, 0, 0),
                "updated_at": datetime(2024, 1, 1, 12, 0, 0)
            },
            {
                "id": 2,
                "sensor_id": "sensor_002",
                "temperature": 24.1,
                "humidity": 68.5,
                "timestamp": datetime(2024, 1, 1, 13, 0, 0),
                "location": "Room B",
                "created_at": datetime(2024, 1, 1, 13, 0, 0),
                "updated_at": datetime(2024, 1, 1, 13, 0, 0)
            }
        ]
    
    @staticmethod
    def successful_update_response() -> Dict[str, Any]:
        """Mock successful update operation response."""
        return {
            "id": 1,
            "sensor_id": "sensor_001",
            "temperature": 25.0,  # Updated value
            "humidity": 65.2,
            "timestamp": datetime(2024, 1, 1, 12, 0, 0),
            "location": "Room A",
            "created_at": datetime(2024, 1, 1, 12, 0, 0),
            "updated_at": datetime(2024, 1, 1, 14, 0, 0)  # Updated timestamp
        }
    
    @staticmethod
    def error_response() -> Dict[str, Any]:
        """Mock error response."""
        return {
            "error": "Database connection failed",
            "code": "DB_CONNECTION_ERROR",
            "details": "Unable to connect to TimescaleDB instance"
        }


class MockSQLAlchemySession:
    """Mock SQLAlchemy session for testing."""
    
    def __init__(self):
        self.committed = False
        self.rolled_back = False
        self.added_objects = []
        self.executed_queries = []
    
    def add(self, obj):
        """Mock add method."""
        self.added_objects.append(obj)
    
    def commit(self):
        """Mock commit method."""
        self.committed = True
    
    def rollback(self):
        """Mock rollback method."""
        self.rolled_back = True
    
    def refresh(self, obj):
        """Mock refresh method."""
        # Simulate setting an ID after commit
        if not hasattr(obj, 'id') or obj.id is None:
            obj.id = 1
    
    def execute(self, query):
        """Mock execute method."""
        self.executed_queries.append(query)
        return MockResult()
    
    def get(self, model, primary_key):
        """Mock get method."""
        if primary_key == 1:
            return self._create_mock_object()
        return None
    
    def scalars(self, query):
        """Mock scalars method."""
        return MockScalars()
    
    def close(self):
        """Mock close method."""
        pass
    
    def _create_mock_object(self):
        """Create a mock database object."""
        obj = Mock()
        obj.id = 1
        obj.sensor_id = "sensor_001"
        obj.temperature = 23.5
        obj.humidity = 65.2
        obj.timestamp = datetime(2024, 1, 1, 12, 0, 0)
        obj.location = "Room A"
        obj.created_at = datetime(2024, 1, 1, 12, 0, 0)
        obj.updated_at = datetime(2024, 1, 1, 12, 0, 0)
        return obj


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
    
    def __init__(self, data=None):
        self.data = data or []
    
    def all(self):
        return self.data
    
    def first(self):
        return self.data[0] if self.data else None


class MockTimescaleDBEngine:
    """Mock TimescaleDB engine for testing."""
    
    def __init__(self):
        self.connected = False
        self.tables_created = False
    
    def connect(self):
        """Mock connect method."""
        self.connected = True
        return self
    
    def execute(self, query):
        """Mock execute method."""
        if "CREATE EXTENSION" in str(query):
            return MockResult()
        if "SELECT create_hypertable" in str(query):
            return MockResult()
        return MockResult()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connected = False


def create_mock_session_factory():
    """Create a mock session factory."""
    def mock_session():
        return MockSQLAlchemySession()
    return mock_session


def create_mock_engine():
    """Create a mock SQLAlchemy engine."""
    engine = Mock()
    engine.connect.return_value = MockTimescaleDBEngine()
    engine.execute.return_value = MockResult()
    return engine
