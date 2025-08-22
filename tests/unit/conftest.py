"""
Configuration for unit tests.
"""

import pytest
from unittest.mock import patch, Mock


@pytest.fixture(autouse=True)
def mock_db_connector():
    """Mock the database connector to prevent real DB connections."""

    class MockConnector:
        def __init__(self, model, config):
            self.model = model
            self.config = config
            # The decorator works with the connector *instance*
            # so we mock the instance and its config
            self.instance = Mock()
            self.instance.config = config

        def connect(self):
            pass

        def disconnect(self):
            pass

        def create_table(self):
            pass

        def _get_connection(self):
            return Mock()

        def _get_table_name(self):
            return self.config.get("table_name", "mock_table")

    # A mock for the connector class that returns an instance of our MockConnector
    MockConnectorClass = Mock(
        side_effect=lambda model, config: MockConnector(model, config)
    )

    # Patch the CONNECTOR_MAP to return the mock class
    with patch(
        "tsdb.decorators.pydantic_decorator.CONNECTOR_MAP",
        {
            "timescaledb": MockConnectorClass,
            "duckdb": MockConnectorClass,
            "timestream": MockConnectorClass,
        },
    ) as mock_map:
        yield mock_map
