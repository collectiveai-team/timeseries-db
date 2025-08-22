"""
Custom exceptions for connectors.
"""


class ConnectorError(Exception):
    """Base exception for connector-related errors."""


class ConnectionError(ConnectorError):
    """Raised when a connection to the database fails."""


class ConfigurationError(ConnectorError):
    """Raised when there is a configuration error."""
