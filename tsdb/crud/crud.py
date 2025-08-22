"""
TimescaleDB CRUD Decorator for Pydantic Models

This module provides a decorator that adds CRUD operations to Pydantic models
for use with SQLAlchemy and TimescaleDB.
"""

import logging
from typing import Any, Generic, TypeVar, ClassVar
import builtins as blt

from pydantic import Field, BaseModel
from tsdb.crud.exceptions import CRUDError
from tsdb.connectors.base import BaseConnector

logger = logging.getLogger(__name__)

# Type variables
T = TypeVar("T", bound=BaseModel)


class CRUDConfig(BaseModel):
    """Configuration for CRUD operations"""

    db_type: str
    table_name: str
    primary_key: str = "id"
    time_column: str = "created_at"
    enable_soft_delete: bool = False
    soft_delete_column: str = "deleted_at"
    enable_audit: bool = True
    audit_columns: dict[str, str] = Field(
        default_factory=lambda: {"created_at": "created_at", "updated_at": "updated_at"}
    )
    # Connector-specific config
    connector_config: dict[str, Any] = Field(default_factory=dict)


class CRUDMixin(Generic[T]):
    """Mixin class that provides a connector-based CRUD interface."""

    _connector: ClassVar[BaseConnector[T] | None] = None

    @classmethod
    def set_connector(cls, connector: BaseConnector[T]) -> None:
        """Set the database connector."""
        cls._connector = connector

    @classmethod
    def _get_connector(cls) -> BaseConnector[T]:
        """Get the current connector or raise an error."""
        if cls._connector is None:
            raise CRUDError(
                "Database connector not configured. Call set_connector() first."
            )
        return cls._connector

    @classmethod
    def create(cls, data: T) -> T:
        """Create a new record."""
        connector = cls._get_connector()
        return connector.create(data)

    @classmethod
    def get_by_id(cls, record_id: Any) -> T | None:
        """Get a record by ID."""
        connector = cls._get_connector()
        return connector.get_by_id(record_id)

    @classmethod
    def list(
        cls,
        limit: int | None = 100,
        offset: int = 0,
        filters: dict[str, Any] | None = None,
        order_by: str | None = None,
        order_desc: bool = False,
    ) -> blt.list[T]:
        """List records via the configured connector."""
        connector = cls._get_connector()
        return connector.list(
            limit=limit,
            offset=offset,
            filters=filters,
            order_by=order_by,
            order_desc=order_desc,
        )

    @classmethod
    def list_all(
        cls,
        limit: int | None = None,
        offset: int = 0,
        filters: dict[str, Any] | None = None,
        order_by: str | None = None,
        order_desc: bool = False,
    ) -> blt.list[T]:
        """List all records by delegating to list() without a limit."""
        return cls.list(
            limit=None,
            offset=offset,
            filters=filters,
            order_by=order_by,
            order_desc=order_desc,
        )

    @classmethod
    def update(cls, record_id: Any, data: dict[str, Any]) -> T | None:
        """Update a record by ID."""
        connector = cls._get_connector()
        return connector.update(record_id, data)

    @classmethod
    def delete(cls, record_id: Any, hard_delete: bool = False) -> bool:
        """Delete a record by ID."""
        connector = cls._get_connector()
        return connector.delete(record_id, hard_delete)

    @classmethod
    def count(cls, filters: dict[str, Any] | None = None) -> int:
        """Count records."""
        connector = cls._get_connector()
        return connector.count(filters=filters)

    @classmethod
    def bulk_insert(cls, data_list: blt.list[T]) -> blt.list[T]:
        """Bulk insert records."""
        connector = cls._get_connector()
        return connector.bulk_insert(data_list)

    @classmethod
    def get_last_k_items(
        cls, k: int, filters: dict[str, Any] | None = None
    ) -> blt.list[T]:
        """Get the last k items."""
        connector = cls._get_connector()
        return connector.get_last_k_items(k, filters)
