from abc import ABC, abstractmethod
from typing import Any, Type, TypeVar, Generic

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class BaseConnector(ABC, Generic[T]):
    """Abstract base class for database connectors."""

    @abstractmethod
    def __init__(self, model: Type[T], config: dict[str, Any]):
        """Initialize the connector with a configuration object."""
        pass

    @abstractmethod
    def connect(self):
        """Establish a connection to the database."""
        pass

    @abstractmethod
    def disconnect(self):
        """Disconnect from the database."""
        pass

    @abstractmethod
    def create_table(self):
        """Create a table for the given model."""
        pass

    @abstractmethod
    def create(self, instance: T) -> T:
        """Create a new record."""
        pass

    @abstractmethod
    def get_by_id(self, item_id: Any) -> T | None:
        """Get a record by its primary key."""
        pass

    @abstractmethod
    def list(
        self,
        *,
        limit: int | None = 100,
        offset: int = 0,
        filters: dict[str, Any] | None = None,
        order_by: str | None = None,
        order_desc: bool = False,
        **kwargs: Any,
    ) -> list[T]:
        """List records with optional filtering, ordering and pagination.

        Implementations should honor:
        - limit: maximum number of rows to return (None means no limit)
        - offset: number of rows to skip (if supported)
        - filters: equality filters per field name
        - order_by/order_desc: ordering
        Additional connector-specific options can be passed via kwargs.
        """
        pass

    def list_all(
        self,
        *,
        filters: dict[str, Any] | None = None,
        order_by: str | None = None,
        order_desc: bool = False,
        **kwargs: Any,
    ) -> list[T]:
        """List all records by delegating to list() without a limit."""
        return self.list(
            limit=None,
            offset=0,
            filters=filters,
            order_by=order_by,
            order_desc=order_desc,
            **kwargs,
        )

    @abstractmethod
    def update(self, item_id: Any, data: dict[str, Any]) -> T | None:
        """Update a record by its primary key."""
        pass

    @abstractmethod
    def delete(self, item_id: Any, hard_delete: bool = False) -> None:
        """Delete a record by its primary key."""
        pass

    @abstractmethod
    def count(self, **kwargs) -> int:
        """Count records with optional filtering."""
        pass

    @abstractmethod
    def bulk_insert(self, instances: list[T]) -> list[T]:
        """Create multiple records efficiently."""
        pass

    @abstractmethod
    def get_last_k_items(self, k: int, time_column: str | None = None) -> list[T]:
        """Get the last k items, ordered by the time column."""
        pass
