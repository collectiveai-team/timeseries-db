import logging
from typing import Any, Type, TypeVar, ClassVar
import os

from pydantic import BaseModel

from tsdb.crud.crud import CRUDMixin, CRUDProtocol
from tsdb.connectors.base import BaseConnector
from tsdb.connectors.timescaledb import TimescaleDBConnector
from tsdb.connectors.duckdb import DuckDBConnector
from tsdb.connectors.timestream import AWSTimestreamConnector

# Import other connectors here as they are created
# from tsdb.connectors.timestream import AWSTimestreamConnector

logger = logging.getLogger(__name__)

# Type variables
T = TypeVar("T", bound=BaseModel)


# Mapping of db_type to connector class
CONNECTOR_MAP: dict[str, Type[BaseConnector]] = {
    "timescaledb": TimescaleDBConnector,
    "duckdb": DuckDBConnector,
    "timestream": AWSTimestreamConnector,
}


def db_crud(
    db_type: str,
    table_name: str,
    *,
    primary_key: str = "id",
    time_column: str = "created_at",
    enable_soft_delete: bool = False,
    soft_delete_column: str = "deleted_at",
    enable_audit: bool = True,
    audit_columns: dict[str, str] | None = None,
    db_uri: str | None = None,
    # Pass connector-specific config via a flat structure for simplicity
    **kwargs: Any,
):
    """
    Decorator that adds CRUD operations to a Pydantic model for a specified database.

    Args:
        db_type: The type of the database (e.g., 'timescaledb', 'duckdb').
        table_name: The name of the database table.
        primary_key: The name of the primary key column.
        time_column: The name of the time series column.
        enable_soft_delete: If True, enable soft delete functionality.
        soft_delete_column: The name of the soft delete column.
        enable_audit: If True, enable audit columns (created_at, updated_at).
        audit_columns: Dictionary to specify custom names for audit columns.
        **kwargs: Additional database-specific configuration arguments.

    Returns:
        A decorator that enhances a Pydantic model with CRUD capabilities.
    """

    def decorator(cls: Type[T]) -> Type[CRUDProtocol[T]]:
        """The actual decorator function"""

        db_connection_uri = db_uri or os.getenv("TSDB_DATABASE_URI")
        # 1. Combine all configurations into a single dictionary
        config_dict = {
            "db_type": db_type,
            "table_name": table_name,
            "primary_key": primary_key,
            "time_column": time_column,
            "enable_soft_delete": enable_soft_delete,
            "soft_delete_column": soft_delete_column,
            "enable_audit": enable_audit,
            "audit_columns": audit_columns
            if audit_columns
            else {"created_at": "created_at", "updated_at": "updated_at"},
            "db_uri": db_connection_uri,
            **kwargs,
        }

        # 2. Get and initialize the connector
        connector_class = CONNECTOR_MAP.get(db_type)
        if not connector_class:
            raise ValueError(f"Unsupported database type: {db_type}")

        connector = connector_class(model=cls, config=config_dict)
        connector.connect()
        connector.create_table()

        # 3. Create Enhanced Model with CRUD Mixin
        class EnhancedModel(cls, CRUDMixin):
            _connector: ClassVar[BaseConnector] = connector

            def save_instance(self, **kwargs) -> T:
                """Save the current instance to the database (create or update)."""
                pk_value = self._get_primary_key_value()
                if pk_value is None:
                    created_obj = self._connector.create(self)
                    # Update instance with created data (e.g., new ID)
                    if created_obj:
                        for field in created_obj.model_fields:
                            setattr(self, field, getattr(created_obj, field))
                else:
                    updated_obj = self._connector.update(pk_value, self.model_dump())
                    if updated_obj:
                        for field in updated_obj.model_fields:
                            setattr(self, field, getattr(updated_obj, field))
                return self

            def delete_instance(self, hard_delete: bool = False) -> None:
                """Delete the current instance from the database."""
                pk_value = self._get_primary_key_value()
                if pk_value is not None:
                    self._connector.delete(pk_value, hard_delete=hard_delete)

            def refresh_instance(self) -> T:
                """Refresh the instance with the latest data from the database."""
                pk_value = self._get_primary_key_value()
                if pk_value is not None:
                    updated_data = self._connector.get_by_id(pk_value)
                    if updated_data:
                        for field in updated_data.model_fields:
                            setattr(self, field, getattr(updated_data, field))
                return self

            def _get_primary_key_value(self) -> Any:
                """Get the value of the primary key for this instance."""
                return getattr(self, primary_key, None)

        EnhancedModel.__name__ = cls.__name__
        EnhancedModel.__qualname__ = cls.__qualname__
        EnhancedModel.__module__ = cls.__module__
        # Set the connector on the class for class-level methods
        EnhancedModel.set_connector(connector)
        return EnhancedModel

    return decorator


__all__ = ["db_crud"]
