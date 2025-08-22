"""
TimescaleDB Storage Decorator for Darts TimeSeries

This module provides a decorator that adds TimescaleDB storage capabilities
to classes that work with Darts TimeSeries objects.
"""

import logging
import pickle
from datetime import datetime
from typing import Any, TypeVar, type

try:
    from darts import TimeSeries

    DARTS_AVAILABLE = True
except ImportError:
    TimeSeries = None
    DARTS_AVAILABLE = False

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    String,
    Text,
    Boolean,
    Float,
    LargeBinary,
    select,
    update,
    delete,
)
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

# Type variables
T = TypeVar("T")
SQLModelType = TypeVar("SQLModelType")

Base = declarative_base()


def timeseries_storage(
    table_name: str,
    primary_key: str = "id",
    time_column: str = "created_at",
    name_column: str = "series_name",
    metadata_columns: dict[str, type] | None = None,
    enable_soft_delete: bool = False,
    soft_delete_column: str = "deleted_at",
    enable_audit: bool = True,
    audit_columns: dict[str, str] | None = None,
    create_hypertable: bool = True,
    chunk_time_interval: str = "1 day",
):
    """
    Decorator that adds TimescaleDB storage capabilities for Darts TimeSeries objects.

    Args:
        table_name: Name of the database table
        primary_key: Name of the primary key column
        time_column: Name of the time column for hypertable partitioning
        name_column: Column name for storing series name/identifier
        metadata_columns: Additional metadata columns to store with the series
        enable_soft_delete: Enable soft delete functionality
        soft_delete_column: Column name for soft delete timestamp
        enable_audit: Enable audit fields (created_at, updated_at)
        audit_columns: Custom mapping for audit column names
        create_hypertable: Whether to create a TimescaleDB hypertable
        chunk_time_interval: Time interval for hypertable chunks

    Returns:
        Decorated class with TimeSeries storage operations
    """

    def decorator(target_class: type[T]) -> type[T]:
        # Set default metadata columns if not provided
        meta_cols = metadata_columns or {}

        # Set default audit columns
        audit_cols = audit_columns or {
            "created_at": "created_at",
            "updated_at": "updated_at",
        }

        # Create SQLAlchemy model for TimeSeries storage
        sql_model_attrs = {
            "__tablename__": table_name,
        }

        # Add primary key
        sql_model_attrs[primary_key] = Column(
            Integer, primary_key=True, autoincrement=True
        )

        # Add series name/identifier column
        sql_model_attrs[name_column] = Column(String(255), nullable=False)

        # Add serialized TimeSeries data column
        sql_model_attrs["series_data"] = Column(LargeBinary, nullable=False)

        # Add metadata about the series
        sql_model_attrs["start_time"] = Column(DateTime, nullable=False)
        sql_model_attrs["end_time"] = Column(DateTime, nullable=False)
        sql_model_attrs["frequency"] = Column(String(50), nullable=True)
        sql_model_attrs["components"] = Column(
            Text, nullable=True
        )  # JSON string of component names
        sql_model_attrs["n_timesteps"] = Column(Integer, nullable=False)
        sql_model_attrs["n_components"] = Column(Integer, nullable=False)
        sql_model_attrs["n_samples"] = Column(Integer, nullable=False)

        # Add custom metadata columns
        for col_name, col_type in meta_cols.items():
            if col_type is int:
                sql_model_attrs[col_name] = Column(Integer, nullable=True)
            elif col_type is str:
                sql_model_attrs[col_name] = Column(String(255), nullable=True)
            elif col_type is float:
                sql_model_attrs[col_name] = Column(Float, nullable=True)
            elif col_type is bool:
                sql_model_attrs[col_name] = Column(Boolean, nullable=True)
            elif col_type is datetime:
                sql_model_attrs[col_name] = Column(DateTime, nullable=True)
            else:
                sql_model_attrs[col_name] = Column(Text, nullable=True)

        # Add audit columns if enabled
        if enable_audit:
            for logical_name, column_name in audit_cols.items():
                if logical_name in ["created_at", "updated_at"]:
                    sql_model_attrs[column_name] = Column(
                        DateTime, default=datetime.utcnow
                    )

        # Add soft delete column if enabled
        if enable_soft_delete:
            sql_model_attrs[soft_delete_column] = Column(DateTime, nullable=True)

        # Add TimescaleDB hypertable configuration if enabled
        if create_hypertable:
            sql_model_attrs["__table_args__"] = {
                "timescaledb_hypertable": {
                    "time_column_name": time_column,
                    "chunk_time_interval": chunk_time_interval,
                }
            }

        # Create SQLAlchemy model class with unique name based on table name
        table_class_name = "".join(word.capitalize() for word in table_name.split("_"))
        sql_model_class_name = f"{table_class_name}SQL"
        sql_model = type(sql_model_class_name, (Base,), sql_model_attrs)

        # Create new class that inherits from target class with TimeSeries storage capabilities
        class EnhancedModel(target_class):
            _sql_model = sql_model
            _session: Session | None = None

            @classmethod
            def set_session(cls, session: Session) -> None:
                """Set the database session for operations"""
                cls._session = session

            @classmethod
            def get_session(cls) -> Session:
                """Get the current database session"""
                if cls._session is None:
                    raise ValueError(
                        "No database session set. Call set_session() first."
                    )
                return cls._session

            @classmethod
            def save_timeseries(
                cls,
                series: TimeSeries,
                name: str,
                metadata: dict[str, Any] | None = None,
            ) -> int:
                """Save a TimeSeries to the database"""
                if not DARTS_AVAILABLE:
                    raise ImportError(
                        "darts package is required for TimeSeries operations. Install with: uv add tsdb[forecast]"
                    )

                try:
                    session = cls.get_session()

                    # Serialize the TimeSeries
                    series_data = pickle.dumps(series)

                    # Extract metadata from TimeSeries
                    start_time = series.start_time()
                    end_time = series.end_time()
                    frequency = str(series.freq) if series.freq else None
                    components = (
                        ",".join(series.components)
                        if hasattr(series, "components")
                        else None
                    )
                    n_timesteps = len(series)
                    n_components = series.n_components
                    n_samples = series.n_samples

                    # Create record data
                    record_data = {
                        name_column: name,
                        "series_data": series_data,
                        "start_time": start_time,
                        "end_time": end_time,
                        "frequency": frequency,
                        "components": components,
                        "n_timesteps": n_timesteps,
                        "n_components": n_components,
                        "n_samples": n_samples,
                    }

                    # Add custom metadata
                    if metadata:
                        for key, value in metadata.items():
                            if key in meta_cols:
                                record_data[key] = value

                    # Add audit fields
                    if enable_audit:
                        now = datetime.utcnow()
                        record_data[audit_cols["created_at"]] = now
                        record_data[audit_cols["updated_at"]] = now

                    # Insert record
                    new_record = cls._sql_model(**record_data)
                    session.add(new_record)
                    session.commit()

                    logger.info(
                        f"Saved TimeSeries '{name}' with ID {getattr(new_record, primary_key)}"
                    )
                    return getattr(new_record, primary_key)

                except SQLAlchemyError as e:
                    session.rollback()
                    logger.error(f"Error saving TimeSeries '{name}': {e}")
                    raise

            @classmethod
            def load_timeseries(cls, name: str) -> TimeSeries | None:
                """Load a TimeSeries from the database by name"""
                if not DARTS_AVAILABLE:
                    raise ImportError(
                        "darts package is required for TimeSeries operations. Install with: uv add tsdb[forecast]"
                    )

                try:
                    session = cls.get_session()

                    # Query for the series
                    stmt = select(cls._sql_model).where(
                        getattr(cls._sql_model, name_column) == name
                    )

                    if enable_soft_delete:
                        stmt = stmt.where(
                            getattr(cls._sql_model, soft_delete_column).is_(None)
                        )

                    result = session.execute(stmt).first()

                    if result is None:
                        logger.warning(f"TimeSeries '{name}' not found")
                        return None

                    # Deserialize the TimeSeries
                    series_data = result[0].series_data
                    series = pickle.loads(series_data)

                    logger.info(f"Loaded TimeSeries '{name}'")
                    return series

                except SQLAlchemyError as e:
                    logger.error(f"Error loading TimeSeries '{name}': {e}")
                    raise

            @classmethod
            def list_timeseries(cls, limit: int | None = None) -> list[dict[str, Any]]:
                """List all TimeSeries metadata in the database"""
                try:
                    session = cls.get_session()

                    stmt = select(cls._sql_model)

                    if enable_soft_delete:
                        stmt = stmt.where(
                            getattr(cls._sql_model, soft_delete_column).is_(None)
                        )

                    if limit:
                        stmt = stmt.limit(limit)

                    results = session.execute(stmt).fetchall()

                    series_list = []
                    for result in results:
                        record = result[0]
                        series_info = {
                            primary_key: getattr(record, primary_key),
                            "name": getattr(record, name_column),
                            "start_time": record.start_time,
                            "end_time": record.end_time,
                            "frequency": record.frequency,
                            "components": record.components,
                            "n_timesteps": record.n_timesteps,
                            "n_components": record.n_components,
                            "n_samples": record.n_samples,
                        }

                        # Add custom metadata
                        for col_name in meta_cols.keys():
                            if hasattr(record, col_name):
                                series_info[col_name] = getattr(record, col_name)

                        # Add audit fields
                        if enable_audit:
                            for logical_name, column_name in audit_cols.items():
                                if hasattr(record, column_name):
                                    series_info[logical_name] = getattr(
                                        record, column_name
                                    )

                        series_list.append(series_info)

                    logger.info(f"Listed {len(series_list)} TimeSeries records")
                    return series_list

                except SQLAlchemyError as e:
                    logger.error(f"Error listing TimeSeries: {e}")
                    raise

            @classmethod
            def delete_timeseries(cls, name: str, soft: bool | None = None) -> bool:
                """Delete a TimeSeries from the database"""
                try:
                    session = cls.get_session()

                    # Use soft delete if enabled and not explicitly overridden
                    use_soft_delete = enable_soft_delete if soft is None else soft

                    if use_soft_delete and enable_soft_delete:
                        # Soft delete
                        stmt = (
                            update(cls._sql_model)
                            .where(getattr(cls._sql_model, name_column) == name)
                            .where(
                                getattr(cls._sql_model, soft_delete_column).is_(None)
                            )
                            .values({soft_delete_column: datetime.utcnow()})
                        )
                    else:
                        # Hard delete
                        stmt = delete(cls._sql_model).where(
                            getattr(cls._sql_model, name_column) == name
                        )

                    result = session.execute(stmt)
                    session.commit()

                    if result.rowcount > 0:
                        delete_type = (
                            "soft" if use_soft_delete and enable_soft_delete else "hard"
                        )
                        logger.info(
                            f"Performed {delete_type} delete of TimeSeries '{name}'"
                        )
                        return True
                    else:
                        logger.warning(f"TimeSeries '{name}' not found for deletion")
                        return False

                except SQLAlchemyError as e:
                    session.rollback()
                    logger.error(f"Error deleting TimeSeries '{name}': {e}")
                    raise

        # Add method to initialize database
        @classmethod
        def init_db(cls, engine, create_tables: bool = True):
            """Initialize database tables and hypertables"""
            if create_tables:
                Base.metadata.create_all(engine)
                logger.info("Created tables for TimeSeries storage")

        EnhancedModel.init_db = init_db

        # Preserve original class name and module
        EnhancedModel.__name__ = target_class.__name__
        EnhancedModel.__qualname__ = target_class.__qualname__
        EnhancedModel.__module__ = target_class.__module__

        return EnhancedModel

    return decorator
