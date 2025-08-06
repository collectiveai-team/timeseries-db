"""
TimescaleDB CRUD Decorator for Pydantic Models

This module provides a decorator that adds CRUD operations to Pydantic models
for use with SQLAlchemy and TimescaleDB.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Type, TypeVar, Union, Generic

from darts.datasets import TimeSeries
from pydantic import Field, BaseModel
from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    String,
    Text,
    Boolean,
    Float,
    create_engine,
    select,
    update,
    delete,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

# Type variables
T = TypeVar("T", bound=TimeSeries)
SQLModelType = TypeVar("SQLModelType")

Base = declarative_base()


class CRUDError(Exception):
    """Base exception for CRUD operations"""

    pass


class CRUDConfig(BaseModel):
    """Configuration for CRUD operations"""

    table_name: str
    primary_key: str = "id"
    time_column: str = "created_at"
    enable_soft_delete: bool = False
    soft_delete_column: str = "deleted_at"
    enable_audit: bool = True
    audit_columns: Dict[str, str] = Field(
        default_factory=lambda: {"created_at": "created_at", "updated_at": "updated_at"}
    )


class CRUDMixin(Generic[T]):
    """Mixin class that provides CRUD operations"""

    _session: Optional[Session] = None
    _config: Optional[CRUDConfig] = None
    _sql_model: Optional[Type] = None

    @classmethod
    def set_session(cls, session: Session) -> None:
        """Set the database session"""
        cls._session = session

    @classmethod
    def set_config(cls, config: CRUDConfig) -> None:
        """Set the CRUD configuration"""
        cls._config = config

    @classmethod
    def set_sql_model(cls, sql_model: Type) -> None:
        """Set the SQLAlchemy model"""
        cls._sql_model = sql_model

    @classmethod
    def _get_session(cls) -> Session:
        """Get the current session or raise error"""
        if cls._session is None:
            raise CRUDError(
                "Database session not configured. Call set_session() first."
            )
        return cls._session

    @classmethod
    def _get_config(cls) -> CRUDConfig:
        """Get the current config or raise error"""
        if cls._config is None:
            raise CRUDError("CRUD config not set. Call set_config() first.")
        return cls._config

    @classmethod
    def _get_sql_model(cls) -> Type:
        """Get the SQLAlchemy model or raise error"""
        if cls._sql_model is None:
            raise CRUDError("SQL model not set. Call set_sql_model() first.")
        return cls._sql_model

    @classmethod
    def create(cls, data: Union[T, Dict[str, Any]]) -> T:
        """Create a new record"""
        try:
            session = cls._get_session()
            config = cls._get_config()
            sql_model = cls._get_sql_model()

            # Convert Pydantic model to dict if needed
            if isinstance(data, BaseModel):
                data_dict = data.model_dump(exclude_unset=True)
            else:
                data_dict = data.copy()

            # Add audit fields
            if config.enable_audit:
                now = datetime.utcnow()
                if config.audit_columns.get("created_at"):
                    data_dict[config.audit_columns["created_at"]] = now
                if config.audit_columns.get("updated_at"):
                    data_dict[config.audit_columns["updated_at"]] = now

            # Create SQLAlchemy instance
            db_obj = sql_model(**data_dict)
            session.add(db_obj)
            session.commit()
            session.refresh(db_obj)

            # Convert back to Pydantic model
            return cls._sql_to_pydantic(db_obj)

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error during create: {e}")
            raise CRUDError(f"Failed to create record: {e}")
        except Exception as e:
            session.rollback()
            logger.error(f"Unexpected error during create: {e}")
            raise CRUDError(f"Unexpected error: {e}")

    @classmethod
    def get_by_id(cls, record_id: Any) -> Optional[T]:
        """Get a record by ID"""
        try:
            session = cls._get_session()
            config = cls._get_config()
            sql_model = cls._get_sql_model()

            query = select(sql_model).where(
                getattr(sql_model, config.primary_key) == record_id
            )

            # Apply soft delete filter
            if config.enable_soft_delete:
                query = query.where(
                    getattr(sql_model, config.soft_delete_column).is_(None)
                )

            result = session.execute(query).scalar_one_or_none()

            if result is None:
                return None

            return cls._sql_to_pydantic(result)

        except SQLAlchemyError as e:
            logger.error(f"Database error during get_by_id: {e}")
            raise CRUDError(f"Failed to get record: {e}")

    @classmethod
    def list(
        cls,
        limit: int = 100,
        offset: int = 0,
        filters: Optional[Dict[str, Any]] = None,
        order_by: Optional[str] = None,
        order_desc: bool = False,
    ) -> List[T]:
        """List records with optional filtering and pagination"""
        try:
            session = cls._get_session()
            config = cls._get_config()
            sql_model = cls._get_sql_model()

            query = select(sql_model)

            # Apply soft delete filter
            if config.enable_soft_delete:
                query = query.where(
                    getattr(sql_model, config.soft_delete_column).is_(None)
                )

            # Apply custom filters
            if filters:
                for field, value in filters.items():
                    if hasattr(sql_model, field):
                        if isinstance(value, dict):
                            # Handle complex filters like {"gt": 10}, {"in": [1,2,3]}
                            for op, op_value in value.items():
                                column = getattr(sql_model, field)
                                if op == "gt":
                                    query = query.where(column > op_value)
                                elif op == "gte":
                                    query = query.where(column >= op_value)
                                elif op == "lt":
                                    query = query.where(column < op_value)
                                elif op == "lte":
                                    query = query.where(column <= op_value)
                                elif op == "in":
                                    query = query.where(column.in_(op_value))
                                elif op == "like":
                                    query = query.where(column.like(f"%{op_value}%"))
                        else:
                            # Simple equality filter
                            query = query.where(getattr(sql_model, field) == value)

            # Apply ordering
            if order_by and hasattr(sql_model, order_by):
                column = getattr(sql_model, order_by)
                if order_desc:
                    query = query.order_by(column.desc())
                else:
                    query = query.order_by(column)

            # Apply pagination
            query = query.offset(offset).limit(limit)

            results = session.execute(query).scalars().all()

            return [cls._sql_to_pydantic(result) for result in results]

        except SQLAlchemyError as e:
            logger.error(f"Database error during list: {e}")
            raise CRUDError(f"Failed to list records: {e}")

    @classmethod
    def update(cls, record_id: Any, data: Union[T, Dict[str, Any]]) -> Optional[T]:
        """Update a record by ID"""
        try:
            session = cls._get_session()
            config = cls._get_config()
            sql_model = cls._get_sql_model()

            # Convert Pydantic model to dict if needed
            if isinstance(data, BaseModel):
                data_dict = data.model_dump(exclude_unset=True)
            else:
                data_dict = data.copy()

            # Add audit fields
            if config.enable_audit and config.audit_columns.get("updated_at"):
                data_dict[config.audit_columns["updated_at"]] = datetime.utcnow()

            # Build update query
            query = update(sql_model).where(
                getattr(sql_model, config.primary_key) == record_id
            )

            # Apply soft delete filter
            if config.enable_soft_delete:
                query = query.where(
                    getattr(sql_model, config.soft_delete_column).is_(None)
                )

            query = query.values(**data_dict)

            result = session.execute(query)

            if result.rowcount == 0:
                return None

            session.commit()

            # Return updated record
            return cls.get_by_id(record_id)

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error during update: {e}")
            raise CRUDError(f"Failed to update record: {e}")

    @classmethod
    def delete(cls, record_id: Any, hard_delete: bool = False) -> bool:
        """Delete a record by ID"""
        try:
            session = cls._get_session()
            config = cls._get_config()
            sql_model = cls._get_sql_model()

            if config.enable_soft_delete and not hard_delete:
                # Soft delete
                query = (
                    update(sql_model)
                    .where(getattr(sql_model, config.primary_key) == record_id)
                    .where(getattr(sql_model, config.soft_delete_column).is_(None))
                    .values({config.soft_delete_column: datetime.utcnow()})
                )

                result = session.execute(query)
                success = result.rowcount > 0
            else:
                # Hard delete
                query = delete(sql_model).where(
                    getattr(sql_model, config.primary_key) == record_id
                )

                if config.enable_soft_delete and not hard_delete:
                    query = query.where(
                        getattr(sql_model, config.soft_delete_column).is_(None)
                    )

                result = session.execute(query)
                success = result.rowcount > 0

            if success:
                session.commit()

            return success

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error during delete: {e}")
            raise CRUDError(f"Failed to delete record: {e}")

    @classmethod
    def count(cls, filters: Optional[Dict[str, Any]] = None) -> int:
        """Count records with optional filtering"""
        try:
            session = cls._get_session()
            config = cls._get_config()
            sql_model = cls._get_sql_model()

            from sqlalchemy import func

            query = select(func.count()).select_from(sql_model)

            # Apply soft delete filter
            if config.enable_soft_delete:
                query = query.where(
                    getattr(sql_model, config.soft_delete_column).is_(None)
                )

            # Apply custom filters
            if filters:
                for field, value in filters.items():
                    if hasattr(sql_model, field):
                        query = query.where(getattr(sql_model, field) == value)

            result = session.execute(query).scalar()
            return result or 0

        except SQLAlchemyError as e:
            logger.error(f"Database error during count: {e}")
            raise CRUDError(f"Failed to count records: {e}")

    @classmethod
    def _sql_to_pydantic(cls, sql_obj) -> T:
        """Convert SQLAlchemy object to Pydantic model"""
        # Get all column values as dict
        data = {}
        for column in sql_obj.__table__.columns:
            data[column.name] = getattr(sql_obj, column.name)

        # Create Pydantic instance
        return cls(**data)


def timescale_crud(
    table_name: str,
    primary_key: str = "id",
    time_column: str = "created_at",
    enable_soft_delete: bool = False,
    soft_delete_column: str = "deleted_at",
    enable_audit: bool = True,
    audit_columns: Optional[Dict[str, str]] = None,
    create_hypertable: bool = True,
    chunk_time_interval: str = "1 day",
):
    """
    Decorator that adds CRUD operations to a Pydantic model for TimescaleDB.

    Args:
        table_name: Name of the database table
        primary_key: Name of the primary key column
        time_column: Name of the time column for hypertable partitioning
        enable_soft_delete: Enable soft delete functionality
        soft_delete_column: Column name for soft delete timestamp
        enable_audit: Enable audit fields (created_at, updated_at)
        audit_columns: Custom mapping for audit column names
        create_hypertable: Whether to create a TimescaleDB hypertable
        chunk_time_interval: Time interval for hypertable chunks

    Returns:
        Decorated Pydantic model class with CRUD operations
    """

    def decorator(pydantic_model: Type[T]) -> Type[T]:
        # Create CRUD config
        config = CRUDConfig(
            table_name=table_name,
            primary_key=primary_key,
            time_column=time_column,
            enable_soft_delete=enable_soft_delete,
            soft_delete_column=soft_delete_column,
            enable_audit=enable_audit,
            audit_columns=audit_columns
            or {"created_at": "created_at", "updated_at": "updated_at"},
        )

        # Create SQLAlchemy model dynamically
        sql_model_attrs = {
            "__tablename__": table_name,
        }

        # Add columns based on Pydantic model fields
        for field_name, field_info in pydantic_model.model_fields.items():
            python_type = field_info.annotation

            # Map Python types to SQLAlchemy types
            if python_type == int:
                if field_name == primary_key:
                    sql_model_attrs[field_name] = Column(
                        Integer, primary_key=True, autoincrement=True
                    )
                else:
                    sql_model_attrs[field_name] = Column(Integer)
            elif python_type == str:
                sql_model_attrs[field_name] = Column(String(255))
            elif python_type == float:
                sql_model_attrs[field_name] = Column(Float)
            elif python_type == bool:
                sql_model_attrs[field_name] = Column(Boolean)
            elif python_type == datetime:
                sql_model_attrs[field_name] = Column(DateTime)
            else:
                # Default to Text for complex types
                sql_model_attrs[field_name] = Column(Text)

        # Add audit columns if enabled
        if enable_audit:
            if audit_columns:
                for logical_name, column_name in audit_columns.items():
                    if logical_name in ["created_at", "updated_at"]:
                        sql_model_attrs[column_name] = Column(
                            DateTime, default=datetime.utcnow
                        )

        # Add soft delete column if enabled
        if enable_soft_delete:
            sql_model_attrs[soft_delete_column] = Column(DateTime, nullable=True)

        # Create SQLAlchemy model class
        sql_model = type(f"{pydantic_model.__name__}SQL", (Base,), sql_model_attrs)

        # Create new class that inherits from both Pydantic model and CRUDMixin
        class EnhancedModel(pydantic_model, CRUDMixin[pydantic_model]):
            pass

        # Set configuration
        EnhancedModel.set_config(config)
        EnhancedModel.set_sql_model(sql_model)

        # Store references for hypertable creation
        EnhancedModel._create_hypertable = create_hypertable
        EnhancedModel._chunk_time_interval = chunk_time_interval
        EnhancedModel._sql_model_class = sql_model

        # Add method to initialize database
        @classmethod
        def init_db(cls, engine, create_tables: bool = True):
            """Initialize database tables and hypertables"""
            if create_tables:
                Base.metadata.create_all(engine)

            if create_hypertable:
                # Create TimescaleDB hypertable
                with engine.connect() as conn:
                    try:
                        conn.execute(f"""
                            SELECT create_hypertable('{table_name}', '{time_column}', 
                                                    chunk_time_interval => INTERVAL '{chunk_time_interval}');
                        """)
                        conn.commit()
                        logger.info(f"Created hypertable for {table_name}")
                    except Exception as e:
                        # Hypertable might already exist
                        logger.warning(
                            f"Could not create hypertable for {table_name}: {e}"
                        )

        EnhancedModel.init_db = init_db

        # Preserve original class name and module
        EnhancedModel.__name__ = pydantic_model.__name__
        EnhancedModel.__qualname__ = pydantic_model.__qualname__
        EnhancedModel.__module__ = pydantic_model.__module__

        return EnhancedModel

    return decorator


# Utility function to create database session
def create_session(database_url: str, echo: bool = False) -> Session:
    """Create a database session"""
    engine = create_engine(database_url, echo=echo)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()
