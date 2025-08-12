"""
TimescaleDB CRUD Decorator for Pydantic Models

This module provides a decorator that adds CRUD operations to Pydantic models
for use with SQLAlchemy and TimescaleDB.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, Optional, Type, TypeVar, Union

from pydantic import BaseModel
from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    String,
    Text,
    Boolean,
    Float,
    create_engine,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from tsdb.crud import CRUDMixin, CRUDConfig

logger = logging.getLogger(__name__)

# Type variables
T = TypeVar("T", bound=BaseModel)
SQLModelType = TypeVar("SQLModelType")

Base = declarative_base()


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

            # Handle Optional/Union types (e.g., int | None, Optional[int])
            actual_type = python_type
            nullable = False

            # Check if it's a Union type (including Optional)
            # Handle both old typing.Union and new Python 3.10+ union syntax (int | None)
            is_union = False
            union_args = None

            if hasattr(python_type, "__origin__") and python_type.__origin__ is Union:
                # Old typing.Union syntax
                is_union = True
                union_args = python_type.__args__
            elif (
                hasattr(python_type, "__class__")
                and python_type.__class__.__name__ == "UnionType"
            ):
                # New Python 3.10+ union syntax (int | None)
                is_union = True
                union_args = python_type.__args__

            if is_union and union_args:
                # Get non-None types from Union
                non_none_types = [t for t in union_args if t is not type(None)]
                if len(non_none_types) == 1:
                    actual_type = non_none_types[0]
                    nullable = type(None) in union_args
                else:
                    # Multiple non-None types, default to Text
                    actual_type = str
                    nullable = type(None) in union_args

            # Map Python types to SQLAlchemy types
            if actual_type is int:
                if field_name == primary_key:
                    sql_model_attrs[field_name] = Column(
                        Integer, primary_key=True, autoincrement=True
                    )
                else:
                    sql_model_attrs[field_name] = Column(Integer, nullable=nullable)
            elif actual_type is str:
                sql_model_attrs[field_name] = Column(String(255), nullable=nullable)
            elif actual_type is float:
                sql_model_attrs[field_name] = Column(Float, nullable=nullable)
            elif actual_type is bool:
                sql_model_attrs[field_name] = Column(Boolean, nullable=nullable)
            elif actual_type is datetime:
                sql_model_attrs[field_name] = Column(DateTime, nullable=nullable)
            else:
                # Default to Text for complex types
                sql_model_attrs[field_name] = Column(Text, nullable=nullable)

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

        # Add TimescaleDB hypertable configuration if enabled
        if create_hypertable:
            sql_model_attrs["__table_args__"] = {
                "timescaledb_hypertable": {
                    "time_column_name": time_column,
                    "chunk_time_interval": chunk_time_interval,
                }
            }

        # Create SQLAlchemy model class with unique name based on table name
        # Convert table name to CamelCase for class name
        table_class_name = "".join(
            word.capitalize() for word in config.table_name.split("_")
        )
        sql_model_class_name = f"{table_class_name}SQL"
        sql_model = type(sql_model_class_name, (Base,), sql_model_attrs)

        # Create new class that inherits from both Pydantic model and CRUDMixin
        class EnhancedModel(pydantic_model, CRUDMixin[pydantic_model]):
            def save(self, session) -> 'EnhancedModel':
                """Save this instance to the database (create or update)"""
                # Extract underlying SQLAlchemy session if it's a CRUDSession wrapper
                actual_session = getattr(session, '_session', session)
                
                # Set session for the class
                self.__class__.set_session(actual_session)
                
                if self._get_primary_key_value() is None:
                    # Create new record
                    return CRUDMixin.create.__func__(self.__class__, self)
                else:
                    # Update existing record
                    pk_value = self._get_primary_key_value()
                    return CRUDMixin.update.__func__(self.__class__, pk_value, self)
            
            def delete(self, session, hard_delete: bool = False) -> bool:
                """Delete this instance from the database"""
                # Extract underlying SQLAlchemy session if it's a CRUDSession wrapper
                actual_session = getattr(session, '_session', session)
                
                # Set session for the class
                self.__class__.set_session(actual_session)
                
                pk_value = self._get_primary_key_value()
                if pk_value is None:
                    raise ValueError("Cannot delete instance without primary key")
                return CRUDMixin.delete.__func__(self.__class__, pk_value, hard_delete=hard_delete)
            
            def refresh(self, session) -> 'EnhancedModel':
                """Refresh this instance from the database"""
                # Extract underlying SQLAlchemy session if it's a CRUDSession wrapper
                actual_session = getattr(session, '_session', session)
                
                # Set session for the class
                self.__class__.set_session(actual_session)
                
                pk_value = self._get_primary_key_value()
                if pk_value is None:
                    raise ValueError("Cannot refresh instance without primary key")
                refreshed = CRUDMixin.get_by_id.__func__(self.__class__, pk_value)
                if refreshed is None:
                    config = self.__class__._get_config()
                    raise ValueError(f"Record with {config.primary_key}={pk_value} not found")
                return refreshed
            
            def _get_primary_key_value(self):
                """Get the primary key value of this instance"""
                config = self.__class__._get_config()
                return getattr(self, config.primary_key, None)

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

# Utility function to create CRUD session with enhanced functionality
def create_crud_session(database_url: str, echo: bool = False):
    """Create a CRUD session with enhanced save/delete functionality"""
    from tsdb.crud.session import CRUDSession
    engine = create_engine(database_url, echo=echo)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return CRUDSession(SessionLocal())
