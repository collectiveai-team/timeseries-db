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
    select,
    update,
    delete,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from tsdb.crud.exceptions import CRUDError

logger = logging.getLogger(__name__)

# Type variables
T = TypeVar("T", bound=TimeSeries)
SQLModelType = TypeVar("SQLModelType")

Base = declarative_base()


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
        session = cls._get_session()
        try:
            config = cls._get_config()
            sql_model = cls._get_sql_model()

            # Convert Pydantic model to dict if needed
            if isinstance(data, BaseModel):
                data_dict = data.model_dump()
            else:
                # Create Pydantic instance first to trigger defaults, then convert to dict
                pydantic_instance = cls(**data)
                data_dict = pydantic_instance.model_dump()

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
        session = cls._get_session()
        try:
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
        session = cls._get_session()
        try:
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
        session = cls._get_session()
        try:
            config = cls._get_config()
            sql_model = cls._get_sql_model()

            # Convert Pydantic model to dict if needed
            if isinstance(data, BaseModel):
                data_dict = data.model_dump()
            else:
                # For updates, just copy the dict since it's partial data (not all fields required)
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
        session = cls._get_session()
        try:
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
        session = cls._get_session()
        try:
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
    def bulk_insert(
        cls,
        data_list: List[Union[T, Dict[str, Any]]],
        batch_size: int = 1000,
        return_defaults: bool = False,
    ) -> List[T]:
        """
        Create multiple records efficiently using bulk insert

        Args:
            data_list: List of Pydantic models or dictionaries to insert
            batch_size: Number of records to insert per batch (default: 1000)
            return_defaults: If True, return created records with defaults/IDs (slower)

        Returns:
            List of created Pydantic models (empty if return_defaults=False)
        """
        session = cls._get_session()
        try:
            config = cls._get_config()
            sql_model = cls._get_sql_model()

            if not data_list:
                return []

            created_records = []

            for i in range(0, len(data_list), batch_size):
                batch = data_list[i : i + batch_size]
                batch_dicts = []

                # Convert batch to dictionaries with audit fields
                for item in batch:
                    if isinstance(item, BaseModel):
                        data_dict = item.model_dump()
                    else:
                        pydantic_instance = cls(**item)
                        data_dict = pydantic_instance.model_dump()

                    # Add audit fields
                    if config.enable_audit:
                        now = datetime.utcnow()
                        if config.audit_columns.get("created_at"):
                            data_dict[config.audit_columns["created_at"]] = now
                        if config.audit_columns.get("updated_at"):
                            data_dict[config.audit_columns["updated_at"]] = now

                    batch_dicts.append(data_dict)

                # Use bulk_insert_mappings for maximum performance
                session.bulk_insert_mappings(sql_model, batch_dicts)
                session.commit()

            return created_records

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error during bulk create: {e}")
            raise CRUDError(f"Failed to bulk create records: {e}")

    @classmethod
    def _sql_to_pydantic(cls, sql_obj) -> T:
        """Convert SQLAlchemy object to Pydantic model"""
        # Get all column values as dict
        data = {}
        for column in sql_obj.__table__.columns:
            data[column.name] = getattr(sql_obj, column.name)

        # Create Pydantic instance
        return cls(**data)
