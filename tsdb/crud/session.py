"""
Enhanced CRUD Session for SQLModel-like functionality

This module provides a session wrapper that enables SQLModel-like syntax
for saving and deleting model instances.
"""

from __future__ import annotations

import logging
from typing import TypeVar, Any

from pydantic import BaseModel
from sqlalchemy.orm import Session
from tsdb.crud.exceptions import CRUDError

logger = logging.getLogger(__name__)

# Type variables
T = TypeVar("T", bound=BaseModel)


class CRUDSession:
    """
    Enhanced session wrapper that provides SQLModel-like functionality.

    Enables syntax like:
    - session.save(user)
    - session.delete(user)
    - session.refresh(user)
    """

    def __init__(self, session: Session):
        """Initialize with a SQLAlchemy session"""
        self._session = session

    def save(self, instance: T) -> T:
        """
        Save an instance to the database (create or update).

        Args:
            instance: Pydantic model instance with CRUD capabilities

        Returns:
            The saved instance with updated fields

        Raises:
            CRUDError: If the instance doesn't support CRUD operations
        """
        if not hasattr(instance, "save"):
            raise CRUDError(
                f"Instance of type {type(instance).__name__} does not support CRUD operations. "
                "Make sure the model is decorated with @timescale_crud."
            )

        # Set the session for the model class if not already set
        if hasattr(instance.__class__, "set_session"):
            instance.__class__.set_session(self._session)

        return instance.save(self._session)

    def delete(self, instance: T, hard_delete: bool = False) -> bool:
        """
        Delete an instance from the database.

        Args:
            instance: Pydantic model instance with CRUD capabilities
            hard_delete: Whether to perform a hard delete (bypass soft delete)

        Returns:
            True if deletion was successful, False otherwise

        Raises:
            CRUDError: If the instance doesn't support CRUD operations
        """
        if not hasattr(instance, "delete"):
            raise CRUDError(
                f"Instance of type {type(instance).__name__} does not support CRUD operations. "
                "Make sure the model is decorated with @timescale_crud."
            )

        # Set the session for the model class if not already set
        if hasattr(instance.__class__, "set_session"):
            instance.__class__.set_session(self._session)

        return instance.delete(self._session, hard_delete=hard_delete)

    def refresh(self, instance: T) -> T:
        """
        Refresh an instance from the database.

        Args:
            instance: Pydantic model instance with CRUD capabilities

        Returns:
            The refreshed instance with current database values

        Raises:
            CRUDError: If the instance doesn't support CRUD operations
        """
        if not hasattr(instance, "refresh"):
            raise CRUDError(
                f"Instance of type {type(instance).__name__} does not support CRUD operations. "
                "Make sure the model is decorated with @timescale_crud."
            )

        # Set the session for the model class if not already set
        if hasattr(instance.__class__, "set_session"):
            instance.__class__.set_session(self._session)

        return instance.refresh(self._session)

    def get_by_id(self, model_class: type[T], record_id: Any) -> T | None:
        """
        Get a record by ID using the model class.

        Args:
            model_class: The model class to query
            record_id: The primary key value

        Returns:
            The found instance or None
        """
        if hasattr(model_class, "set_session"):
            model_class.set_session(self._session)

        return model_class.get_by_id(record_id)

    def list(self, model_class: type[T], **kwargs) -> list[T]:
        """
        List records using the model class.

        Args:
            model_class: The model class to query
            **kwargs: Additional arguments for the list method

        Returns:
            List of found instances
        """
        if hasattr(model_class, "set_session"):
            model_class.set_session(self._session)

        return model_class.list(**kwargs)

    def count(self, model_class: type[T], **kwargs) -> int:
        """
        Count records using the model class.

        Args:
            model_class: The model class to query
            **kwargs: Additional arguments for the count method

        Returns:
            Number of matching records
        """
        if hasattr(model_class, "set_session"):
            model_class.set_session(self._session)

        return model_class.count(**kwargs)

    def bulk_insert(
        self, instances: list[T], batch_size: int = 1000, **kwargs
    ) -> list[T]:
        """
        Bulk insert a list of instances to the database.

        Args:
            instances: List of Pydantic model instances with CRUD capabilities
            batch_size: Number of records to insert per batch (default: 1000)
            **kwargs: Additional arguments passed to the model's bulk_insert method

        Returns:
            List of created instances (may be empty if return_defaults=False)

        Raises:
            CRUDError: If the instances don't support CRUD operations
        """
        if len(instances) == 0:
            return []

        # Check if the first instance supports bulk_insert
        first_instance = instances[0]
        model_class = type(first_instance)

        if not hasattr(model_class, "bulk_insert"):
            raise CRUDError(
                f"Model class {model_class.__name__} does not support bulk_insert operations. "
                "Make sure the model is decorated with @timescale_crud."
            )

        # Set the session for the model class if not already set
        if hasattr(model_class, "set_session"):
            model_class.set_session(self._session)

        # Convert instances to the format expected by bulk_insert
        # The bulk_insert method expects List[Union[T, Dict[str, Any]]]
        return model_class.bulk_insert(instances, batch_size=batch_size, **kwargs)

    def commit(self) -> None:
        """Commit the current transaction"""
        self._session.commit()

    def rollback(self) -> None:
        """Rollback the current transaction"""
        self._session.rollback()

    def close(self) -> None:
        """Close the session"""
        self._session.close()

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()
        self.close()

    @property
    def session(self) -> Session:
        """Access to the underlying SQLAlchemy session"""
        return self._session
