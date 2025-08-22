"""
This module exposes the decorators for easy import.
"""

from tsdb.decorators.darts_decorator import timeseries_storage
from tsdb.decorators.pydantic_decorator import db_crud

__all__ = ["db_crud", "timeseries_storage"]
