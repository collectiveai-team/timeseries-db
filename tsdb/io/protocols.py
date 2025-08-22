from typing import Any, Iterable, Iterator, Protocol, Type, TypeVar

from pydantic import BaseModel

Model = TypeVar("Model", bound=BaseModel)


class BulkIOAdapter(Protocol[Model]):
    """Protocol for a bulk IO adapter."""

    def read_iter(
        self, query: str, *, itersize: int = 5_000, **kwargs: Any
    ) -> Iterator[Model]:
        """Read data from the database and yield it as an iterator of Pydantic models."""
        ...

    def write_bulk(
        self, table_name: str, data: Iterable[Model], *, batch_size: int = 50_000
    ) -> None:
        """Write a large volume of Pydantic models to the database."""
        ...


def get_bulk_io_adapter(conn: Any, model: Type[Model]) -> BulkIOAdapter[Model]:
    """Get the appropriate bulk IO adapter for the given connection."""
    # a bit of a hack to avoid circular imports
    from . import duckdb, timescaledb

    conn_module = conn.__class__.__module__
    if conn_module.startswith("duckdb"):
        return duckdb.DuckDBBulkIOAdapter(conn, model)
    if conn_module.startswith("psycopg"):
        return timescaledb.TimescaleDBBulkIOAdapter(conn, model)
    raise NotImplementedError(f"No bulk IO adapter for connection type: {type(conn)}")
