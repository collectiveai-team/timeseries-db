import csv
import datetime
import io
from typing import Any, Iterable, Iterator, Type, TypeVar

import psycopg
from pydantic import BaseModel, TypeAdapter

from tsdb.io.protocols import BulkIOAdapter

Model = TypeVar("Model", bound=BaseModel)


def _chunked(iterable: Iterable[Any], size: int) -> Iterable[list[Any]]:
    """Yield successive chunks from iterable."""
    chunk = []
    for i, item in enumerate(iterable, 1):
        chunk.append(item)
        if i % size == 0:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


class TimescaleDBBulkIOAdapter(BulkIOAdapter[Model]):
    """Bulk IO adapter for TimescaleDB/Postgres."""

    def __init__(self, conn: psycopg.Connection, model: Type[Model]):
        self.conn = conn
        self.model = model
        self.model_fields = list(model.model_fields.keys())
        self.type_adapter = TypeAdapter(list[model])

    def read_iter(
        self, query: str, *, itersize: int = 5_000, **kwargs: Any
    ) -> Iterator[Model]:
        """Read data from TimescaleDB using a server-side cursor."""
        with self.conn.cursor(name="server_side_cursor") as cur:
            cur.itersize = itersize
            cur.execute(query, kwargs, binary=False)

            # Get column names from cursor description
            colnames = [desc.name for desc in cur.description]

            while True:
                rows = cur.fetchmany(itersize)
                if not rows:
                    break
                # Use model_construct to bypass validation on trusted DB rows
                for row in rows:
                    yield self.model.model_construct(**dict(zip(colnames, row)))

    def _batch_iterator(
        self, data: Iterable[Model], batch_size: int
    ) -> Iterator[list[Model]]:
        """Yield successive n-sized chunks from an iterable."""
        batch = []
        for item in data:
            batch.append(item)
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _format_value(self, value: Any) -> Any:
        """Format values for CSV serialization."""
        if isinstance(value, datetime.datetime):
            return value.isoformat()
        return value

    def _rows_of_models(self, models: Iterable[Model]):
        """Generator that yields tuples from Pydantic models for CSV writing."""
        for m in models:
            d = m.model_dump(mode="python")
            yield tuple(d[field] for field in self.model_fields)

    def write_bulk(
        self, table_name: str, data: Iterable[Model], *, batch_size: int = 100_000
    ) -> None:
        """Write Pydantic models to TimescaleDB in bulk using COPY."""
        with self.conn.cursor() as cur:
            # Optional: per-transaction durability tradeoff for performance
            # cur.execute("SET LOCAL synchronous_commit TO OFF")
            with cur.copy(
                f"COPY {table_name} ({','.join(self.model_fields)}) FROM STDIN (FORMAT CSV)"
            ) as cp:
                for batch in self._batch_iterator(data, batch_size):
                    # Use an in-memory text buffer to format CSV data, then write bytes.
                    with io.StringIO() as buffer:
                        writer = csv.writer(buffer)
                        writer.writerows(
                            [
                                tuple(
                                    self._format_value(getattr(row, f))
                                    for f in self.model_fields
                                )
                                for row in batch
                            ]
                        )
                        buffer.seek(0)
                        cp.write(buffer.read().encode("utf-8"))
