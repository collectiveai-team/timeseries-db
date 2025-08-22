from typing import Any, Iterable, Iterator, Type, TypeVar

import duckdb
import pyarrow as pa
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


class DuckDBBulkIOAdapter(BulkIOAdapter[Model]):
    """Bulk IO adapter for DuckDB."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, model: Type[Model]):
        self.conn = conn
        self.model = model
        self.model_fields = list(model.model_fields.keys())
        self.type_adapter = TypeAdapter(list[model])

    def read_iter(
        self, query: str, *, itersize: int = 50_000, **kwargs: Any
    ) -> Iterator[Model]:
        """Read data from DuckDB using Arrow streaming and yield Pydantic models."""
        arrow_table = self.conn.execute(query, parameters=list(kwargs.values())).arrow()
        for batch in arrow_table.to_batches(max_chunksize=itersize):
            # Using model_construct to bypass validation for performance on trusted data
            for row in batch.to_pylist():
                yield self.model.model_construct(**row)

    def _to_arrow_chunk(self, models: Iterable[Model]) -> pa.Table:
        """Convert an iterable of Pydantic models to a PyArrow Table."""
        # Vectorized conversion for performance
        cols = {field: [] for field in self.model_fields}
        for m in models:
            d = m.model_dump(mode="python")
            for field in self.model_fields:
                cols[field].append(d.get(field))
        return pa.Table.from_pydict(cols)

    def write_bulk(
        self, table_name: str, data: Iterable[Model], *, batch_size: int = 200_000
    ) -> None:
        """Write Pydantic models to DuckDB in bulk using Arrow tables."""
        for chunk in _chunked(data, batch_size):
            tbl = self._to_arrow_chunk(chunk)
            # Register the Arrow table as a temporary view for zero-copy integration
            self.conn.register("chunk_tbl", tbl)
            self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM chunk_tbl")
            self.conn.unregister("chunk_tbl")
