import logging
from typing import Any, Type, TypeVar

import duckdb
from pydantic import BaseModel
import builtins as blt

from tsdb.connectors.base import BaseConnector
from tsdb.connectors.exceptions import ConnectionError, ConnectorError

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class DuckDBConnector(BaseConnector[T]):
    """Connector for DuckDB.

    This connector stores data in a local or in-memory DuckDB database.
    It's suitable for local development, testing, or small-scale applications.
    """

    def __init__(self, model: Type[T], config: dict[str, Any]) -> None:
        # BaseConnector.__init__ is abstract; set attributes directly
        self.model = model
        self.config = config
        self.db_path = self.config.get("db_path", ":memory:")  # Default to in-memory
        self.conn: duckdb.DuckDBPyConnection | None = None

    def connect(self) -> None:
        """Establish a connection to the DuckDB database."""
        try:
            self.conn = duckdb.connect(database=self.db_path, read_only=False)
            logger.info(f"Successfully connected to DuckDB at '{self.db_path}'.")
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}")
            raise ConnectionError(f"Failed to connect to DuckDB: {e}") from e

    def disconnect(self) -> None:
        """Close the connection to the database."""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("DuckDB connection closed.")

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get the current connection, raising an error if not connected."""
        if not self.conn:
            raise ConnectionError("Not connected to DuckDB. Call connect() first.")
        return self.conn

    def _get_primary_key(self) -> str:
        return self.config.get("primary_key", "id")

    def _get_table_name(self) -> str:
        return self.config["table_name"]

    def create_table(self) -> None:
        """Create a table based on the Pydantic model's schema."""
        conn = self._get_connection()
        table_name = self._get_table_name()

        result = conn.execute(
            f"SELECT table_name FROM information_schema.tables WHERE table_name = '{table_name}'"
        ).fetchone()
        if result:
            logger.info(f"Table '{table_name}' already exists in DuckDB.")
            return

        pk_name = self._get_primary_key()
        sequence_name = f"{table_name}_{pk_name}_seq"
        try:
            conn.execute(f'CREATE SEQUENCE "{sequence_name}"')
        except Exception as e:
            # Assuming the sequence might already exist if table creation failed midway
            if "already exists" not in str(e):
                logger.error(f"Failed to create sequence '{sequence_name}': {e}")
                raise ConnectorError(
                    f"Failed to create sequence '{sequence_name}': {e}"
                ) from e

        columns = []
        pk_added = False
        for field_name, field in self.model.model_fields.items():
            if field_name == pk_name:
                columns.append(
                    f"\"{field_name}\" INTEGER PRIMARY KEY DEFAULT nextval('{sequence_name}')"
                )
                pk_added = True
            else:
                if "int" in str(field.annotation):
                    col_type = "INTEGER"
                elif "float" in str(field.annotation):
                    col_type = "DOUBLE"
                elif "str" in str(field.annotation):
                    col_type = "VARCHAR"
                elif "datetime" in str(field.annotation):
                    col_type = "TIMESTAMP"
                elif "bool" in str(field.annotation):
                    col_type = "BOOLEAN"
                else:
                    col_type = "VARCHAR"
                columns.append(f'"{field_name}" {col_type}')

        if not pk_added:
            columns.insert(
                0,
                f"\"{pk_name}\" INTEGER PRIMARY KEY DEFAULT nextval('{sequence_name}')",
            )

        create_table_sql = f'CREATE TABLE "{table_name}" ({", ".join(columns)})'
        try:
            conn.execute(create_table_sql)
            logger.info(f"Successfully created table '{table_name}' in DuckDB.")
        except Exception as e:
            logger.error(f"Failed to create table '{table_name}': {e}")
            raise ConnectorError(f"Failed to create table '{table_name}': {e}") from e

    def create(self, instance: T) -> T:
        conn = self._get_connection()
        table_name = self._get_table_name()
        data = instance.model_dump()
        pk_name = self._get_primary_key()
        if pk_name in data:
            del data[pk_name]
        columns = '", "'.join(data.keys())
        placeholders = ", ".join(["?" for _ in data])
        sql = f'INSERT INTO "{table_name}" ("{columns}") VALUES ({placeholders}) RETURNING *'
        try:
            result = conn.execute(sql, list(data.values())).fetchone()
            if result:
                columns = [desc[0] for desc in conn.description]
                return self.model(**dict(zip(columns, result)))
            raise ConnectorError("Failed to create record: no result returned.")
        except Exception as e:
            raise ConnectorError(f"Failed to create record: {e}") from e

    def get_by_id(self, item_id: Any) -> T | None:
        conn = self._get_connection()
        table_name = self._get_table_name()
        pk = self._get_primary_key()
        sql = f'SELECT * FROM "{table_name}" WHERE "{pk}" = ?'
        result = conn.execute(sql, [item_id]).fetchone()
        if result:
            columns = [desc[0] for desc in conn.description]
            return self.model(**dict(zip(columns, result)))
        return None

    def list(
        self,
        *,
        limit: int | None = 100,
        offset: int = 0,
        filters: dict[str, Any] | None = None,
        order_by: str | None = None,
        order_desc: bool = False,
        **kwargs: Any,
    ) -> blt.list[T]:
        conn = self._get_connection()
        table_name = self._get_table_name()
        sql = f'SELECT * FROM "{table_name}"'
        params: list[Any] = []

        # Merge legacy kwargs into filters for backward-compat
        if filters is None and kwargs:
            filters = {**kwargs}

        if filters:
            conditions = []
            for key, value in filters.items():
                conditions.append(f'"{key}" = ?')
                params.append(value)
            sql += " WHERE " + " AND ".join(conditions)

        if order_by:
            direction = "DESC" if order_desc else "ASC"
            sql += f' ORDER BY "{order_by}" {direction}'

        if offset:
            sql += " OFFSET ?"
            params.append(offset)

        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)

        results = conn.execute(sql, params).fetchall()
        columns = [desc[0] for desc in conn.description]
        return [self.model(**dict(zip(columns, row))) for row in results]

    def list_all(
        self,
        *,
        filters: dict[str, Any] | None = None,
        order_by: str | None = None,
        order_desc: bool = False,
        **kwargs: Any,
    ) -> blt.list[T]:
        return self.list(
            limit=None,
            offset=0,
            filters=filters,
            order_by=order_by,
            order_desc=order_desc,
            **kwargs,
        )

    def update(self, item_id: Any, data: dict[str, Any]) -> T | None:
        conn = self._get_connection()
        table_name = self._get_table_name()
        pk = self._get_primary_key()

        set_clauses = []
        params = []
        for key, value in data.items():
            set_clauses.append(f'"{key}" = ?')
            params.append(value)

        params.append(item_id)
        sql = f'UPDATE "{table_name}" SET {", ".join(set_clauses)} WHERE "{pk}" = ?'

        try:
            conn.execute(sql, params)
            return self.get_by_id(item_id)
        except Exception as e:
            raise ConnectorError(f"Failed to update record: {e}") from e

    def delete(self, item_id: Any, hard_delete: bool = False) -> None:
        # DuckDB connector doesn't support soft delete in this simple implementation
        conn = self._get_connection()
        table_name = self._get_table_name()
        pk = self._get_primary_key()
        sql = f'DELETE FROM "{table_name}" WHERE "{pk}" = ?'
        try:
            conn.execute(sql, [item_id])
        except Exception as e:
            raise ConnectorError(f"Failed to delete record: {e}") from e

    def count(self, *, filters: dict[str, Any] | None = None, **kwargs: Any) -> int:
        conn = self._get_connection()
        table_name = self._get_table_name()
        sql = f'SELECT COUNT(*) FROM "{table_name}"'
        params: list[Any] = []

        # Merge legacy kwargs into filters for backward-compat
        if filters is None and kwargs:
            filters = {**kwargs}

        if filters:
            conditions = []
            for key, value in filters.items():
                conditions.append(f'"{key}" = ?')
                params.append(value)
            sql += " WHERE " + " AND ".join(conditions)

        result = conn.execute(sql, params).fetchone()
        return result[0] if result else 0

    def bulk_insert(self, instances: blt.list[T]) -> blt.list[T]:
        """Insert multiple records efficiently using executemany.

        Falls back to no-op if instances is empty.
        """
        if not instances:
            return []

        conn = self._get_connection()
        table_name = self._get_table_name()

        # Assume all instances share the same schema
        first = instances[0]
        data_keys = list(first.model_dump().keys())
        columns = '", "'.join(data_keys)
        placeholders = ", ".join(["?" for _ in data_keys])
        sql = f'INSERT INTO "{table_name}" ("{columns}") VALUES ({placeholders})'

        rows: list[list[Any]] = []
        for inst in instances:
            data = inst.model_dump()
            rows.append([data[k] for k in data_keys])

        try:
            conn.executemany(sql, rows)
            return instances
        except Exception as e:
            raise ConnectorError(f"Failed bulk insert: {e}") from e

    def get_last_k_items(self, k: int, time_column: str | None = None) -> blt.list[T]:
        conn = self._get_connection()
        table_name = self._get_table_name()
        time_col = time_column or self.config.get("time_column", "created_at")

        sql = f'SELECT * FROM "{table_name}" ORDER BY "{time_col}" DESC LIMIT ?'
        results = conn.execute(sql, [k]).fetchall()
        columns = [desc[0] for desc in conn.description]
        return [self.model(**dict(zip(columns, row))) for row in results]
