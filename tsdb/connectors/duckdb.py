import logging
from typing import Any, Type

import duckdb
from pydantic import BaseModel

from .base import BaseConnector
from .exceptions import ConnectionError, ConnectorError

logger = logging.getLogger(__name__)


class DuckDBConnector(BaseConnector):
    """Connector for DuckDB.

    This connector stores data in a local or in-memory DuckDB database.
    It's suitable for local development, testing, or small-scale applications.
    """

    def __init__(self, model: Type[BaseModel], config: dict[str, Any]) -> None:
        super().__init__(model, config)
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

        columns = []
        for field_name, field in self.model.model_fields.items():
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

        create_table_sql = f'CREATE TABLE "{table_name}" ({", ".join(columns)})'
        try:
            conn.execute(create_table_sql)
            logger.info(f"Successfully created table '{table_name}' in DuckDB.")
        except Exception as e:
            logger.error(f"Failed to create table '{table_name}': {e}")
            raise ConnectorError(f"Failed to create table '{table_name}': {e}") from e

    def create(self, instance: BaseModel) -> BaseModel:
        conn = self._get_connection()
        table_name = self._get_table_name()
        data = instance.model_dump()
        columns = '", "'.join(data.keys())
        placeholders = ", ".join(["?" for _ in data])
        sql = f'INSERT INTO "{table_name}" ("{columns}") VALUES ({placeholders})'
        try:
            conn.execute(sql, list(data.values()))
            return instance
        except Exception as e:
            raise ConnectorError(f"Failed to create record: {e}") from e

    def get_by_id(self, item_id: Any) -> BaseModel | None:
        conn = self._get_connection()
        table_name = self._get_table_name()
        pk = self._get_primary_key()
        sql = f'SELECT * FROM "{table_name}" WHERE "{pk}" = ?'
        result = conn.execute(sql, [item_id]).fetchone()
        if result:
            columns = [desc[0] for desc in conn.description]
            return self.model(**dict(zip(columns, result)))
        return None

    def list(self, **kwargs) -> list[BaseModel]:
        conn = self._get_connection()
        table_name = self._get_table_name()
        sql = f'SELECT * FROM "{table_name}"'
        params = []
        if kwargs:
            conditions = []
            for key, value in kwargs.items():
                conditions.append(f'"{key}" = ?')
                params.append(value)
            sql += " WHERE " + " AND ".join(conditions)

        results = conn.execute(sql, params).fetchall()
        columns = [desc[0] for desc in conn.description]
        return [self.model(**dict(zip(columns, row))) for row in results]

    def update(self, item_id: Any, data: dict[str, Any]) -> BaseModel | None:
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

    def count(self, **kwargs) -> int:
        conn = self._get_connection()
        table_name = self._get_table_name()
        sql = f'SELECT COUNT(*) FROM "{table_name}"'
        params = []
        if kwargs:
            conditions = []
            for key, value in kwargs.items():
                conditions.append(f'"{key}" = ?')
                params.append(value)
            sql += " WHERE " + " AND ".join(conditions)

        result = conn.execute(sql, params).fetchone()
        return result[0] if result else 0

    def get_last_k_items(
        self, k: int, time_column: str | None = None
    ) -> list[BaseModel]:
        conn = self._get_connection()
        table_name = self._get_table_name()
        time_col = time_column or self.config.get("time_column", "created_at")

        sql = f'SELECT * FROM "{table_name}" ORDER BY "{time_col}" DESC LIMIT ?'
        results = conn.execute(sql, [k]).fetchall()
        columns = [desc[0] for desc in conn.description]
        return [self.model(**dict(zip(columns, row))) for row in results]
