import logging
import types
from datetime import datetime
from typing import Any, TypeVar
import builtins as blt

from pydantic import BaseModel
from sqlalchemy import (
    create_engine,
    select,
    update,
    delete,
    Column,
    DateTime,
    Integer,
    String,
    Text,
    Boolean,
    Float,
    func,
)
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError

from tsdb.connectors.base import BaseConnector
from tsdb.connectors.exceptions import ConnectionError, ConnectorError

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)
Base = declarative_base()


class TimescaleDBConnector(BaseConnector[T]):
    """Connector for TimescaleDB using SQLAlchemy."""

    def __init__(self, model: type[T], config: dict[str, Any]):
        self.model = model
        self.config = config
        self.engine = None
        self.SessionLocal = None
        self._sql_model = self._create_sql_model()

    def connect(self):
        try:
            self.engine = create_engine(
                self.config["db_uri"], echo=self.config.get("echo", False)
            )
            self.SessionLocal = sessionmaker(
                autocommit=False, autoflush=False, bind=self.engine
            )
        except Exception as e:
            raise ConnectionError(f"Failed to connect to TimescaleDB: {e}")

    def disconnect(self):
        if self.engine:
            self.engine.dispose()

    def _create_sql_model(self) -> type:
        """Create a SQLAlchemy model dynamically from a Pydantic model."""
        table_name = self.config["table_name"]
        primary_key = self.config.get("primary_key", "id")
        time_column = self.config.get("time_column", "created_at")

        sql_model_attrs = {"__tablename__": table_name}

        for field_name, field_info in self.model.model_fields.items():
            python_type = field_info.annotation
            actual_type, nullable = self._get_type_and_nullable(python_type)

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
                sql_model_attrs[field_name] = Column(Text, nullable=nullable)

        if self.config.get("enable_audit", True):
            audit_columns = self.config.get("audit_columns", {})
            created_at = audit_columns.get("created_at", "created_at")
            updated_at = audit_columns.get("updated_at", "updated_at")
            sql_model_attrs[created_at] = Column(DateTime, default=datetime.utcnow)
            sql_model_attrs[updated_at] = Column(
                DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
            )

        if self.config.get("enable_soft_delete", False):
            soft_delete_column = self.config.get("soft_delete_column", "deleted_at")
            sql_model_attrs[soft_delete_column] = Column(DateTime, nullable=True)

        if self.config.get("create_hypertable", True):
            sql_model_attrs["__table_args__"] = {
                "timescaledb_hypertable": {
                    "time_column_name": time_column,
                    "chunk_time_interval": self.config.get(
                        "chunk_time_interval", "1 day"
                    ),
                }
            }

        table_class_name = "".join(word.capitalize() for word in table_name.split("_"))
        sql_model_class_name = f"{table_class_name}SQL"
        return type(sql_model_class_name, (Base,), sql_model_attrs)

    def _get_type_and_nullable(self, python_type):
        nullable = False
        actual_type = python_type
        if (
            hasattr(python_type, "__origin__")
            and python_type.__origin__ is types.UnionType
        ) or isinstance(python_type, types.UnionType):
            union_args = python_type.__args__
            non_none_types = [t for t in union_args if t is not type(None)]
            if len(non_none_types) == 1:
                actual_type = non_none_types[0]
                nullable = type(None) in union_args
        return actual_type, nullable

    def create_table(self):
        if not self.engine:
            raise ConnectorError("Connector is not connected. Call connect() first.")
        Base.metadata.create_all(self.engine)

    def _get_session(self) -> Session:
        if not self.SessionLocal:
            raise ConnectorError("Connector is not connected. Call connect() first.")
        return self.SessionLocal()

    def create(self, data: T) -> T:
        session = self._get_session()
        try:
            data_dict = data.model_dump()

            if self.config.get("enable_audit", True):
                now = datetime.utcnow()
                audit_columns = self.config.get("audit_columns", {})
                if audit_columns.get("created_at"):
                    data_dict[audit_columns["created_at"]] = now
                if audit_columns.get("updated_at"):
                    data_dict[audit_columns["updated_at"]] = now

            db_obj = self._sql_model(**data_dict)
            session.add(db_obj)
            session.commit()
            session.refresh(db_obj)
            return self._sql_to_pydantic(db_obj)
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error during create: {e}")
            raise ConnectorError(f"Failed to create record: {e}")
        finally:
            session.close()

    def get_by_id(self, record_id: Any) -> T | None:
        session = self._get_session()
        try:
            primary_key = self.config.get("primary_key", "id")
            query = select(self._sql_model).where(
                getattr(self._sql_model, primary_key) == record_id
            )

            if self.config.get("enable_soft_delete", False):
                soft_delete_column = self.config.get("soft_delete_column", "deleted_at")
                query = query.where(
                    getattr(self._sql_model, soft_delete_column).is_(None)
                )

            result = session.execute(query).scalar_one_or_none()
            return self._sql_to_pydantic(result) if result else None
        except SQLAlchemyError as e:
            logger.error(f"Database error during get_by_id: {e}")
            raise ConnectorError(f"Failed to get record: {e}")
        finally:
            session.close()

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
        session = self._get_session()
        try:
            query = select(self._sql_model)

            if self.config.get("enable_soft_delete", False):
                soft_delete_column = self.config.get("soft_delete_column", "deleted_at")
                query = query.where(
                    getattr(self._sql_model, soft_delete_column).is_(None)
                )

            if filters:
                for field, value in filters.items():
                    if hasattr(self._sql_model, field):
                        query = query.where(getattr(self._sql_model, field) == value)

            if order_by and hasattr(self._sql_model, order_by):
                column = getattr(self._sql_model, order_by)
                query = query.order_by(column.desc() if order_desc else column)

            query = query.offset(offset)
            if limit is not None:
                query = query.limit(limit)
            results = session.execute(query).scalars().all()
            return [self._sql_to_pydantic(res) for res in results]
        except SQLAlchemyError as e:
            logger.error(f"Database error during list: {e}")
            raise ConnectorError(f"Failed to list records: {e}")
        finally:
            session.close()

    def update(self, record_id: Any, data: dict[str, Any]) -> T | None:
        session = self._get_session()
        try:
            primary_key = self.config.get("primary_key", "id")

            if self.config.get("enable_audit", True):
                audit_columns = self.config.get("audit_columns", {})
                if audit_columns.get("updated_at"):
                    data[audit_columns["updated_at"]] = datetime.utcnow()

            query = (
                update(self._sql_model)
                .where(getattr(self._sql_model, primary_key) == record_id)
                .values(**data)
            )

            if self.config.get("enable_soft_delete", False):
                soft_delete_column = self.config.get("soft_delete_column", "deleted_at")
                query = query.where(
                    getattr(self._sql_model, soft_delete_column).is_(None)
                )

            result = session.execute(query)
            if result.rowcount == 0:
                return None

            session.commit()
            return self.get_by_id(record_id)
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error during update: {e}")
            raise ConnectorError(f"Failed to update record: {e}")
        finally:
            session.close()

    def delete(self, record_id: Any, hard_delete: bool = False) -> bool:
        session = self._get_session()
        try:
            primary_key = self.config.get("primary_key", "id")
            soft_delete = self.config.get("enable_soft_delete", False)
            soft_delete_column = self.config.get("soft_delete_column", "deleted_at")

            if soft_delete and not hard_delete:
                query = (
                    update(self._sql_model)
                    .where(getattr(self._sql_model, primary_key) == record_id)
                    .where(getattr(self._sql_model, soft_delete_column).is_(None))
                    .values({soft_delete_column: datetime.utcnow()})
                )
            else:
                query = delete(self._sql_model).where(
                    getattr(self._sql_model, primary_key) == record_id
                )

            result = session.execute(query)
            session.commit()
            return result.rowcount > 0
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error during delete: {e}")
            raise ConnectorError(f"Failed to delete record: {e}")
        finally:
            session.close()

    def count(self, filters: dict[str, Any] | None = None) -> int:
        session = self._get_session()
        try:
            query = select(func.count()).select_from(self._sql_model)

            if self.config.get("enable_soft_delete", False):
                soft_delete_column = self.config.get("soft_delete_column", "deleted_at")
                query = query.where(
                    getattr(self._sql_model, soft_delete_column).is_(None)
                )

            if filters:
                for field, value in filters.items():
                    if hasattr(self._sql_model, field):
                        query = query.where(getattr(self._sql_model, field) == value)

            result = session.execute(query).scalar()
            return result or 0
        except SQLAlchemyError as e:
            logger.error(f"Database error during count: {e}")
            raise ConnectorError(f"Failed to count records: {e}")
        finally:
            session.close()

    def bulk_insert(self, data_list: blt.list[T]) -> blt.list[T]:
        session = self._get_session()
        try:
            if not data_list:
                return []

            batch_dicts = []
            for item in data_list:
                data_dict = item.model_dump()
                if self.config.get("enable_audit", True):
                    now = datetime.utcnow()
                    audit_columns = self.config.get("audit_columns", {})
                    if audit_columns.get("created_at"):
                        data_dict[audit_columns["created_at"]] = now
                    if audit_columns.get("updated_at"):
                        data_dict[audit_columns["updated_at"]] = now
                batch_dicts.append(data_dict)

            session.bulk_insert_mappings(self._sql_model, batch_dicts)
            session.commit()
            return []  # bulk_insert_mappings does not return objects
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error during bulk create: {e}")
            raise ConnectorError(f"Failed to bulk create records: {e}")
        finally:
            session.close()

    def get_last_k_items(
        self, k: int, filters: dict[str, Any] | None = None
    ) -> blt.list[T]:
        time_column = self.config.get("time_column", "created_at")
        return self.list_all(
            limit=k, filters=filters, order_by=time_column, order_desc=True
        )

    def _sql_to_pydantic(self, sql_obj) -> T:
        if not sql_obj:
            return None
        data = {c.name: getattr(sql_obj, c.name) for c in sql_obj.__table__.columns}
        return self.model(**data)
