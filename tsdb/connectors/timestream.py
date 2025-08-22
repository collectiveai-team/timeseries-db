import logging
from typing import Any, Type

from pydantic import BaseModel

from .base import BaseConnector
from .exceptions import (
    ConfigurationError,
    ConnectionError,
    ConnectorError,
)

logger = logging.getLogger(__name__)


try:
    import boto3
except ImportError:
    logger.warning(
        "boto3 not found. Please install it with `pip install boto3` to use the AWSTimestreamConnector."
    )
    boto3 = None


class AWSTimestreamConnector(BaseConnector):
    """Connector for AWS Timestream."""

    def __init__(self, model: Type[BaseModel], config: dict[str, Any]) -> None:
        super().__init__(model, config)
        if not boto3:
            raise ImportError(
                "boto3 is required for AWSTimestreamConnector. Please install it."
            )
        self.region_name = self.config.get("aws_region")
        self.database_name = self.config.get("database_name")
        self.table_name = self.config.get("table_name")
        self.write_client = None
        self.query_client = None

    def connect(self) -> None:
        """Establish a connection to AWS Timestream."""
        try:
            self.write_client = boto3.client(
                "timestream-write", region_name=self.region_name
            )
            self.query_client = boto3.client(
                "timestream-query", region_name=self.region_name
            )
            logger.info(
                f"Successfully connected to AWS Timestream in region '{self.region_name}'."
            )
        except Exception as e:
            raise ConnectionError(f"Failed to connect to AWS Timestream: {e}") from e

    def disconnect(self) -> None:
        """Disconnect is not explicitly needed for boto3 clients."""
        self.write_client = None
        self.query_client = None
        logger.info("AWS Timestream clients have been cleared.")

    def create_table(self) -> None:
        """Create a table in Timestream. (Handled outside connector for now)"""
        # In a real scenario, you might check if the database and table exist.
        # For this example, we assume they are pre-configured in AWS.
        logger.info(
            f"Table creation for '{self.database_name}.{self.table_name}' is assumed to be handled manually in AWS Timestream."
        )

    def create(self, instance: BaseModel) -> BaseModel:
        """Write a single record to Timestream."""
        if not self.write_client:
            raise ConnectionError("Not connected to AWS Timestream.")

        dimensions = []
        if self.config.get("tags"):
            for tag_name in self.config["tags"]:
                tag_value = getattr(instance, tag_name, None)
                if tag_value is not None:
                    dimensions.append({"Name": tag_name, "Value": str(tag_value)})

        if not dimensions:
            raise ConfigurationError(
                "At least one dimension (tag) must be specified for a Timestream record."
            )

        time_column = self.config.get("time_column", "time")
        record_time = getattr(instance, time_column)

        # Convert to milliseconds since epoch
        time_value = str(int(record_time.timestamp() * 1000))

        common_attributes = {
            "Dimensions": dimensions,
            "Time": time_value,
            "TimeUnit": "MILLISECONDS",
        }

        records = []
        instance_dict = instance.dict()
        measure_fields = {
            k
            for k, v in self.model.__fields__.items()
            if k not in self.config.get("tags", []) and k != time_column
        }

        for field in measure_fields:
            value = instance_dict.get(field)
            if value is not None:
                # Determine measure value type
                if isinstance(value, bool):
                    measure_type = "BOOLEAN"
                    measure_value = str(value)
                elif isinstance(value, int) or isinstance(value, float):
                    measure_type = "DOUBLE"
                    measure_value = str(value)
                else:
                    measure_type = "VARCHAR"
                    measure_value = str(value)

                records.append(
                    {
                        "MeasureName": field,
                        "MeasureValue": measure_value,
                        "MeasureValueType": measure_type,
                    }
                )

        if not records:
            raise ValueError("No measure values found in the instance to write.")

        try:
            self.write_client.write_records(
                DatabaseName=self.database_name,
                TableName=self.table_name,
                Records=records,
                CommonAttributes=common_attributes,
            )
            logger.debug(f"Successfully wrote {len(records)} measures to Timestream.")
            return instance
        except self.write_client.exceptions.RejectedRecordsException as e:
            logger.error(f"Rejected records: {e.rejected_records}")
            raise ConnectorError(f"Failed to write to Timestream: {e}") from e
        except Exception as e:
            raise ConnectorError(
                f"An unexpected error occurred while writing to Timestream: {e}"
            ) from e

    def get_by_id(self, item_id: Any) -> BaseModel | None:
        """Get a single record by its primary key."""
        primary_key_field = self.config.get("primary_key")
        if not primary_key_field:
            raise ConfigurationError("Primary key not configured for this model.")

        if primary_key_field not in self.config.get("tags", []):
            raise ConfigurationError(
                f"The primary key '{primary_key_field}' must be a dimension (tag) for get_by_id to work with Timestream."
            )

        results = self.list(**{primary_key_field: item_id})
        return results[0] if results else None

    def list(self, **kwargs) -> list[BaseModel]:
        """List records from Timestream based on query filters."""
        if not self.query_client:
            raise ConnectionError("Not connected to AWS Timestream.")

        time_column = self.config.get("time_column", "time")
        tags = self.config.get("tags", [])
        all_fields = list(self.model.__fields__.keys())

        # Base query selects all dimensions and the measure name/value
        query = f'SELECT {time_column}, {", ".join(tags)}, measure_name, measure_value FROM "{self.database_name}"."{self.table_name}"'

        # Build WHERE clause from kwargs
        where_clauses = []
        for key, value in kwargs.items():
            if key not in all_fields:
                logger.warning(f"Ignoring unknown filter key: {key}")
                continue
            # Timestream requires single quotes for string literals in SQL
            if isinstance(value, str):
                value = f"'{value}'"
            where_clauses.append(f'"{key}" = {value}')

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        query += f" ORDER BY {time_column} DESC"

        try:
            paginator = self.query_client.get_paginator("query")
            page_iterator = paginator.paginate(QueryString=query)

            all_results = []
            for page in page_iterator:
                parsed_page = self._parse_query_result(page)
                all_results.extend(parsed_page)

            # The results are pivoted, so now we create the models
            return [self.model(**data) for data in all_results]

        except Exception as e:
            logger.error(f"Failed to query Timestream: {e}")
            raise ConnectorError(f"Failed to list items from Timestream: {e}") from e

    def _parse_query_result(self, response: dict[str, Any]) -> list[dict[str, Any]]:
        """Parse a single page of a Timestream query result and pivot the data."""
        column_info = response["ColumnInfo"]
        columns = [col["Name"] for col in column_info]

        pivoted_data: dict[tuple, dict[str, Any]] = {}

        for row in response["Rows"]:
            row_data = {
                columns[i]: datum.get("ScalarValue")
                for i, datum in enumerate(row["Data"])
            }

            # Create a unique key for the record based on time and dimensions
            time_val = row_data[self.config.get("time_column", "time")]
            dimension_keys = self.config.get("tags", [])
            record_key_values = tuple(row_data[dim] for dim in dimension_keys)
            record_key = (time_val,) + record_key_values

            if record_key not in pivoted_data:
                # Initialize the record with time and dimension values
                pivoted_data[record_key] = {
                    self.config.get("time_column", "time"): time_val
                }
                for i, dim in enumerate(dimension_keys):
                    pivoted_data[record_key][dim] = record_key_values[i]

            # Add the measure to the record
            measure_name = row_data["measure_name"]
            measure_value_str = row_data["measure_value"]

            # Cast the measure value to its proper type
            field = self.model.__fields__.get(measure_name)
            if field:
                try:
                    casted_value = field.type_(measure_value_str)
                    pivoted_data[record_key][measure_name] = casted_value
                except (TypeError, ValueError) as e:
                    logger.warning(
                        f"Could not cast '{measure_value_str}' to type {field.type_} for field '{measure_name}': {e}"
                    )
                    pivoted_data[record_key][measure_name] = (
                        measure_value_str  # Keep as string if cast fails
                    )
            else:
                pivoted_data[record_key][measure_name] = measure_value_str

        return list(pivoted_data.values())

    def update(self, item_id: Any, data: dict[str, Any]) -> BaseModel | None:
        raise NotImplementedError("AWS Timestream does not support updates directly.")

    def delete(self, item_id: Any, hard_delete: bool = False) -> None:
        raise NotImplementedError("AWS Timestream does not support deletes directly.")

    def count(self, **kwargs) -> int:
        """Count records in Timestream based on query filters."""
        if not self.query_client:
            raise ConnectionError("Not connected to AWS Timestream.")

        time_column = self.config.get("time_column", "time")

        # We count distinct timestamps to get the number of unique "items"
        query = f'SELECT COUNT(DISTINCT {time_column}) FROM "{self.database_name}"."{self.table_name}"'

        where_clauses = []
        for key, value in kwargs.items():
            if isinstance(value, str):
                value = f"'{value}'"
            where_clauses.append(f'"{key}" = {value}')

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        try:
            response = self.query_client.query(QueryString=query)
            # The result of a COUNT query is a single row with a single scalar value
            count = int(response["Rows"][0]["Data"][0]["ScalarValue"])
            return count
        except Exception as e:
            logger.error(f"Failed to count items in Timestream: {e}")
            raise ConnectorError(f"Failed to count items in Timestream: {e}") from e

    def get_last_k_items(
        self, k: int, time_column: str | None = None
    ) -> list[BaseModel]:
        """Get the last k items, ordered by time."""
        if not self.query_client:
            raise ConnectionError("Not connected to AWS Timestream.")

        time_col = time_column or self.config.get("time_column", "time")
        tags = self.config.get("tags", [])

        # This query is complex because we need to get the last K distinct time points
        # and then fetch all measures for those time points.
        sub_query = f'SELECT DISTINCT {time_col} FROM "{self.database_name}"."{self.table_name}" ORDER BY {time_col} DESC LIMIT {k}'
        query = f'SELECT {time_col}, {", ".join(tags)}, measure_name, measure_value FROM "{self.database_name}"."{self.table_name}" WHERE {time_col} IN ({sub_query}) ORDER BY {time_col} DESC'

        try:
            paginator = self.query_client.get_paginator("query")
            page_iterator = paginator.paginate(QueryString=query)

            all_results = []
            for page in page_iterator:
                parsed_page = self._parse_query_result(page)
                all_results.extend(parsed_page)

            return [self.model(**data) for data in all_results]

        except Exception as e:
            logger.error(f"Failed to get last {k} items from Timestream: {e}")
            raise ConnectorError(
                f"Failed to get last {k} items from Timestream: {e}"
            ) from e
