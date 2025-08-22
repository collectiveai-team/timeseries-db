import datetime
import logging
import os
from uuid import UUID, uuid4

from pydantic import Field, BaseModel

from tsdb.decorators import db_crud

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# --- 1. Define a Pydantic Model ---
# This model will be used for all database backends.
class SensorReading(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    sensor_id: int
    location: str
    temperature: float
    humidity: float
    is_active: bool = True
    created_at: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)


# --- 2. DuckDB Example (In-Memory) ---
@db_crud(
    db_type="duckdb",
    table_name="sensor_readings_duckdb",
    primary_key="id",
    time_column="created_at",
    tags=["location", "sensor_id"],  # Dimensions for timeseries analysis
    db_path=":memory:",  # Use in-memory DuckDB
)
class SensorReadingDuckDB(SensorReading):
    pass


def run_duckdb_example():
    logger.info("--- Running DuckDB Example ---")
    SensorReadingDuckDB.connect()
    SensorReadingDuckDB.create_table()

    # Create
    reading1 = SensorReadingDuckDB(
        sensor_id=1, location="living_room", temperature=22.5, humidity=45.0
    )
    reading1.save()
    logger.info(f"Created reading: {reading1.id}")

    # Read
    retrieved = SensorReadingDuckDB.get_by_id(reading1.id)
    logger.info(f"Retrieved reading: {retrieved}")

    # List
    all_readings = SensorReadingDuckDB.list(location="living_room")
    logger.info(f"Found {len(all_readings)} readings in the living room.")

    # Count
    count = SensorReadingDuckDB.count(location="living_room")
    logger.info(f"Counted {count} readings.")

    # Get last k
    last_reading = SensorReadingDuckDB.get_last_k_items(k=1)
    logger.info(f"Last reading: {last_reading[0] if last_reading else 'None'}")

    # Delete
    reading1.delete()
    logger.info(f"Deleted reading: {reading1.id}")
    retrieved_after_delete = SensorReadingDuckDB.get_by_id(reading1.id)
    logger.info(f"Retrieved after delete: {retrieved_after_delete}")

    SensorReadingDuckDB.disconnect()


# --- 3. TimescaleDB Example (Requires a running TimescaleDB instance) ---
# Set the DATABASE_URL environment variable, e.g.:
# export DATABASE_URL="postgresql://user:password@localhost:5432/mydatabase"


@db_crud(
    db_type="timescaledb",
    table_name="sensor_readings_timescaledb",
    primary_key="id",
    time_column="created_at",
    db_url=os.getenv("DATABASE_URL"),
    hypertable_config={
        "time_column_name": "created_at",
        "chunk_time_interval": "1 day",
    },
)
class SensorReadingTimescale(SensorReading):
    pass


def run_timescaledb_example():
    if not os.getenv("DATABASE_URL"):
        logger.warning("--- Skipping TimescaleDB Example: DATABASE_URL not set. ---")
        return

    logger.info("--- Running TimescaleDB Example ---")
    SensorReadingTimescale.connect()
    SensorReadingTimescale.create_table()

    # Create
    reading1 = SensorReadingTimescale(
        sensor_id=2, location="kitchen", temperature=25.0, humidity=60.1
    )
    reading1.save()
    logger.info(f"Created reading: {reading1.id}")

    # Read
    retrieved = SensorReadingTimescale.get_by_id(reading1.id)
    logger.info(f"Retrieved reading: {retrieved}")

    # Delete
    reading1.delete()
    logger.info(f"Deleted reading: {reading1.id}")

    SensorReadingTimescale.disconnect()


# --- 4. AWS Timestream Example (Requires AWS credentials and a pre-configured table) ---
# Ensure your environment is configured with AWS credentials (e.g., via aws configure)
# and that you have created a database and table in Timestream.


@db_crud(
    db_type="timestream",
    table_name=os.getenv("TIMESTREAM_TABLE_NAME", "my-timestream-table"),
    primary_key="id",
    time_column="created_at",
    tags=["location", "sensor_id"],  # These must be dimensions in your Timestream table
    aws_region=os.getenv("AWS_REGION", "us-east-1"),
    database_name=os.getenv("TIMESTREAM_DATABASE_NAME", "my-timestream-db"),
)
class SensorReadingTimestream(SensorReading):
    pass


def run_timestream_example():
    if not all(
        [os.getenv("TIMESTREAM_TABLE_NAME"), os.getenv("TIMESTREAM_DATABASE_NAME")]
    ):
        logger.warning(
            "--- Skipping AWS Timestream Example: Environment variables not set. ---"
        )
        return

    logger.info("--- Running AWS Timestream Example ---")
    SensorReadingTimestream.connect()

    # Create
    reading1 = SensorReadingTimestream(
        sensor_id=3, location="garage", temperature=18.0, humidity=70.5
    )
    reading1.save()
    logger.info(f"Created reading in Timestream: {reading1.id}")

    # List (Timestream queries can have eventual consistency)
    import time

    time.sleep(5)  # Give time for the record to be indexed
    results = SensorReadingTimestream.list(sensor_id=3, location="garage")
    logger.info(f"Found {len(results)} readings in Timestream for sensor 3.")

    SensorReadingTimestream.disconnect()


if __name__ == "__main__":
    run_duckdb_example()
    print("\n" + "=" * 80 + "\n")
    run_timescaledb_example()
    print("\n" + "=" * 80 + "\n")
    run_timestream_example()
