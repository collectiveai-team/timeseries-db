"""
Example usage of the DuckDB CRUD decorator.

This file demonstrates how to use the @db_crud decorator
with Pydantic models backed by the DuckDB connector.

Notes/limitations in this simple DuckDB connector:
- No automatic primary key generation. Set IDs explicitly when creating records.
- Filters support equality only (no gt/gte/like). Use simple exact matches.
- count() is not demonstrated to avoid API mismatch; use len(Model.list(...)).
"""

from datetime import datetime
import traceback
from pydantic import BaseModel, Field

from tsdb.decorators.pydantic_decorator import db_crud


# Example 1: Simple time series data model (DuckDB)
@db_crud(
    db_type="duckdb",
    table_name="sensor_readings_duckdb",
    time_column="timestamp",
    enable_audit=False,  # Disable audit since we use timestamp field
    db_path=":memory:",  # In-memory DuckDB for example purposes
)
class SensorReading(BaseModel):
    id: int
    sensor_id: str
    temperature: float
    humidity: float
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    location: str | None = None


# Example 2: User model (DuckDB)
@db_crud(
    db_type="duckdb",
    table_name="users_duckdb",
    primary_key="user_id",
    time_column="created_at",
    enable_audit=True,
    db_path=":memory:",
)
class User(BaseModel):
    user_id: int
    username: str
    email: str
    is_active: bool = True
    created_at: datetime | None = None
    modified_at: datetime | None = None


# Example 3: Stock price data (DuckDB)
@db_crud(
    db_type="duckdb",
    table_name="stock_prices_duckdb",
    time_column="trade_time",
    enable_audit=False,  # No audit columns needed
    db_path=":memory:",
)
class StockPrice(BaseModel):
    id: int
    symbol: str
    price: float
    volume: int
    trade_time: datetime
    exchange: str


def example_usage() -> None:
    """Demonstrate how to use the decorated models with DuckDB"""

    try:
        # Example 1: Create sensor readings (IDs set explicitly)
        print("=== Creating Sensor Readings (DuckDB) ===")

        reading1 = SensorReading.create(
            SensorReading(
                id=1,
                sensor_id="TEMP_001",
                temperature=23.5,
                humidity=45.2,
                location="Office",
            )
        )
        print(f"Created reading: {reading1}")

        reading2 = SensorReading.create(
            SensorReading(
                id=2,
                sensor_id="TEMP_002",
                temperature=25.1,
                humidity=50.8,
                location="Warehouse",
            )
        )
        print(f"Created reading: {reading2}")

        # Example 2: List readings (no advanced operators; equality filters only)
        print("\n=== Listing Sensor Readings ===")

        # Get all readings
        all_readings = SensorReading.list(limit=10)
        print(f"Total readings: {len(all_readings)}")

        # Filter by sensor ID (equality)
        sensor_readings = SensorReading.list(filters={"sensor_id": "TEMP_001"})
        print(f"TEMP_001 readings: {len(sensor_readings)}")

        # Example 3: Update a reading (using class method)
        print("\n=== Updating Sensor Reading ===")

        updated_reading = SensorReading.update(
            1, {"temperature": 24.0, "humidity": 48.0}
        )
        print(f"Updated reading: {updated_reading}")

        # Example 4: User operations
        print("\n=== User Operations ===")

        # Create users with explicit IDs
        user1 = User.create(
            User(user_id=1, username="alice", email="alice@example.com")
        )
        print(f"Created user: {user1}")

        user2 = User.create(User(user_id=2, username="bob", email="bob@example.com"))
        print(f"Created user: {user2}")

        # List users
        users = User.list()
        print(f"Users: {len(users)}")

        # Delete a user (DuckDB connector uses hard delete)
        deleted_success = User.delete(1)
        print(f"Deleted user 1: {deleted_success}")

        # List users again (should be one less)
        users_after_delete = User.list()
        print(f"Users after delete: {len(users_after_delete)}")

        # Example 5: Stock price operations
        print("\n=== Stock Price Operations ===")

        # Create stock prices
        prices = [
            StockPrice(
                id=1,
                symbol="AAPL",
                price=150.25,
                volume=1_000_000,
                trade_time=datetime.utcnow(),
                exchange="NASDAQ",
            ),
            StockPrice(
                id=2,
                symbol="GOOGL",
                price=2800.50,
                volume=500_000,
                trade_time=datetime.utcnow(),
                exchange="NASDAQ",
            ),
            StockPrice(
                id=3,
                symbol="TSLA",
                price=800.75,
                volume=2_000_000,
                trade_time=datetime.utcnow(),
                exchange="NASDAQ",
            ),
        ]

        for price in prices:
            created = StockPrice.create(price)
            print(f"Created stock price: {created.symbol} @ ${created.price}")

        # Filter by symbol (equality)
        aapl_prices = StockPrice.list(filters={"symbol": "AAPL"})
        print(f"AAPL prices: {len(aapl_prices)}")

        # Example 6: Get by ID
        print("\n=== Get by ID Operations ===")

        fetched_reading = SensorReading.get_by_id(1)
        print(f"Fetched reading by ID: {fetched_reading}")

        fetched_user = User.get_by_id(2)
        print(f"Fetched user by ID: {fetched_user}")

        print("\n=== DuckDB Example completed successfully! ===")

    except Exception as e:  # pragma: no cover - demonstration script
        print(f"Error during example execution: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    example_usage()
