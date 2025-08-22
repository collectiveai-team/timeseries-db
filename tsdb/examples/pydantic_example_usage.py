"""
Example usage of the TimescaleDB CRUD decorator.

This file demonstrates how to use the @timescale_crud decorator
with Pydantic models for TimescaleDB operations.
"""

from datetime import datetime
from pydantic import BaseModel, Field
from sqlalchemy import create_engine

from tsdb.crud.session import create_session
from tsdb.decorators.pydantic_decorator import db_crud


# Example 1: Simple time series data model
@db_crud(
    db_type="timescaledb",
    table_name="sensor_readings",
    time_column="timestamp",
    hypertable_config={
        "time_column_name": "timestamp",
        "chunk_time_interval": "1 hour",
    },
    enable_audit=False,  # Disable audit since we use timestamp field
)
class SensorReading(BaseModel):
    id: int | None = None
    sensor_id: str
    temperature: float
    humidity: float
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    location: str | None = None


# Example 2: User model with soft delete and custom audit columns
@db_crud(
    db_type="timescaledb",
    table_name="users",
    primary_key="user_id",
    time_column="created_at",
    enable_soft_delete=True,
    enable_audit=True,
    audit_columns={"created_at": "created_at", "updated_at": "modified_at"},
)
class User(BaseModel):
    user_id: int | None = None
    username: str
    email: str
    is_active: bool = True
    created_at: datetime | None = None
    modified_at: datetime | None = None


# Example 3: Stock price data with custom configuration
@db_crud(
    db_type="timescaledb",
    table_name="stock_prices",
    time_column="trade_time",
    hypertable_config={
        "time_column_name": "trade_time",
        "chunk_time_interval": "1 day",
    },
    enable_audit=False,  # No audit columns needed
)
class StockPrice(BaseModel):
    id: int | None = None
    symbol: str
    price: float
    volume: int
    trade_time: datetime
    exchange: str


def example_usage():
    """Demonstrate how to use the decorated models"""

    # Setup database connection
    DATABASE_URL = "postgresql://tsdb_user:tsdb_password@localhost:5432/tsdb"
    engine = create_engine(DATABASE_URL, echo=True)
    session = create_session(DATABASE_URL)

    # Initialize database tables and hypertables
    SensorReading.init_db(engine)
    User.init_db(engine)
    StockPrice.init_db(engine)

    # Set session for all models
    SensorReading.set_session(session)
    User.set_session(session)
    StockPrice.set_session(session)

    try:
        # Example 1: Create sensor readings
        print("=== Creating Sensor Readings ===")

        reading1 = SensorReading.create(
            SensorReading(
                sensor_id="TEMP_001",
                temperature=23.5,
                humidity=45.2,
                location="Office",
            )
        )
        print(f"Created reading: {reading1}")

        reading2 = SensorReading.create(
            SensorReading(
                sensor_id="TEMP_002",
                temperature=25.1,
                humidity=50.8,
                location="Warehouse",
            )
        )
        print(f"Created reading: {reading2}")

        # Example 2: List readings with filtering
        print("\n=== Listing Sensor Readings ===")

        # Get all readings
        all_readings = SensorReading.list(limit=10)
        print(f"Total readings: {len(all_readings)}")

        # Filter by temperature range
        hot_readings = SensorReading.list(
            filters={"temperature": {"gt": 24.0}}, order_by="timestamp", order_desc=True
        )
        print(f"Hot readings (>24°C): {len(hot_readings)}")

        # Filter by sensor ID
        sensor_readings = SensorReading.list(filters={"sensor_id": "TEMP_001"})
        print(f"TEMP_001 readings: {len(sensor_readings)}")

        # Example 3: Update a reading
        print("\n=== Updating Sensor Reading ===")

        if reading1.id:
            updated_reading = SensorReading.update(
                reading1.id, {"temperature": 24.0, "humidity": 48.0}
            )
            print(f"Updated reading: {updated_reading}")

        # Example 4: User operations with soft delete
        print("\n=== User Operations ===")

        # Create users
        user1 = User.create({"username": "alice", "email": "alice@example.com"})
        print(f"Created user: {user1}")

        user2 = User.create({"username": "bob", "email": "bob@example.com"})
        print(f"Created user: {user2}")

        # List users
        users = User.list()
        print(f"Active users: {len(users)}")

        # Soft delete a user
        if user1.user_id:
            deleted = User.delete(user1.user_id)  # Soft delete
            print(f"Soft deleted user: {deleted}")

        # List users again (should be one less)
        users_after_delete = User.list()
        print(f"Active users after soft delete: {len(users_after_delete)}")

        # Example 5: Stock price operations
        print("\n=== Stock Price Operations ===")

        # Create stock prices
        prices = [
            {
                "symbol": "AAPL",
                "price": 150.25,
                "volume": 1000000,
                "trade_time": datetime.utcnow(),
                "exchange": "NASDAQ",
            },
            {
                "symbol": "GOOGL",
                "price": 2800.50,
                "volume": 500000,
                "trade_time": datetime.utcnow(),
                "exchange": "NASDAQ",
            },
            {
                "symbol": "TSLA",
                "price": 800.75,
                "volume": 2000000,
                "trade_time": datetime.utcnow(),
                "exchange": "NASDAQ",
            },
        ]

        created_prices = []
        for price_data in prices:
            price = StockPrice.create(price_data)
            created_prices.append(price)
            print(f"Created stock price: {price.symbol} @ ${price.price}")

        # Get count of stock prices
        total_prices = StockPrice.count()
        print(f"Total stock prices: {total_prices}")

        # Filter by symbol
        aapl_prices = StockPrice.list(filters={"symbol": "AAPL"})
        print(f"AAPL prices: {len(aapl_prices)}")

        # Filter by price range
        expensive_stocks = StockPrice.list(
            filters={"price": {"gt": 1000.0}}, order_by="price", order_desc=True
        )
        print(f"Expensive stocks (>$1000): {len(expensive_stocks)}")

        # Example 6: Get by ID
        print("\n=== Get by ID Operations ===")

        if reading1.id:
            fetched_reading = SensorReading.get_by_id(reading1.id)
            print(f"Fetched reading by ID: {fetched_reading}")

        if user2.user_id:
            fetched_user = User.get_by_id(user2.user_id)
            print(f"Fetched user by ID: {fetched_user}")

        # Example 7: Complex filtering
        print("\n=== Complex Filtering ===")

        # Multiple filters
        filtered_readings = SensorReading.list(
            filters={
                "temperature": {"gte": 20.0, "lt": 30.0},
                "humidity": {"gt": 40.0},
            },
            limit=5,
        )
        print(f"Filtered readings: {len(filtered_readings)}")

        # String pattern matching
        office_readings = SensorReading.list(filters={"location": {"like": "Office"}})
        print(f"Office readings: {len(office_readings)}")

        print("\n=== Example completed successfully! ===")

    except Exception as e:
        print(f"Error during example execution: {e}")
        session.rollback()
    finally:
        session.close()


if __name__ == "__main__":
    example_usage()
