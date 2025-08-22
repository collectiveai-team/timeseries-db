# TimescaleDB CRUD Decorator

A powerful decorator for Pydantic models that provides CRUD operations with SQLAlchemy and TimescaleDB integration.

## Features

- **Automatic CRUD Operations**: Create, Read, Update, Delete operations for Pydantic models
- **TimescaleDB Integration**: Automatic hypertable creation and time-series optimization
- **Soft Delete Support**: Optional soft delete functionality with timestamp tracking
- **Audit Trails**: Automatic created_at and updated_at timestamp management
- **Advanced Filtering**: Support for complex queries with operators (gt, lt, in, like, etc.)
- **Pagination**: Built-in limit/offset pagination support
- **Type Safety**: Full type hints with Pydantic v2 compatibility
- **Flexible Configuration**: Customizable table names, primary keys, and column mappings

## Installation

### Basic Installation

```bash
uv add tsdb
```

### Optional Dependencies

For TimeSeries forecasting functionality with Darts integration:

```bash
uv add tsdb[forecast]
```

This installs the optional `darts>=0.36.0` dependency required for TimeSeries storage and forecasting operations.

### Development Installation

```bash
uv add tsdb[dev]
```

The package uses the following core dependencies:

```toml
dependencies = [
    "pydantic==2.10.6",
    "sqlalchemy-timescaledb>=0.4.1",
    # ... other dependencies
]
```

## Quick Start

### 1. Define Your Pydantic Model with the Decorator

```python
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field
from tsdb.decorators.pydantic_decorator import timescale_crud

@timescale_crud(
    table_name="sensor_readings",
    time_column="timestamp",
    create_hypertable=True,
    chunk_time_interval="1 hour"
)
class SensorReading(BaseModel):
    id: Optional[int] = None
    sensor_id: str
    temperature: float
    humidity: float
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    location: Optional[str] = None
```

### 2. Initialize Database and Session

```python
from sqlalchemy import create_engine
from tsdb.decorators.pydantic_decorator import create_session

# Create database connection
TSDB_DATABASE_URI = "postgresql://user:password@localhost:5432/timeseries_db"
engine = create_engine(TSDB_DATABASE_URI)
session = create_session(TSDB_DATABASE_URI)

# Initialize tables and hypertables
SensorReading.init_db(engine)
SensorReading.set_session(session)
```

### 3. Use CRUD Operations

```python
# Create records
reading = SensorReading.create({
    "sensor_id": "TEMP_001",
    "temperature": 23.5,
    "humidity": 45.2,
    "location": "Office"
})

# Read records
all_readings = SensorReading.list(limit=100)
reading_by_id = SensorReading.get_by_id(1)

# Update records
updated = SensorReading.update(1, {"temperature": 24.0})

# Delete records
deleted = SensorReading.delete(1)  # Soft delete if enabled
hard_deleted = SensorReading.delete(1, hard_delete=True)

# Count records
total = SensorReading.count()
```

## Advanced Usage

### Complex Filtering

```python
# Filter with operators
hot_readings = SensorReading.list(
    filters={
        "temperature": {"gt": 25.0},
        "humidity": {"gte": 40.0, "lt": 60.0},
        "sensor_id": {"in": ["TEMP_001", "TEMP_002"]},
        "location": {"like": "Office"}
    },
    order_by="timestamp",
    order_desc=True,
    limit=50,
    offset=0
)
```

### Soft Delete Configuration

```python
@timescale_crud(
    table_name="users",
    enable_soft_delete=True,
    soft_delete_column="deleted_at",
    enable_audit=True,
    audit_columns={
        "created_at": "created_at",
        "updated_at": "modified_at"
    }
)
class User(BaseModel):
    id: Optional[int] = None
    username: str
    email: str
    created_at: Optional[datetime] = None
    modified_at: Optional[datetime] = None
```

### Custom Primary Key and Time Column

```python
@timescale_crud(
    table_name="events",
    primary_key="event_id",
    time_column="event_time",
    create_hypertable=True,
    chunk_time_interval="1 day"
)
class Event(BaseModel):
    event_id: Optional[int] = None
    event_type: str
    event_data: str
    event_time: datetime
```

## Decorator Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `table_name` | `str` | Required | Database table name |
| `primary_key` | `str` | `"id"` | Primary key column name |
| `time_column` | `str` | `"created_at"` | Time column for hypertable partitioning |
| `enable_soft_delete` | `bool` | `False` | Enable soft delete functionality |
| `soft_delete_column` | `str` | `"deleted_at"` | Column name for soft delete timestamp |
| `enable_audit` | `bool` | `True` | Enable audit fields (created_at, updated_at) |
| `audit_columns` | `Dict[str, str]` | `{"created_at": "created_at", "updated_at": "updated_at"}` | Custom audit column mapping |
| `create_hypertable` | `bool` | `True` | Create TimescaleDB hypertable |
| `chunk_time_interval` | `str` | `"1 day"` | Time interval for hypertable chunks |

## Available CRUD Methods

### Create
```python
# From dictionary
record = Model.create({"field1": "value1", "field2": "value2"})

# From Pydantic instance
record = Model.create(Model(field1="value1", field2="value2"))
```

### Read
```python
# Get by ID
record = Model.get_by_id(1)

# List with filtering and pagination
records = Model.list(
    limit=100,
    offset=0,
    filters={"field": "value"},
    order_by="created_at",
    order_desc=True
)

# Count records
total = Model.count(filters={"field": "value"})
```

### Update
```python
# Update by ID
updated = Model.update(1, {"field": "new_value"})

# Returns None if record not found
```

### Delete
```python
# Soft delete (if enabled)
success = Model.delete(1)

# Hard delete
success = Model.delete(1, hard_delete=True)
```

## Filter Operators

The `filters` parameter supports various operators:

- `{"field": "value"}` - Equality
- `{"field": {"gt": 10}}` - Greater than
- `{"field": {"gte": 10}}` - Greater than or equal
- `{"field": {"lt": 10}}` - Less than
- `{"field": {"lte": 10}}` - Less than or equal
- `{"field": {"in": [1, 2, 3]}}` - In list
- `{"field": {"like": "pattern"}}` - SQL LIKE pattern matching

## Error Handling

The decorator raises `CRUDError` exceptions for database-related errors:

```python
from tsdb.crud import CRUDError

try:
    record = Model.create(invalid_data)
except CRUDError as e:
    print(f"Database error: {e}")
```

## Type Mapping

Python types are automatically mapped to SQLAlchemy types:

| Python Type | SQLAlchemy Type |
|-------------|-----------------|
| `int` | `Integer` |
| `str` | `String(255)` |
| `float` | `Float` |
| `bool` | `Boolean` |
| `datetime` | `DateTime` |
| Others | `Text` |

## Best Practices

1. **Session Management**: Always set the session before performing operations
2. **Error Handling**: Wrap operations in try-catch blocks
3. **Connection Pooling**: Use connection pooling for production applications
4. **Indexing**: Add appropriate indexes for frequently queried columns
5. **Chunk Intervals**: Choose appropriate chunk intervals based on your data ingestion rate

## Example: Complete Application

See `tsdb/core/example_usage.py` for a comprehensive example showing:
- Multiple model types (sensor data, users, stock prices)
- Different configurations (hypertables, soft delete, custom columns)
- All CRUD operations with various filtering options
- Error handling and session management

## Contributing

This decorator is part of the TimescaleDB project. Follow the project's coding standards:
- Use type hints consistently
- Follow PEP8 style guide
- Write modular, readable code
- Add comprehensive tests for new features
