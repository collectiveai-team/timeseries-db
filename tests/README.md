# Testing Guide for TimescaleDB CRUD System

This document explains how to run tests for the TimescaleDB CRUD system, including both unit tests with mocks and integration tests with a real TimescaleDB instance.

## Quick Start

### 1. Start TimescaleDB with Docker Compose

```bash
# Start TimescaleDB
docker compose up -d timescaledb

# Check if it's running
docker compose ps
```

### 2. Install Test Dependencies

```bash
# Install development dependencies including test packages
uv sync --group dev
```

### 3. Run Tests

```bash
# Run all tests
python tests/test_runner.py all

# Run only unit tests (no database required)
python tests/test_runner.py unit

# Run only integration tests (requires TimescaleDB)
python tests/test_runner.py integration

# Run with coverage
python tests/test_runner.py all --coverage

# Start database automatically and run integration tests
python tests/test_runner.py integration --start-db
```

## Test Structure

```
tests/
├── __init__.py
├── conftest.py                 # Test fixtures and configuration
├── test_runner.py             # Test runner script
├── unit/                      # Unit tests with mocks
│   ├── __init__.py
│   └── test_pydantic_decorator.py
├── integration/               # Integration tests with real DB
│   ├── __init__.py
│   └── test_timescaledb_integration.py
└── mocks/                     # Mock objects and API responses
    ├── __init__.py
    └── api_mocks.py
```

## Test Types

### Unit Tests

Unit tests use SQLAlchemy mocks and don't require a database connection. They test:

- CRUD decorator functionality
- Pydantic model enhancement
- Error handling
- Configuration management
- Session management

**Run unit tests:**
```bash
pytest tests/unit/ -v
```

### Integration Tests

Integration tests use a real TimescaleDB instance and test:

- Full database integration
- TimescaleDB hypertable creation
- Time-series data operations
- Chunk creation and management
- Real database transactions

**Run integration tests:**
```bash
# Make sure TimescaleDB is running first
docker compose up -d timescaledb

# Set test database URL
export TEST_DATABASE_URL="postgresql://test_user:test_password@localhost:5432/tsdb"

# Run tests
pytest tests/integration/ -v
```

## Docker Compose Setup

### Services

- **timescaledb**: TimescaleDB instance for testing and development
- **pgadmin**: Web-based PostgreSQL administration (optional)

### Configuration

The Docker Compose setup includes:

- TimescaleDB with PostgreSQL 16
- Automatic TimescaleDB extension installation
- Test user creation with appropriate permissions
- Health checks for service readiness
- Persistent data volumes

### Environment Variables

Copy `.env.example` to `.env` and customize if needed:

```bash
cp .env.example .env
```

Default configuration:
- Database: `tsdb`
- User: `tsdb_user`
- Password: `tsdb_password`
- Test User: `test_user`
- Test Password: `test_password`
- Port: `5432`

### Database Access

**Main database:**
```
postgresql://tsdb_user:tsdb_password@localhost:5432/tsdb
```

**Test database (same DB, different user):**
```
postgresql://test_user:test_password@localhost:5432/tsdb
```

**pgAdmin (optional):**
- URL: http://localhost:8080
- Email: admin@tsdb.local
- Password: admin

## Mock System

The test suite includes comprehensive mocks for SQLAlchemy operations:

### MockSQLAlchemySession

Simulates SQLAlchemy session behavior:
- Transaction management (commit, rollback)
- Object persistence (add, refresh)
- Query execution
- Record retrieval

### MockDatabaseResponses

Provides realistic database response data:
- Successful CRUD operations
- Error responses
- Time-series data samples

### Example Usage

```python
from tests.mocks.api_mocks import MockSQLAlchemySession

def test_create_operation():
    mock_session = MockSQLAlchemySession()
    # Use mock_session in your tests
    # It tracks all operations for verification
```

## Test Configuration

### pytest Configuration

The `pyproject.toml` includes pytest configuration:

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
filterwarnings = [
    "error",
    "ignore::UserWarning",
    'ignore:.*:DeprecationWarning',
]
```

### Test Fixtures

Common fixtures in `conftest.py`:

- `in_memory_engine`: SQLite in-memory database for unit tests
- `db_session`: Database session with automatic cleanup
- `mock_session`: Mock SQLAlchemy session
- `sample_timeseries_model`: Example TimescaleDB model
- `sample_data`: Test data for CRUD operations

## Running Tests in CI/CD

### GitHub Actions Example

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    services:
      timescaledb:
        image: timescale/timescaledb:latest-pg16
        env:
          POSTGRES_DB: tsdb
          POSTGRES_USER: test_user
          POSTGRES_PASSWORD: test_password
        ports:
          - 5432:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5

    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.13'
    
    - name: Install dependencies
      run: |
        pip install uv
        uv sync --group dev
    
    - name: Run unit tests
      run: python tests/test_runner.py unit --coverage
    
    - name: Run integration tests
      env:
        TEST_DATABASE_URL: postgresql://test_user:test_password@localhost:5432/tsdb
      run: python tests/test_runner.py integration
```

## Troubleshooting

### Common Issues

1. **TimescaleDB not starting:**
   ```bash
   # Check Docker logs
   docker compose logs timescaledb
   
   # Restart services
   docker compose down
   docker compose up -d
   ```

2. **Connection refused errors:**
   ```bash
   # Wait for database to be ready
   docker compose exec timescaledb pg_isready -U tsdb_user
   ```

3. **Permission errors:**
   ```bash
   # Check database permissions
   docker compose exec timescaledb psql -U tsdb_user -d tsdb -c "\du"
   ```

4. **Test failures in integration tests:**
   - Ensure TimescaleDB is running
   - Check database URL is correct
   - Verify TimescaleDB extension is installed

### Debugging Tests

```bash
# Run with verbose output
pytest tests/ -v -s

# Run specific test
pytest tests/unit/test_pydantic_decorator.py::TestCRUDOperations::test_create_operation_success -v

# Run with debugger
pytest tests/ --pdb

# Run with coverage and HTML report
pytest tests/ --cov=tsdb --cov-report=html
```

## Performance Testing

For performance testing with large datasets:

```python
import pytest
from datetime import datetime, timedelta

@pytest.mark.performance
def test_bulk_insert_performance(sensor_model):
    """Test performance with large dataset."""
    start_time = datetime.now()
    
    # Create 1000 records
    for i in range(1000):
        data = {
            "sensor_id": f"perf_sensor_{i:04d}",
            "temperature": 20.0 + (i % 10),
            "humidity": 60.0 + (i % 20),
            "timestamp": start_time + timedelta(seconds=i)
        }
        sensor_model.create(data)
    
    # Measure query performance
    query_start = datetime.now()
    results = sensor_model.list(limit=100)
    query_time = (datetime.now() - query_start).total_seconds()
    
    assert len(results) <= 100
    assert query_time < 1.0  # Should complete in under 1 second
```

Run performance tests:
```bash
pytest tests/ -m performance -v
```
