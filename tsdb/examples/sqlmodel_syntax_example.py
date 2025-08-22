"""
SQLModel-like Syntax Example

This file demonstrates the new SQLModel-like syntax for the TimescaleDB CRUD decorator.
Shows both the new instance-based methods and session-based operations.
"""

from datetime import datetime
from pydantic import BaseModel, Field
from sqlalchemy import create_engine

from tsdb.crud.session import create_crud_session
from tsdb.decorators.pydantic_decorator import db_crud


# Example model with the new SQLModel-like functionality
@db_crud(
    db_type="timescaledb",
    table_name="users_sqlmodel",
    primary_key="id",
    time_column="created_at",
    enable_soft_delete=True,
    enable_audit=True,
)
class User(BaseModel):
    id: int | None = None
    username: str
    email: str
    is_active: bool = True
    created_at: datetime | None = None
    updated_at: datetime | None = None


@db_crud(
    db_type="timescaledb",
    table_name="sensor_data_sqlmodel",
    time_column="timestamp",
    hypertable_config={
        "time_column_name": "timestamp",
        "chunk_time_interval": "1 hour",
    },
    enable_audit=False,
)
class SensorData(BaseModel):
    id: int | None = None
    sensor_id: str
    temperature: float
    humidity: float
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    location: str | None = None


def demonstrate_sqlmodel_syntax():
    """Demonstrate the new SQLModel-like syntax"""

    # Setup database connection
    TSDB_DATABASE_URI = "postgresql://tsdb_user:tsdb_password@localhost:5432/tsdb"
    engine = create_engine(TSDB_DATABASE_URI, echo=True)

    # Initialize database tables
    User.init_db(engine)
    SensorData.init_db(engine)

    # Create a CRUD session (SQLModel-like)
    with create_crud_session(TSDB_DATABASE_URI) as session:
        print("=== SQLModel-like Syntax Demo ===\n")

        # 1. Create instances and save them (SQLModel-like)
        print("1. Creating and saving instances:")

        user = User(username="alice_sqlmodel", email="alice@sqlmodel.com")
        print(f"Created user instance: {user}")

        # Save using instance method
        saved_user = user.save(session)
        print(f"Saved user: {saved_user}")
        print(f"User now has ID: {saved_user.id}\n")

        # Alternative: Save using session method
        user2 = User(username="bob_sqlmodel", email="bob@sqlmodel.com")
        saved_user2 = session.save(user2)
        print(f"Saved user via session.save(): {saved_user2}\n")

        # 2. Create sensor data
        print("2. Creating sensor data:")

        sensor = SensorData(
            sensor_id="TEMP_SQLMODEL_001",
            temperature=22.5,
            humidity=45.0,
            location="Lab",
        )
        saved_sensor = sensor.save(session)
        print(f"Saved sensor data: {saved_sensor}\n")

        # 3. Update instances (SQLModel-like)
        print("3. Updating instances:")

        # Modify the instance
        saved_user.email = "alice_updated@sqlmodel.com"
        saved_user.is_active = False

        # Save the changes
        updated_user = saved_user.save(session)
        print(f"Updated user: {updated_user}\n")

        # 4. Refresh instances from database
        print("4. Refreshing instances:")

        # Simulate external changes by updating via class method
        User.set_session(session.session)
        User.update(updated_user.id, {"username": "alice_external_update"})

        # Refresh the instance to get latest data
        refreshed_user = updated_user.refresh(session)
        print(f"Refreshed user: {refreshed_user}\n")

        # 5. Query using session methods
        print("5. Querying using session:")

        # Get by ID
        fetched_user = session.get_by_id(User, saved_user2.id)
        print(f"Fetched user by ID: {fetched_user}")

        # List all users
        all_users = session.list(User, limit=10)
        print(f"All users: {len(all_users)} found")

        # Count users
        user_count = session.count(User)
        print(f"Total users: {user_count}\n")

        # 6. Delete instances
        print("6. Deleting instances:")

        # Delete using instance method (soft delete)
        deleted = saved_sensor.delete(session)
        print(f"Soft deleted sensor: {deleted}")

        # Delete using session method (hard delete)
        deleted_hard = session.delete(saved_user2, hard_delete=True)
        print(f"Hard deleted user: {deleted_hard}")

        # Verify deletion
        remaining_users = session.count(User)
        print(f"Remaining users after deletion: {remaining_users}\n")

        print("=== Comparison: Old vs New Syntax ===\n")

        print("OLD SYNTAX (still supported):")
        print("  User.set_session(session)")
        print("  user = User.create(User(username='test', email='test@example.com'))")
        print("  updated = User.update(user.id, {'email': 'new@example.com'})")
        print("  deleted = User.delete(user.id)")
        print()

        print("NEW SYNTAX (SQLModel-like):")
        print("  user = User(username='test', email='test@example.com')")
        print("  saved_user = user.save(session)  # or session.save(user)")
        print("  saved_user.email = 'new@example.com'")
        print("  updated_user = saved_user.save(session)")
        print("  deleted = saved_user.delete(session)")
        print()

        print("=== Demo completed successfully! ===")


def demonstrate_context_manager():
    """Demonstrate using the session as a context manager"""

    TSDB_DATABASE_URI = "postgresql://tsdb_user:tsdb_password@localhost:5432/tsdb"

    print("\n=== Context Manager Demo ===")

    # Using session as context manager (auto-commit/rollback)
    with create_crud_session(TSDB_DATABASE_URI) as session:
        user = User(username="context_user", email="context@example.com")

        saved_user = session.save(user)
        print(f"Saved user in context: {saved_user}")

        # Any exception here would trigger rollback
        # Normal completion triggers commit

    print("Context manager demo completed!")


if __name__ == "__main__":
    try:
        demonstrate_sqlmodel_syntax()
        demonstrate_context_manager()
    except Exception as e:
        print(f"Error during demo: {e}")
        import traceback

        traceback.print_exc()
