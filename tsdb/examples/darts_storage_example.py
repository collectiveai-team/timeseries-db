"""
Example usage of the Darts TimeSeries storage decorator.

This example demonstrates how to use the timeseries_storage decorator
to add TimescaleDB storage capabilities to a class that works with Darts TimeSeries.
"""

try:
    from darts import TimeSeries
    from darts.datasets import AirPassengersDataset

    DARTS_AVAILABLE = True
except ImportError:
    TimeSeries = None
    AirPassengersDataset = None
    DARTS_AVAILABLE = False
from sqlalchemy import create_engine

from tsdb.decorators.darts_decorator import timeseries_storage, create_session


@timeseries_storage(
    table_name="timeseries_data",
    primary_key="id",
    time_column="created_at",
    name_column="series_name",
    metadata_columns={
        "category": str,
        "source": str,
        "version": int,
    },
    enable_audit=True,
    create_hypertable=True,
    chunk_time_interval="1 day",
)
class TimeSeriesManager:
    """A class for managing TimeSeries objects with database storage."""

    def __init__(self, database_url: str):
        """Initialize the manager with a database connection."""
        self.database_url = database_url
        self.engine = create_engine(database_url, echo=False)

        # Initialize the database tables
        self.init_db(self.engine)

        # Create and set session
        session = create_session(database_url)
        self.set_session(session)

    def store_series_with_metadata(
        self,
        series: TimeSeries,
        name: str,
        category: str | None = None,
        source: str | None = None,
        version: int | None = None,
    ) -> int:
        """Store a TimeSeries with additional metadata."""
        metadata = {}
        if category:
            metadata["category"] = category
        if source:
            metadata["source"] = source
        if version:
            metadata["version"] = version

        return self.save_timeseries(series, name, metadata)

    def get_series_by_category(self, category: str) -> list:
        """Get all series metadata for a specific category."""
        all_series = self.list_timeseries()
        return [s for s in all_series if s.get("category") == category]


def main():
    """Example usage of the TimeSeriesManager."""
    if not DARTS_AVAILABLE:
        print("ERROR: darts package is required to run this example.")
        print("Install with: uv add tsdb[forecast]")
        return

    # Database connection string (adjust as needed)
    database_url = "postgresql://tsdb_user:tsdb_password@localhost:5432/tsdb"

    try:
        # Create manager
        manager = TimeSeriesManager(database_url)

        # Load sample data
        air_passengers = AirPassengersDataset().load()

        # Store the series with metadata
        series_id = manager.store_series_with_metadata(
            series=air_passengers,
            name="air_passengers_monthly",
            category="transportation",
            source="classic_dataset",
            version=1,
        )
        print(f"Stored TimeSeries with ID: {series_id}")

        # List all stored series
        all_series = manager.list_timeseries()
        print(f"Found {len(all_series)} stored series:")
        for series_info in all_series:
            print(f"  - {series_info['name']}: {series_info['n_timesteps']} timesteps")

        # Load the series back
        loaded_series = manager.load_timeseries("air_passengers_monthly")
        if loaded_series:
            print(f"Loaded series: {len(loaded_series)} timesteps")
            print(f"Start: {loaded_series.start_time()}")
            print(f"End: {loaded_series.end_time()}")

        # Get series by category
        transport_series = manager.get_series_by_category("transportation")
        print(f"Transportation series: {len(transport_series)}")

        # Clean up - delete the series
        deleted = manager.delete_timeseries("air_passengers_monthly")
        print(f"Series deleted: {deleted}")

    except Exception as e:
        print(f"Error: {e}")
        print("Make sure TimescaleDB is running and the database exists.")


if __name__ == "__main__":
    main()
