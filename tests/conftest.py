"""
Pytest fixtures for pipeline tests.
"""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    session = (
        SparkSession.builder
        .appName("pipeline_tests")
        .master("local[*]")
        .getOrCreate()
    )
    yield session
    session.stop()
