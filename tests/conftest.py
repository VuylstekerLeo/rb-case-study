"""
Test configuration module for SparkSession fixture setup.
"""
# conftest.py

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark():
    """
    Fixture to set up and provide a SparkSession for testing purposes.

    Yields:
        SparkSession: A SparkSession instance.
    """
    spark_session = (
        SparkSession
        .builder
        .enableHiveSupport()
        .appName("Test code")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()
