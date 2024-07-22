"""
Test functions for module src/workloads/club_elo.py
"""
import pathlib

from pyspark.sql import SparkSession

from src.workloads.club_elo import etl


def test_etl(spark: SparkSession) -> None:
    """
    Test etl

    Args:
        spark (SparkSession): SparkSession instance

    Returns:
        None

    Raises:
        AssertionError: if the test fails.
    """

    root = pathlib.Path(__file__).parent.parent.parent.parent
    folder = f"{root}/data/raw/club_elo_api"

    spark_session = (
        SparkSession
        .builder
        .enableHiveSupport()
        .appName("Test code")
        .getOrCreate()
    )

    spark_session.sql("drop table if exists bronze.raw_club_elo")

    spark_session.sql("""
        create table if not exists bronze.raw_club_elo(
        Rank long,
        Club string,
        Country string,
        Level int,
        Elo double,
        From date,
        To date,
        file_modification_time timestamp
        )
        """)

    etl(spark, folder)

    assert spark.table("bronze.raw_club_elo").count() >= 1
