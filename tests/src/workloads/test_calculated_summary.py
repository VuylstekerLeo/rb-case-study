"""
Test functions for module src/workloads/calculated_summary.py
"""
import pathlib
from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, DateType, TimestampType

import src.workloads.club_elo as club_elo
import src.workloads.calculated_summary as calculated_summary


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

    spark.sql("drop table if exists bronze.raw_club_elo")

    spark.sql("""
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

    club_elo.etl(spark, folder)

    spark.sql("""
        create table if not exists bronze.raw_match_info(
        match_id int,
        home_team string,
        away_team string,
        venue string,
        date date,
        file_modification_time timestamp
        )
        """)

    # Sample data
    schema = StructType([
        StructField("match_id", IntegerType(), True),
        StructField("home_team", StringType(), True),
        StructField("away_team", StringType(), True),
        StructField("venue", StringType(), True),
        StructField("date", DateType(), True),
        StructField("file_modification_time", TimestampType(), True)
    ])
    data = [
        (1, "Lille", "Lens", "Pierre Mauroy", date(2024, 3, 29), datetime.now()),
        (2, "Lens", "Lille", "Bollaert", date(2023, 8, 10), datetime.now())
    ]

    # Create DataFrame
    df = spark.createDataFrame(data, schema=schema)
    df.write.mode("overwrite").insertInto("bronze.raw_match_info", overwrite=True)

    spark.sql("""
        create table if not exists bronze.raw_match_summary(
        match_id int,
        team_id string,
        goals int,
        shots int,
        possession int,
        file_modification_time timestamp
        )
        """)

    # Sample data
    schema = StructType([
        StructField("match_id", IntegerType(), True),
        StructField("team_id", StringType(), True),
        StructField("goals", IntegerType(), True),
        StructField("shots", IntegerType(), True),
        StructField("possession", IntegerType(), True),
        StructField("file_modification_time", TimestampType(), True)
    ])
    data = [
        (1, "Lille", 2, 15, 49, datetime.now()),
        (2, "Lille", 1, 8, 56, datetime.now()),
        (1, "Lens", 1, 9, 51, datetime.now()),
        (2, "Lens", 1, 14, 4, datetime.now()),
    ]

    # Create DataFrame
    df = spark.createDataFrame(data, schema=schema)
    df.write.mode("overwrite").insertInto("bronze.raw_match_summary", overwrite=True)

    spark.sql("""
        create table if not exists silver.calculated_summary(
        venue string,
        date date,
        home_team string,
        home_elo double,
        home_goals int,
        home_shots int,
        home_possession int,
        away_team string,
        away_elo double,
        away_goals int,
        away_shots int,
        away_possession int
        )
        """)

    calculated_summary.etl(spark)

    assert spark.table("silver.calculated_summary").count() == 2
