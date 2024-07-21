"""
ETL to feed club_elo raw
"""
import pathlib
from datetime import datetime

import pyspark.sql.functions as func
from pyspark.sql import SparkSession, Window
from typedspark import DataSet

from src.workloads.quality.schemas.team import ClubElo


def extract(spark, directory) -> DataSet[ClubElo]:
    """
    Extract last written files and cast schema.

    Args:
        spark (SparSession): SparkSession instance
        directory (str): Path to ingested files
    Returns
        Cast DataSet
    """
    target_table = spark.table("bronze.raw_club_elo")
    last_timestamp = target_table.select(func.max("file_modification_time")).collect()[0][0]
    if last_timestamp is None:
        last_timestamp = datetime(2000, 1, 11)
    new_data = (
        spark
        .read
        .csv(
            directory, header=True, schema=target_table.drop("file_modification_time").schema,
            enforceSchema=True, mode="PERMISSIVE", columnNameOfCorruptRecord="malformed_rows"
        )
        .select('*', '_metadata.file_modification_time')
        .where(func.col("file_modification_time") > last_timestamp)
    )
    return DataSet[ClubElo](new_data)


def transform(df: DataSet[ClubElo]) -> DataSet[ClubElo]:
    """
    Filter duplicated elements.

    Args:
        df (DataSet[ClubElo]): DataFrame to transform
    Returns:
        Filtered DataFrame
    """
    window_spec = Window().partitionBy("Club", "From").orderBy(func.col("file_modification_time").desc())
    return (
        df
        .where(func.col("Club").isNotNull())
        .withColumn("row_number", func.row_number().over(window_spec))
        .where("row_number=1")
        .drop("row_number")
    )


def load(df: DataSet[ClubElo], target_table) -> None:
    """
    Load DataSet to table

    Args:
        df (DataSet[ClubElo]): DataFrame to be written
        target_table: target table name
    Returns:
        None
    """
    # write to target table
    (
        df
        .write
        .mode("overwrite")
        .insertInto(target_table, overwrite=True)
    )


def etl(spark: SparkSession, directory: str) -> None:
    """
    Run club_elo ETL

    Args:
        spark (SparSession): SparkSession instance
        directory (str): Path to ingested files
    Returns
        None
    """
    load(transform(extract(spark, directory)), "bronze.raw_club_elo")


def main() -> None:
    """
    Main entry point

    Returns
        None
    """
    root = pathlib.Path(__file__).parent.parent.parent
    folder = f"{root}/data/raw/club_elo_api"

    spark_session = (
        SparkSession
        .builder
        .enableHiveSupport()
        .appName("Feed raw club_elo")
        .getOrCreate()
    )

    etl(spark_session, folder)


if __name__ == "__main__":
    main()
