"""
ETL to feed table player_id_table
"""
from typing import Tuple

import pyspark.sql.functions as func
from pyspark.sql import SparkSession
from typedspark import DataSet

from src.workloads.quality.schemas.players import Player, PlayerExtra, BlendedPlayer, PlayerIdTable


def extract(spark: SparkSession) -> Tuple[DataSet[PlayerExtra], DataSet[PlayerExtra], DataSet[Player]]:
    """
    Extract and cast player data from providers 1, 2 and 3

    Args:
        spark (SparSession): SparkSession instance
    Returns
        provider_1, provider_2 and provider_3 raw DataSet
    """
    provider_1 = DataSet[PlayerExtra](
        spark.table("bronze.provider_1_players")
        .withColumn("provider_id", func.col("provider_id").cast("long"))
        .withColumn("date_of_birth", func.to_date("date_of_birth", format="yyyy-MM-dd"))
        .withColumn("jersey_number", func.col("jersey_number").cast("int"))
    )
    provider_2 = DataSet[PlayerExtra](
        spark.table("bronze.provider_2_players")
        .withColumn("provider_id", func.col("provider_id").cast("long"))
        .withColumn("date_of_birth", func.to_date("date_of_birth", format="yyyy-MM-dd"))
        .withColumn("jersey_number", func.col("jersey_number").cast("int"))
    )
    provider_3 = DataSet[Player](
        spark.table("bronze.provider_3_players")
        .withColumn("provider_id", func.col("provider_id").cast("long"))
        .withColumn("date_of_birth", func.to_date("date_of_birth", format="yyyy-MM-dd"))
    )
    return provider_1, provider_2, provider_3


def blend_players(provider_1: DataSet[PlayerExtra], provider_2: DataSet[PlayerExtra],
                  provider_3: DataSet[Player]) -> DataSet[BlendedPlayer]:
    """
    Blend player dimension DataFrames from 3 external providers (provider_1, provider_2 and provider_3)
    to a single DataFrame with a global_player_id.

    Args:
        provider_1 (DataSet[PlayerExtra]): DataFrame for provider_1
        provider_2 (DataSet[PlayerExtra]): DataFrame for provider_1
        provider_3 (DataSet[Player]): DataFrame for provider_1

    Returns:
        Blended DataFrame with column global_player_id, provider_1 and provider_2
    """
    # join DataSets together
    score = lambda a, b: \
        f"abs(greatest(length({a}), length({b})) - levenshtein({a},{b})) / greatest(length({a}), length({b}))"
    joined_df = (
        provider_1.alias("provider_1")
        .join(
            provider_2.alias("provider_2"),
            on=func.expr(
                f"({score('provider_1.player_name', 'provider_2.player_name')} > 0.5) "
                "AND (provider_1.date_of_birth=provider_2.date_of_birth)"
            ),
            how="full"
        )
        .join(
            provider_3.alias("provider_3"),
            on=func.expr(
                f"(({score('provider_1.player_name', 'provider_3.player_name')} > 0.5) "
                "AND (provider_1.date_of_birth=provider_3.date_of_birth)) "
                f"OR (({score('provider_2.player_name', 'provider_3.player_name')} > 0.5) "
                "AND (provider_2.date_of_birth=provider_3.date_of_birth))"
            ),
            how="full"
        )
    )
    # column to get non-NULL player_name after join
    non_null_player_name = func.coalesce(
        *[func.col(f"{provider}.player_name") for provider in ("provider_1", "provider_2", "provider_3")]
    )
    # column to get non-NULL date_of_birth after join
    non_null_date_of_birth = func.coalesce(
        *[func.col(f"{provider}.date_of_birth") for provider in ("provider_1", "provider_2", "provider_3")]
    )
    return DataSet[BlendedPlayer](
        joined_df
        .select(
            func.hash(non_null_player_name, non_null_date_of_birth).cast("long").alias("global_player_id"),
            func.struct(func.expr("provider_1.*")).alias("provider_1"),
            func.struct(func.expr("provider_2.*")).alias("provider_2"),
            func.struct(func.expr("provider_3.*")).alias("provider_3")
        )
    )


def transform(provider_1: DataSet[PlayerExtra], provider_2: DataSet[PlayerExtra],
              provider_3: DataSet[Player]) -> DataSet[PlayerIdTable]:
    """
    Blend player dimension DataFrames from 3 external providers (provider_1, provider_2 and provider_3)
    to a single DataFrame with a global_player_id.

    Args:
        provider_1 (DataSet[PlayerExtra]): DataFrame for provider_1
        provider_2 (DataSet[PlayerExtra]): DataFrame for provider_1
        provider_3 (DataSet[Player]): DataFrame for provider_1
    Returns:
        Blended DataFrame with player_id_table mapping
    """

    return DataSet[PlayerIdTable](
        # blend players from the 3 providers
        blend_players(provider_1, provider_2, provider_3)
        # apply mapping
        .select(
            "global_player_id",
            func.coalesce(
                func.col("provider_2.nickname"),
                func.col("provider_1.player_name"),
                func.col("provider_2.player_name"),
                func.col("provider_3.player_name")
            ).alias("name"),
            func.coalesce(
                func.col("provider_1.date_of_birth"),
                func.col("provider_2.date_of_birth"),
                func.col("provider_3.date_of_birth")
            ).alias("dob"),
            func.col("provider_1.provider_id").alias("provider_id1"),
            func.col("provider_2.provider_id").alias("provider_id2"),
            func.col("provider_3.provider_id").alias("provider_id3")
        )
    )


def load(df: DataSet[PlayerIdTable], target_table: str) -> None:
    """
    Load DataSet to table

    Args:
        df (DataSet[PlayerIdTable]: DataFrame to be written
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


def etl(spark: SparkSession):
    """
    Run player_id_table ETL

    Args:
        spark (SparSession): SparkSession instance
    Returns
        None
    """
    load(transform(*extract(spark)), "silver.player_id_table")


if __name__ == "__main__":
    spark_session = (
        SparkSession
        .builder
        .enableHiveSupport()
        .appName("Feed player_id_table")
        .getOrCreate()
    )

    etl(spark_session)
