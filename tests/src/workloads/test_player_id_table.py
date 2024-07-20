"""
Test functions for module src/workloads/player_id_table.py
"""
from datetime import date

from pyspark.sql import SparkSession
from typedspark import create_partially_filled_dataset

from src.workloads.player_id_table import etl
from src.workloads.quality.schemas.players import Player, PlayerExtra


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

    provider_1 = create_partially_filled_dataset(
        spark,
        PlayerExtra,
        {
            PlayerExtra.provider_id: [1],
            PlayerExtra.player_name: ["John"],
            PlayerExtra.first_name: ["John"],
            PlayerExtra.last_name: ["John"],
            PlayerExtra.date_of_birth: [date(1998, 9, 12)],
            PlayerExtra.gender: ["male"],
            PlayerExtra.country: ["France"],
            PlayerExtra.nickname: ["Johnny"],
            PlayerExtra.jersey_number: [10],
            PlayerExtra.nationality: ["France"]
        }
    )

    provider_2 = provider_1

    provider_3 = create_partially_filled_dataset(
        spark,
        Player,
        {
            Player.provider_id: [1],
            Player.player_name: ["John"],
            Player.first_name: ["John"],
            Player.last_name: ["John"],
            Player.date_of_birth: [date(1998, 9, 12)],
            Player.gender: ["male"],
            Player.country: ["France"]
        }
    )

    spark.sql("create database if not exists bronze")
    spark.sql("drop table if exists bronze.provider_1_players")
    spark.sql("drop table if exists bronze.provider_2_players")
    spark.sql("drop table if exists bronze.provider_3_players")
    provider_1.write.mode("overwrite").saveAsTable("bronze.provider_1_players")
    provider_2.write.mode("overwrite").saveAsTable("bronze.provider_2_players")
    provider_3.write.mode("overwrite").saveAsTable("bronze.provider_3_players")
    spark.sql("create database if not exists silver")
    spark.sql("""
    create table if not exists silver.player_id_table(
    global_player_id long,
    name string,
    dob date,
    provider_id1 long,
    provider_id2 long,
    provider_id3 long
    )
    """)
    etl(spark)
    assert spark.table("silver.player_id_table").count() == 1
