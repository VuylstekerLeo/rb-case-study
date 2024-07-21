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
            PlayerExtra.provider_id: [57071, 412372],
            PlayerExtra.player_name: ["Peter Gulacsi", "Lucas Alexandre Galdino de Azevedo"],
            PlayerExtra.first_name: ["Peter", None],
            PlayerExtra.last_name: ["Gulacsi", None],
            PlayerExtra.date_of_birth: [date(1990, 5, 6), date(2001, 2, 26)],
            PlayerExtra.gender: ["male", "male"],
            PlayerExtra.country: ["Hungary", "Brazil"],
            PlayerExtra.nickname: [None, "Lucao"],
            PlayerExtra.jersey_number: [1, 40],
            PlayerExtra.nationality: ["Hungary", "Brazil"]
        }
    )

    provider_2 = create_partially_filled_dataset(
        spark,
        PlayerExtra,
        {
            PlayerExtra.provider_id: [57071, 412372],
            PlayerExtra.player_name: ["Peter Gulacsi", "Lucas Galdino de Azevedo"],
            PlayerExtra.first_name: ["Peter", "Lucas Alexandre"],
            PlayerExtra.last_name: ["Gulacsi", "Galdino de Azevedo"],
            PlayerExtra.date_of_birth: [date(1990, 5, 6), date(2001, 2, 26)],
            PlayerExtra.gender: ["male", "male"],
            PlayerExtra.country: ["Hungary", "Brazil"],
            PlayerExtra.nickname: [None, "Lucao"],
            PlayerExtra.jersey_number: [None, None],
            PlayerExtra.nationality: ["Hungary", "Brazil"]
        }
    )

    provider_3 = create_partially_filled_dataset(
        spark,
        Player,
        {
            Player.provider_id: [1, 10],
            Player.player_name: ["Peter Gulacsi", "Craig Dawson"],
            Player.first_name: ["Peter", "Creg"],
            Player.last_name: ["Gulasci", "Dawson"],
            Player.date_of_birth: [date(1990, 5, 6), date(1990, 5, 6)],
            Player.gender: ["male", "Male"],
            Player.country: ["Hungary", "England"]
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
    spark.table("silver.player_id_table").show(vertical=True, truncate=False)
    assert spark.table("silver.player_id_table").count() == 3
