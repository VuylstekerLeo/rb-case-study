"""
ETL to feed club_elo raw
"""
import pathlib
from typing import Tuple

import pyspark.sql.functions as func
from pyspark.sql import SparkSession
from typedspark import DataSet

from src.workloads.quality.schemas.team import ClubElo
from src.workloads.quality.schemas.match import MatchInfo, MatchSummary, CalculatedMatchSummary


def extract(spark) -> Tuple[DataSet[ClubElo], DataSet[MatchInfo], DataSet[MatchSummary]]:
    """
    Extract last written files and cast schema.

    Args:
        spark (SparSession): SparkSession instance
    Returns
        Raw dataframes club_elo, match_info and match_summary
    """
    club_elo = spark.table("bronze.raw_club_elo")
    match_summary = spark.table("bronze.raw_match_summary")
    match_info = spark.table("bronze.raw_match_info")
    return DataSet[ClubElo](club_elo), DataSet[MatchInfo](match_info), DataSet[MatchSummary](match_summary)


def transform(club_elo: DataSet[ClubElo], match_info: DataSet[MatchInfo], match_summary: DataSet[MatchSummary]) \
        -> DataSet[CalculatedMatchSummary]:
    """
    Filter duplicated elements.

    Args:
        club_elo (DataSet[ClubElo]): DataFrame to transform
        match_info (DataSet[MatchInfo]): DataFrame to transform
        match_summary (DataSet[MatchSummary]): DataFrame to transform
    Returns:
        Joined_dataframe
    """
    return DataSet[CalculatedMatchSummary](
        match_info.alias("match_info")
        .join(
            club_elo.alias("home_club_elo"),
            func.expr(
                "(match_info.home_team=home_club_elo.Club) "
                "AND (match_info.date between home_club_elo.From AND home_club_elo.To)"
            ),
            "left"
        )
        .join(
            club_elo.alias("away_club_elo"),
            func.expr(
                "(match_info.away_team=away_club_elo.Club) "
                "AND (match_info.date between away_club_elo.From AND away_club_elo.To)"
            ),
            "left"
        )
        .join(
            match_summary.alias("home_match_summary"),
            func.expr(
                "(match_info.home_team=home_match_summary.team_id) "
                "AND (match_info.match_id=home_match_summary.match_id)"
            ),
            "left"
        )
        .join(
            match_summary.alias("away_match_summary"),
            func.expr(
                "(match_info.away_team=away_match_summary.team_id) "
                "AND (match_info.match_id=away_match_summary.match_id)"
            ),
            "left"
        )
        .select(
            "match_info.venue",
            "match_info.date",
            "match_info.home_team",
            func.col("home_club_elo.Elo").alias("home_elo"),
            func.col("home_match_summary.goals").alias("home_goals"),
            func.col("home_match_summary.shots").alias("home_shots"),
            func.col("home_match_summary.possession").alias("home_possession"),
            "match_info.away_team",
            func.col("away_club_elo.Elo").alias("away_elo"),
            func.col("away_match_summary.goals").alias("away_goals"),
            func.col("away_match_summary.shots").alias("away_shots"),
            func.col("away_match_summary.possession").alias("away_possession")
        )
    )


def load(df: DataSet[CalculatedMatchSummary], target_table) -> None:
    """
    Load DataSet to table

    Args:
        df (DataSet[CalculatedMatchSummary]): DataFrame to be written
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


def etl(spark: SparkSession) -> None:
    """
    Run club_elo ETL

    Args:
        spark (SparSession): SparkSession instance
    Returns
        None
    """
    load(transform(*extract(spark)), "silver.calculated_summary")


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
        .appName("Feed silver calculated_summary")
        .getOrCreate()
    )

    etl(spark_session, folder)


if __name__ == "__main__":
    main()
