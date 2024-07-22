"""
Typing data quality folder for team dimension.
"""
from typedspark import Column, Schema
from pyspark.sql.types import DateType, IntegerType, StringType, TimestampType, DoubleType


class MatchInfo(Schema):
    match_id: Column[IntegerType]
    home_team: Column[StringType]
    away_team: Column[StringType]
    venue: Column[StringType]
    date: Column[DateType]
    file_modification_time: Column[TimestampType]


class MatchSummary(Schema):
    match_id: Column[IntegerType]
    team_id: Column[StringType]
    goals: Column[IntegerType]
    shots: Column[IntegerType]
    possession: Column[IntegerType]
    file_modification_time: Column[TimestampType]


class CalculatedMatchSummary(Schema):
    venue: Column[StringType]
    date: Column[DateType]
    home_team: Column[StringType]
    home_elo: Column[DoubleType]
    home_goals: Column[IntegerType]
    home_shots: Column[IntegerType]
    home_possession: Column[IntegerType]
    away_team: Column[StringType]
    away_elo: Column[DoubleType]
    away_goals: Column[IntegerType]
    away_shots: Column[IntegerType]
    away_possession: Column[IntegerType]
