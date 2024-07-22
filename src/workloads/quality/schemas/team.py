"""
Typing data quality folder for team dimension.
"""
from typedspark import Column, Schema
from pyspark.sql.types import DateType, DoubleType, IntegerType, LongType, StringType, TimestampType


class ClubElo(Schema):
    Rank: Column[LongType]
    Club: Column[StringType]
    Country: Column[StringType]
    Level: Column[IntegerType]
    Elo: Column[DoubleType]
    From: Column[DateType]
    To: Column[DateType]
    file_modification_time: Column[TimestampType]
