"""
Typing data quality folder for player dimension.
"""
from typedspark import Column, Schema, StructType
from pyspark.sql.types import DateType, IntegerType, LongType, StringType


class Player(Schema):
    provider_id: Column[LongType]
    player_name: Column[StringType]
    first_name: Column[StringType]
    last_name: Column[StringType]
    date_of_birth: Column[DateType]
    gender: Column[StringType]
    country: Column[StringType]


class PlayerExtra(Player):
    nickname: Column[StringType]
    jersey_number: Column[IntegerType]
    nationality: Column[StringType]


class BlendedPlayer(Schema):
    global_player_id: Column[LongType]
    provider_1: Column[StructType[PlayerExtra]]
    provider_2: Column[StructType[PlayerExtra]]
    provider_3: Column[StructType[Player]]


class PlayerIdTable(Schema):
    global_player_id: Column[LongType]
    name: Column[StringType]
    dob: Column[DateType]
    provider_id1: Column[LongType]
    provider_id2: Column[LongType]
    provider_id3: Column[LongType]
