from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType, IntegerType, DoubleType, LongType
from chispa.dataframe_comparer import *

from ..jobs.dedupped_games import do_dedup_teams
from collections import namedtuple

GamesDedupped = namedtuple("GamesDedupped", "game_id team_id team_abbreviation team_city player_id player_name start_position pts" )
GameDetails = namedtuple("GameDetails", "game_id team_id team_abbreviation team_city player_id player_name start_position pts" )


def test_games_dedup(spark):
    record1 = GameDetails(52100211,	1610612740,	"NOP", "New Orleans", 202685, "Jonas Valanciunas", "C", 8.0)
    record2 = GameDetails(52100211,	1610612740,	"NOP", "New Orleans", 202685, "Jonas Valanciunas", "C", 8.0)
    record3 = GameDetails(52100211,	1610612740, "NOP", "New Orleans", 203503, "Tony Snell", None,  0.0)


    input_data = [ record1, record2, record3 ]

    input_dataframe = spark.createDataFrame(input_data)
    actual_df = do_dedup_teams(spark, input_dataframe)

    expected_output = [
        GamesDedupped(52100211, 1610612740, "NOP", "New Orleans", 202685, "Jonas Valanciunas", "C", 8.0),
        GamesDedupped(52100211, 1610612740, "NOP", "New Orleans", 203503, "Tony Snell", None, 0.0)
    ]
    schema = StructType([
        StructField('game_id', LongType(), True),
        StructField('team_id', LongType(), True),
        StructField('team_abbreviation', StringType(), True),
        StructField('team_city', StringType(), True),
        StructField('player_id', LongType(), True),
        StructField('player_name', StringType(), True),
        StructField('start_position', StringType(), True),
        StructField('pts', DoubleType(), True),
    ])
    expected_df = spark.createDataFrame(expected_output, schema=schema)

    assert_df_equality(actual_df, expected_df, ignore_nullable=True)