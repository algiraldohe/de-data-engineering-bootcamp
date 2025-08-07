from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType, IntegerType, MapType
from chispa.dataframe_comparer import *

from ..jobs.cummulated_year_actors_job import do_actors_cumulation
from collections import namedtuple

ActorsCumulative = namedtuple("ActorsCumulative", "actor actorid films quality_class is_active current_year" )
Actors = namedtuple("Actors", "actor actorid film year votes rating filmid")


def test_actors_cumulation(spark):
    record1 = Actors("Adrian Pasdar", "nm0664499", "Solarbabies", 1986, 4058, 4.8, "tt0091981")
    record2 = Actors("Adrian Pasdar", "nm0664499", "Streets of Gold", 1986, 592, 6.0, "tt0092022")
    record3 = Actors("Adrian Pasdar", "nm0664499", "Top Gun", 1986, 299202, 6.9, "tt0092099")

    input_data = [ record1, record2, record3 ]

    input_dataframe = spark.createDataFrame(input_data)
    actual_df = do_actors_cumulation(spark, input_dataframe)

    films_expected = [
        None,
        {
            "rating": "4.8",
            "votes": "4058",
            "film": "Solarbabies",
            "year": "1986",
            "filmid": "tt0091981",
        },
        {
            "rating": "6.0",
            "votes": "592",
            "film": "Streets of Gold",
            "year": "1986",
            "filmid": "tt0092022",
        },
        {
            "rating": "6.9",
            "votes": "299202",
            "film": "Top Gun",
            "year": "1986",
            "filmid": "tt0092099",
        },
    ]

    expected_output = [
        ActorsCumulative(
            actor="Adrian Pasdar",
            actorid="nm0664499",
            films=films_expected,
            quality_class="bad",
            is_active=True,
            current_year=1986,
        )
    ]
    schema = StructType([
        StructField('actor', StringType(), True),
        StructField('actorid', StringType(), True),
        StructField('films', ArrayType(MapType(StringType(), StringType(), True), True), True),
        StructField('quality_class', StringType(), True),
        StructField('is_active', BooleanType(), False),
        StructField('current_year', IntegerType(), False)
    ])
    expected_df = spark.createDataFrame(expected_output, schema=schema)

    assert_df_equality(actual_df, expected_df, ignore_nullable=True)