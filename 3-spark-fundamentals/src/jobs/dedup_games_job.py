from pyspark.sql import SparkSession

query = """
    WITH dedupped AS (
    SELECT
		gd.game_id,
		gd.team_id,
		gd.team_abbreviation,
		gd.team_city,
		gd.player_id,
		gd.player_name,
		gd.start_position,
		gd.pts,
        ROW_NUMBER()
            OVER(PARTITION BY gd.game_id, gd.team_id, gd.player_id
            ORDER BY gd.game_id DESC) row_num -- Picking the last recorded game
    FROM game_details gd

    )

    -- New game_details dataset without duplicates
    SELECT 
		game_id,
		team_id,
		team_abbreviation,
		team_city,
		player_id,
		player_name,
		start_position,
		pts
    FROM dedupped
    WHERE row_num = 1;
"""


def do_dedup_teams(spark, dataframe):
    dataframe.createOrReplaceTempView("game_details")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("games_dedupped") \
        .getOrCreate()
    output_df = do_team_vertex_transformation(spark, spark.table("game_details"))
    output_df.write.mode("overwrite").insertInto("dedupped_games")
