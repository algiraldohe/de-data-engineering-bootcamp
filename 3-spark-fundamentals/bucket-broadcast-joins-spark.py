import pyspark
from pyspark.sql import functions as f

# spark session
spark = SparkSession.builder.appName("SparkFundamentalsHomework").getOrCreate()

# read data
matches_df = spark.read.option("header", True).csv("/home/iceberg/data/matches.csv")
matches_details_df = spark.read.option("header", True).csv("/home/iceberg/data/match_details.csv")
medals_matches_players_df = spark.read.option("header", True).csv("/home/iceberg/data/medals_matches_players.csv")
medals_df = spark.read.option("header", True).csv("/home/iceberg/data/medals.csv")
maps_df = spark.read.option("header", True).csv("/home/iceberg/data/maps.csv")

# create bucketed tables to join
# matches
spark.sql(
    """
        CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed(
            match_id STRING,
            completion_date DATE,
            mapid STRING,
            playlist_id STRING
        )
        USING iceberg
        PARTITIONED BY (completion_date, bucket(4, match_id));
    """
)

# match_details
spark.sql(
    """
        CREATE TABLE IF NOT EXISTS bootcamp.matches_details_bucketed(
            match_id STRING,
            player_gamertag STRING,
            player_total_kills INTEGER
        )
        USING iceberg
        PARTITIONED BY (bucket(4, match_id));
    """
)

# medals_matches_players
spark.sql(
    """
        CREATE TABLE IF NOT EXISTS bootcamp.medals_matches_players_bucketed(
            match_id STRING,
            player_gamertag STRING,
            medal_id STRING,
            medal_count INTEGER
        )
        USING iceberg
        PARTITIONED BY (bucket(4, match_id));
    """
)

# populate tables
# matches
matches_df.select(
    "match_id",
    f.to_date("completion_date").alias("completion_date"),
    "mapid",
    "playlist_id").write.mode("append") \
    .partitionBy("completion_date") \
    .bucketBy(4, "match_id") \
    .saveAsTable("bootcamp.matches_bucketed")

# matches_details
matches_details_df.select(
    "match_id",
    "player_gamertag",
    f.to_number("player_total_kills", f.lit("999")).alias("player_total_kills")) \
    .write.mode("append")\
    .bucketBy(4, "match_id") \
    .saveAsTable("bootcamp.matches_details_bucketed")

# medals_matches_players
medals_matches_players_df.select(
    "match_id",
    "player_gamertag",
    "medal_id",
    f.to_number("count", f.lit("99")).alias("medal_count")) \
    .write.mode("append") \
    .bucketBy(4, "match_id") \
    .saveAsTable("bootcamp.medals_matches_players_bucketed")


# create temporary views to use in spark SQL
# matches
matches_df.createTempView("matches")

# matches details
matches_details_df.createTempView("matches_details")

# medals_matches_players
medals_matches_players_df.createTempView("medals_matches_players")

# clean and dedupped medals dataset
spark.sql(
    """
        WITH cleaned_src AS (
            SELECT 
                name
                , CASE WHEN name = 'Team Takedown' THEN '2896365521'
                       WHEN name = 'Perfect Kill' THEN '1080468863'
                       ELSE medal_id
                  END AS medal_id
                , ROW_NUMBER() OVER(PARTITION BY name ORDER BY medal_id) row_
            FROM dedupped_medals 
        )
    
        SELECT name, medal_id FROM cleaned_src WHERE row_ = 1 AND name IS NOT NULL
    """
).createOrReplaceTempView("dedupped_medals")

# clean maps before join
spark.sql(
    """
        SELECT * FROM maps WHERE mapid IS NOT NULL
    """
).createOrReplaceTempView("cleansed_maps")

# deactivate default broadcast join threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# bucket joins, broadcast joins and aggregations
aggregated_df = spark.sql(
    """
        WITH joined_view AS (
            SELECT
                DATE(m.completion_date) AS match_completion_date
                , mmp.player_gamertag
                , m.playlist_id
                , mp.name AS map_name
                , md.name AS medal_name
                , FIRST(md.player_total_kills) AS player_total_kills
                , SUM(mmp.count) AS medal_count
                , COUNT(m.match_id) AS number_matches
            FROM matches m
            INNER JOIN medals_matches_players mmp ON m.match_id = mmp.match_id
            INNER JOIN matches_details md ON m.match_id = md.match_id AND mmp.player_gamertag = md.player_gamertag
            INNER JOIN dedupped_medals md ON mmp.medal_id = md.medal_id
            INNER JOIN cleansed_maps mp ON m.mapid = mp.mapid
            GROUP BY 1,2,3,4,5
        )

        SELECT 
            match_completion_date
            , player_gamertag
            , playlist_id
            , map_name
            , SUM(number_matches) AS number_matches
            , FIRST(CAST(player_total_kills AS INTEGER)) AS player_total_kills
            , map_from_entries(collect_list(struct(medal_name, medal_count))) AS medal_count_map

        FROM joined_view
        GROUP BY 1,2,3,4
    """
)

# explore different partitions for the aggregated table
aggregated_df.repartition(5, f.col("match_completion_date"), f.col("map_name")) \
                          .sortWithinPartitions(f.col("match_completion_date"), f.col("map_name"))\
                          .write.mode("overwrite").saveAsTable("bootcamp.sorted_agg_medals_matches_players_05")

aggregated_df.repartition(10, f.col("match_completion_date"), f.col("map_name")) \
                          .sortWithinPartitions(f.col("match_completion_date"), f.col("map_name"))\
                          .write.mode("overwrite").saveAsTable("bootcamp.sorted_agg_medals_matches_players_10")

aggregated_df.repartition(5, f.col("match_completion_date"), f.col("map_name"), f.col("playlist_id")) \
                          .sortWithinPartitions(f.col("match_completion_date"), f.col("map_name"), f.col("playlist_id"))\
                          .write.mode("overwrite").saveAsTable("bootcamp.sorted_agg_medals_matches_players_2_05")

aggregated_df.repartition(10, f.col("match_completion_date"), f.col("map_name"), f.col("playlist_id")) \
                          .sortWithinPartitions(f.col("match_completion_date"), f.col("map_name"), f.col("playlist_id"))\
                          .write.mode("overwrite").saveAsTable("bootcamp.sorted_agg_medals_matches_players_2_10")

# compare results of partitioning tables using different number of files and columns
spark.sql(
    """
        SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted_map_only' 
        FROM demo.bootcamp.sorted_agg_medals_matches_players_05.files

        UNION ALL

        SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted_map_only' 
        FROM demo.bootcamp.sorted_agg_medals_matches_players_10.files

        UNION ALL

        SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted_map_playlist' 
        FROM demo.bootcamp.sorted_agg_medals_matches_players_2_05.files

        UNION ALL

        SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted_map_playlist' 
        FROM demo.bootcamp.sorted_agg_medals_matches_players_2_10.files
    """
).show()