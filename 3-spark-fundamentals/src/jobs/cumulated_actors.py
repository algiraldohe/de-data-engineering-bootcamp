from pyspark.sql import SparkSession

cumulative_query = """
    WITH last_year AS (
    	SELECT * FROM bootcamp.actors
    	WHERE current_year = 1985

    ),
	current_year AS (
		SELECT
			actor,
			actorid,
			ARRAY_AGG(MAP(
					'year' ,year,
					'film' ,film,
					'votes' ,votes,
					'rating' ,rating,
					'filmid' ,filmid
			)) films,
			AVG(rating) AS rating,
			year
		FROM actor_films
		WHERE year = 1986
		GROUP BY actor,actorid, year
	)

    SELECT 
    	COALESCE(ly.actor, cy.actor) AS actor,
    	COALESCE(ly.actorid, cy.actorid) AS actorid,
        CONCAT(
          CAST(COALESCE(ly.films, ARRAY(CAST(NULL AS MAP<STRING, STRING>))) AS ARRAY<MAP<STRING, STRING>>),
          CAST(
            CASE 
              WHEN cy.year IS NOT NULL THEN cy.films
              ELSE ARRAY(CAST(NULL AS MAP<STRING, STRING>))
            END
          AS ARRAY<MAP<STRING, STRING>>)
        ) AS films,
    	CASE WHEN cy.year IS NOT NULL THEN
    		(CASE
    			WHEN cy.rating > 8 THEN 'star'
    			WHEN cy.rating > 7 AND cy.rating <= 8 THEN 'good'
    			WHEN cy.rating > 6 AND cy.rating <= 7 THEN 'average'
    			WHEN cy.rating <= 6 THEN 'bad'
    		END)
    		ELSE ly.quality_class
    	END AS quality_class,
    	cy.year IS NOT NULL AS is_active,
    	1986 AS current_year
    FROM last_year ly
    FULL OUTER JOIN current_year cy ON ly.actorid = cy.actorid;
"""


def do_actors_cumulation(spark, dataframe):
    spark.sql(
        """
        	CREATE OR REPLACE TABLE bootcamp.actors (
        		actor STRING,
        		actorid STRING,
        		films ARRAY<MAP<STRING, STRING>>,
        		quality_class STRING,
        		is_active BOOLEAN,
        		current_year INT
            )
            USING iceberg
            PARTITIONED BY (current_year);
    	"""
    )
    dataframe.createOrReplaceTempView("actor_films")
    return spark.sql(cumulative_query)


def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("actors_cumulative") \
        .getOrCreate()
    output_df = do_actors_cumulation(spark, spark.table("actor_films"))
    output_df.write.mode("overwrite").insertInto("actors")