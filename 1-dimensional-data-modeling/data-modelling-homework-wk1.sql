
-- Create the required types
-- DROP TYPE composite_films;
CREATE TYPE composite_films AS (
	year INTEGER,
	film TEXT,
	votes INTEGER,
	rating REAL,
	filmid TEXT
);

CREATE TYPE composite_quality_class AS ENUM ('star','good', 'average', 'bad');

-- DROP TYPE actors_scd_type;
CREATE TYPE actors_scd_type AS (
	is_active BOOLEAN,
	quality_class composite_quality_class,
	start_year INTEGER,
	end_year INTEGER
);

-- Create the `actors` table
-- DROP TABLE actors;
CREATE TABLE actors (
	actor TEXT,
	actorid TEXT,
	films composite_films[],
	quality_class composite_quality_class,
	is_active BOOLEAN,
	current_year INTEGER,
	PRIMARY KEY (actorid, current_year));

-- MAX year 2021 , MIN year 1970
-- Creating cummulative actors table sequentially
WITH last_year AS (
	SELECT * FROM actors
	WHERE current_year = 1985

),
	current_year AS (
		SELECT
			actor,
			actorid,
			ARRAY_AGG(ARRAY[
				ROW(
					year,
					film,
					votes,
					rating,
					filmid
				)
			]::composite_films[]) films,
			AVG(rating) AS rating,
			year
		FROM actor_films
		WHERE year = 1986
		GROUP BY actor,actorid, year
	)
INSERT INTO actors
SELECT
	COALESCE(ly.actor, cy.actor) AS actor,
	COALESCE(ly.actorid, cy.actorid) AS actorid,
	COALESCE(ly.films, ARRAY[]::composite_films[])
		|| CASE WHEN cy.year IS NOT NULL THEN cy.films
			ELSE ARRAY[]::composite_films[]
			END AS films,
	CASE WHEN cy.year IS NOT NULL THEN
		(CASE
			WHEN cy.rating > 8 THEN 'star'
			WHEN cy.rating > 7 AND cy.rating <= 8 THEN 'good'
			WHEN cy.rating > 6 AND cy.rating <= 7 THEN 'average'
			WHEN cy.rating <= 6 THEN 'bad'
		END):: composite_quality_class
		ELSE ly.quality_class
	END AS quality_class,
	cy.year IS NOT NULL AS is_active,
	1986 AS current_year
FROM last_year ly
FULL OUTER JOIN current_year cy ON ly.actorid = cy.actorid;

-- Creating Actors SCD table
-- DROP TABLE actors_history_scd;
CREATE TABLE actors_history_scd (
	actorid TEXT,
	actor TEXT,
	is_active BOOLEAN,
	quality_class composite_quality_class,
	current_year INTEGER,
	start_year INTEGER,
	end_year INTEGER,
	PRIMARY KEY (actorid, start_year)
)

-- Back filling the whole historical data
WITH with_previous AS(
	SELECT * ,
		LAG(quality_class, 1) OVER( PARTITION BY actor ORDER BY current_year) previous_quality_class,
		LAG(is_active, 1) OVER( PARTITION BY actor ORDER BY current_year) previous_is_active
	FROM actors
),

with_indicator AS (
	SELECT
		actor,
		actorid,
		quality_class,
		is_active,
		current_year,
		CASE
			WHEN previous_quality_class <> quality_class THEN 1
			WHEN previous_is_active <> is_active THEN 1
			ELSE 0
		END AS change_indicator

	FROM with_previous
),

with_streaks AS (
	SELECT
		*,
		SUM(change_indicator)
			OVER (PARTITION BY actor ORDER BY current_year) streak_change_indicator
	FROM with_indicator
)
INSERT INTO actors_history_scd
SELECT
	actorid,
	actor,
	is_active,
	quality_class,
	1985 AS current_year,
	MIN(current_year) start_year,
	MAX(current_year) end_year
FROM with_streaks
GROUP BY
	actorid,
	actor,
	streak_change_indicator,
	is_active,
	quality_class
ORDER BY
	actor;

-- Incremental filling SCD

WITH last_year_scd AS (
	SELECT * FROM actors_history_scd
	WHERE current_year = 1985
	AND end_year = 1985
),

-- Historical records that won't change
historical_scd AS (
	SELECT * FROM actors_history_scd
	WHERE current_year = 1985
	AND end_year < 1985

),

this_year_data AS (
	SELECT * FROM actors
	WHERE current_year = 1986
),

-- Records that present no change from last year to this year
unchanged_records AS (
	SELECT
		ly.actorid,
		ly.actor,
		ly.is_active,
		ly.quality_class,
		ly.start_year AS start_year,
		ty.current_year AS end_year
	FROM this_year_data ty
	LEFT JOIN last_year_scd ly ON ty.actorid = ly.actorid
	WHERE ty.is_active = ly.is_active AND ty.quality_class = ly.quality_class

),
-- A changed record is a new record to be inserted in the SCD table hence we use ARRAY
nested_changed_records AS (
	SELECT
		ty.actorid,
		ty.actor,
		UNNEST(ARRAY[
			ROW(
				ly.is_active,
				ly.quality_class,
				ly.start_year,
				ly.end_year
			):: actors_scd_type,
			ROW(
				ty.is_active,
				ty.quality_class,
				ty.current_year,
				ty.current_year
			):: actors_scd_type
		]) AS  records
	FROM this_year_data ty
	LEFT JOIN last_year_scd ly ON ty.actorid = ly.actorid
	WHERE (ty.is_active <> ly.is_active OR ty.quality_class <> ly.quality_class)
),
-- Then we unnest the changed records keeping both
unnested_changed_records AS (
	SELECT
		actorid,
		actor,
		(records::actors_scd_type).is_active,
		(records::actors_scd_type).quality_class,
		1986 AS current_year,
		(records::actors_scd_type).start_year,
		(records::actors_scd_type).end_year
	FROM nested_changed_records

),
-- Records that will start their lifespan in the current year
new_records AS (
	SELECT
		ty.actorid,
		ty.actor,
		ty.is_active,
		ty.quality_class,
		1986 AS current_year,
		ty.current_year AS start_year,
		ty.current_year AS end_year
	FROM this_year_data ty
	LEFT JOIN last_year_scd ly ON ty.actorid = ly.actorid
	WHERE ly.actorid IS NULL

)

-- Concatenating everything together

SELECT * FROM historical_scd

UNION ALL

SELECT * FROM unnested_changed_records

UNION ALL

SELECT * FROM new_records;