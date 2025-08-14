-- How many games in a row did LeBron James score over 10 points a game?

WITH scoring_class AS (
SELECT
	game_id,
	pts,
	CASE WHEN pts > 10 THEN 1 ELSE 0 END AS scored_plus_10,
	CASE WHEN LAG(pts, 1) OVER(ORDER BY game_id) > 10 THEN 1 ELSE 0 END AS prev_scored_plus_10
	FROM game_details
WHERE player_id = '2544' AND pts IS NOT NULL
ORDER BY game_id
),

streak_started AS (
	SELECT
		*,
		CASE WHEN scored_plus_10 != prev_scored_plus_10 THEN true ELSE false END streak_change
	FROM scoring_class
),

streak_identified AS (
 SELECT
	*,
	SUM(CASE WHEN streak_change THEN 1 ELSE 0 END)
		OVER (ORDER BY game_id) as streak_identifier
 FROM streak_started
),

streak_counts AS (
	SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY streak_identifier ORDER BY game_id) streaks_length_count
	FROM streak_identified
)

SELECT MAX(streaks_length_count) FROM streak_counts;