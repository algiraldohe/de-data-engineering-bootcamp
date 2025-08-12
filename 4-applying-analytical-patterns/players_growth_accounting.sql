-- DROP TABLE players_growth_accounting;
CREATE TABLE players_growth_accounting(
	player_name TEXT,
	first_active_season INTEGER,
	last_active_season INTEGER,
	season_active_state TEXT,
	list_seasons_active INTEGER[],
	current_season INTEGER,
	PRIMARY KEY (player_name, current_season)
);


WITH last_season AS (
	SELECT * FROM players_growth_accounting
	WHERE current_season = 2000
),

current_season AS (
	SELECT
		player_name,
		is_active,
		current_season
	FROM players WHERE current_season = 2001
)
INSERT INTO players_growth_accounting
SELECT
	COALESCE(cs.player_name, ls.player_name) AS player_name,
	COALESCE(ls.first_active_season, cs.current_season) AS first_active_season,
	CASE WHEN cs.is_active = true THEN cs.current_season ELSE ls.last_active_season END AS last_active_season,
	CASE
		WHEN ls.player_name IS NULL THEN 'New'
		WHEN cs.is_active = false AND ls.last_active_season = ls.current_season THEN 'Retired'
		WHEN cs.is_active = true AND ls.last_active_season = ls.current_season THEN 'Continued Playing'
		WHEN cs.is_active = true AND ls.last_active_season < ls.current_season THEN 'Returned from Retirement'
	ELSE 'Stayed Retired' END AS season_active_state,
	COALESCE(ls.list_seasons_active, ARRAY []::INTEGER[])
		|| CASE WHEN cs.player_name IS NOT NULL AND cs.is_active = true THEN ARRAY [cs.current_season]
	       ELSE ARRAY []::INTEGER[]
	       END AS list_seasons_active,
	cs.current_season AS current_season
FROM current_season cs
FULL OUTER JOIN last_season ls ON cs.player_name = ls.player_name

/* validations */
--SELECT * FROM players_growth_accounting
--WHERE player_name = 'Michael Jordan'
--ORDER BY current_season DESC;
--
--
--SELECT * FROM players WHERE player_name = 'Michael Jordan';