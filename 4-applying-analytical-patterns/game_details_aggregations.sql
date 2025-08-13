-- DROP TABLE game_analytics;
CREATE TABLE game_analytics AS (
WITH teams_dedupped AS (
	SELECT *, ROW_NUMBER() OVER(PARTITION BY team_id ORDER BY team_id) row_numb
	FROM teams
),

winners AS (
	SELECT
		game_id,
		season,
		CASE WHEN home_team_wins = 1 THEN home_team_id
			 WHEN home_team_wins = 0 THEN visitor_team_id
		ELSE 999999 END AS won_team_id

	FROM games

),

joined_input AS (
	SELECT
		COALESCE(gd.player_name, 'OVERALL') AS player_name,
		COALESCE(td.nickname, 'OVERALL') AS team_name,
		COALESCE(w.season, 9999) AS  season,
		CASE WHEN w.won_team_id = gd.team_id THEN 1 ELSE 0 END AS did_win_game,
		COALESCE(gd.pts, 0) AS pts

	FROM game_details gd
	INNER JOIN teams_dedupped td ON gd.team_id = td.team_id AND td.row_numb = 1
	INNER JOIN winners w ON gd.game_id = w.game_id

)


	SELECT
		player_name,
		team_name,
		season,
		SUM(pts) AS  total_points,
		SUM(did_win_game) AS total_wins
	FROM joined_input
	GROUP BY GROUPING SETS(
		(player_name, team_name),
		(player_name, season),
		(team_name)
	)
);

-- What player scored the most points for one team
SELECT
	player_name,
	team_name,
	total_points
FROM game_analytics
WHERE player_name IS NOT NULL AND team_name IS NOT NULL
ORDER BY total_points DESC
LIMIT 5;


-- What player scored the most points in a season
SELECT
	player_name,
	season,
	total_points
FROM game_analytics
WHERE player_name IS NOT NULL AND season IS NOT NULL
ORDER BY total_points DESC
LIMIT 5;


-- Which team has won the most games
SELECT
	team_name,
	total_wins
FROM game_analytics
WHERE team_name IS NOT NULL
ORDER BY total_wins DESC
LIMIT 5;
