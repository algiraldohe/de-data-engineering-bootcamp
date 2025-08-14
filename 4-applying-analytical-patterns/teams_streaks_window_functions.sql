-- What is the most games a team has won in a 90 game stretch?
WITH base AS (
	SELECT
	  g.game_date_est,
	  g.game_id,
	  CASE s.side WHEN 'home' THEN g.home_team_id ELSE g.visitor_team_id END AS team_id,
	  CASE
	    WHEN s.side = 'home'  THEN (g.home_team_wins = 1)::INTEGER
	    WHEN s.side = 'away'  THEN (g.home_team_wins = 0)::INTEGER
	  END AS is_win
	FROM games g
	CROSS JOIN (VALUES ('home'), ('away')) AS s(side)
),
games_streak AS (
	SELECT
		* ,
		ROW_NUMBER() OVER(PARTITION BY team_id ORDER BY game_date_est) number_games,
		SUM(is_win)
			OVER(
				PARTITION BY team_id
				ORDER BY game_date_est
				ROWS BETWEEN CURRENT ROW AND 89 FOLLOWING
				) "90_game_streak"
	FROM base
)

SELECT
	t.nickname AS team_name,
	MAX(number_games) AS total_number_games,
	MAX("90_game_streak") AS max_90_game_streak,
	DENSE_RANK() OVER(ORDER BY MAX("90_game_streak") DESC) AS ranking
FROM games_streak gs
INNER JOIN teams t ON gs.team_id = t.team_id
GROUP BY team_name
LIMIT 5;