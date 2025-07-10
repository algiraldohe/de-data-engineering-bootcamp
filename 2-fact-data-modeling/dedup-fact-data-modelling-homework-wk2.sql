-- 1. DE DUPLICATE GAME DETAILS
-- CHECKING FOR DUPLICATES
-- To Identify duplciates first we need to figure the grain of the data, then group
SELECT game_id, team_id, player_id, COUNT(1)
FROM game_details
GROUP BY 1,2,3
HAVING COUNT(1) > 1;

-- GETTING RID OF DUPLICATES FROM GAME_DETAILS
WITH dedupped AS (
SELECT
	g.game_date_est,
	gd.*,
	ROW_NUMBER()
		OVER(PARTITION BY gd.game_id, gd.team_id, gd.player_id
		ORDER BY g.game_date_est DESC) row_num -- Picking the last recorded game
FROM game_details gd
JOIN games g ON  gd.game_id = g.game_id

)

-- New game_details dataset without duplicates
SELECT * FROM dedupped
WHERE row_num = 1;