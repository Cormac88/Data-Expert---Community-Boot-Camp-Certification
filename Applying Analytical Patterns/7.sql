-- Find longest consecutive games LeBron scored over 10 points

WITH lebron_games AS (
    SELECT
        game_id,
        COALESCE(pts,0) AS pts,
        CASE WHEN COALESCE(pts,0) > 10 THEN 1 ELSE 0 END AS over10
    FROM game_details
    WHERE player_name = 'LeBron James'
),
streaks AS (
    SELECT *,
           ROW_NUMBER() OVER (ORDER BY game_id)
         - ROW_NUMBER() OVER (PARTITION BY over10 ORDER BY game_id) AS grp
    FROM lebron_games
)
SELECT
    COUNT(*) AS games_in_row_over_10,
    MIN(game_id) AS streak_start_game,
    MAX(game_id) AS streak_end_game
FROM streaks
WHERE over10 = 1
GROUP BY grp
ORDER BY games_in_row_over_10 DESC
FETCH FIRST 1 ROW WITH TIES;
