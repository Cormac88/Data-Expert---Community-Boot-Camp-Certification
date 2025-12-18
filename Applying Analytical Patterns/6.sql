-- Find the maximum wins in any 90-game stretch for each team
-- Assumes game_id ordering roughly corresponds to chronological order
-- If game_date exists, use it instead for correct ordering

WITH team_game_points AS (
    SELECT 
        team_abbreviation, 
        game_id, 
        SUM(COALESCE(pts,0)) AS team_pts
    FROM game_details
    GROUP BY team_abbreviation, game_id
),
team_game_outcome AS (
    SELECT a.team_abbreviation, a.game_id,
           CASE WHEN a.team_pts > b.team_pts THEN 1 ELSE 0 END AS win
    FROM team_game_points a
    JOIN team_game_points b
      ON a.game_id = b.game_id AND a.team_abbreviation <> b.team_abbreviation
),
rolling AS (
    SELECT 
        team_abbreviation,
        game_id,
        SUM(win) OVER (
            PARTITION BY team_abbreviation
            ORDER BY game_id
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS wins_in_90
    FROM team_game_outcome
)
SELECT team_abbreviation, MAX(wins_in_90) AS max_wins_in_90_games
FROM rolling
GROUP BY team_abbreviation
ORDER BY max_wins_in_90_games DESC;
