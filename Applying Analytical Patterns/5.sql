-- Compute total wins per team using team points per game

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
)
SELECT team_abbreviation, SUM(win) AS total_wins
FROM team_game_outcome
GROUP BY team_abbreviation
ORDER BY total_wins DESC
FETCH FIRST 1 ROW WITH TIES;
