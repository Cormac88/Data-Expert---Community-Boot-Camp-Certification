-- Find the player who scored the most points for a single team

WITH pts_by_player_team AS (
    SELECT 
        player_name, 
        team_abbreviation, 
        SUM(COALESCE(pts, 0)) AS total_points
    FROM game_details
    GROUP BY player_name, team_abbreviation
)
SELECT player_name, team_abbreviation, total_points
FROM pts_by_player_team
ORDER BY total_points DESC
FETCH FIRST 1 ROW WITH TIES;
