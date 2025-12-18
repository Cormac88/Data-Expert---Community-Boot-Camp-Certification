-- Find the player with the most points in a single season
-- Join to player_seasons to assign games to correct seasons

WITH game_with_season AS (
    SELECT
        gd.player_name,
        ps.season,
        COALESCE(gd.pts,0) AS pts
    FROM game_details gd
    JOIN player_seasons ps
      ON gd.player_name = ps.player_name
      -- Optional: filter by game_date if available
      -- AND gd.game_date BETWEEN ps.season_start AND ps.season_end
),
pts_by_player_season AS (
    SELECT player_name, season, SUM(pts) AS total_points
    FROM game_with_season
    GROUP BY player_name, season
)
SELECT player_name, season, total_points
FROM pts_by_player_season
ORDER BY total_points DESC
FETCH FIRST 1 ROW WITH TIES;
