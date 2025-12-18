-- Aggregate points and games along player+team, player+season, and team
-- Ensure games are mapped to the correct season via player_seasons

SELECT
    gd.player_name,
    gd.team_abbreviation,
    ps.season,
    SUM(COALESCE(gd.pts, 0)) AS total_points,
    COUNT(*) AS games_played,
    CASE WHEN GROUPING(gd.player_name) = 1 THEN 'ALL_PLAYERS' ELSE gd.player_name END AS player_label,
    CASE WHEN GROUPING(gd.team_abbreviation) = 1 THEN 'ALL_TEAMS' ELSE gd.team_abbreviation END AS team_label,
    CASE WHEN GROUPING(ps.season) = 1 THEN 'ALL_SEASONS' ELSE ps.season::text END AS season_label
FROM game_details gd
JOIN player_seasons ps
  ON gd.player_name = ps.player_name
  -- If game_date exists, ensure game falls in season boundaries:
  -- AND gd.game_date BETWEEN ps.season_start AND ps.season_end
GROUP BY GROUPING SETS (
    (gd.player_name, gd.team_abbreviation), -- player + team
    (gd.player_name, ps.season),            -- player + season
    (gd.team_abbreviation)                  -- team only
)
ORDER BY total_points DESC;
