-- Track player state changes per season using player_seasons as canonical source

-- Step 1: Get all seasons in the league
WITH seasons AS (
    SELECT DISTINCT season FROM player_seasons
),

-- Step 2: Build a player-season grid (all players x all seasons)
player_seasons_grid AS (
    SELECT p.player_name, s.season
    FROM (SELECT DISTINCT player_name FROM players) p
    CROSS JOIN seasons s
),

-- Step 3: Determine activity per player-season
active_flags AS (
    SELECT
        g.player_name,
        g.season,
        CASE 
            WHEN EXISTS (
                SELECT 1
                FROM player_seasons ps
                WHERE ps.player_name = g.player_name
                  AND ps.season = g.season
            ) THEN TRUE
            ELSE FALSE
        END AS is_active
    FROM player_seasons_grid g
),

-- Step 4: Get previous season activity per player
with_prev AS (
    SELECT
        player_name,
        season,
        is_active,
        LAG(is_active) OVER (PARTITION BY player_name ORDER BY season) AS prev_active
    FROM active_flags
)

-- Step 5: Label player state transitions
SELECT
    player_name,
    season,
    CASE
        WHEN prev_active IS NULL AND is_active THEN 'New'
        WHEN prev_active = TRUE AND is_active = TRUE THEN 'Continued Playing'
        WHEN prev_active = TRUE AND is_active = FALSE THEN 'Retired'
        WHEN prev_active = FALSE AND is_active = TRUE THEN 'Returned from Retirement'
        WHEN prev_active = FALSE AND is_active = FALSE THEN 'Stayed Retired'
    END AS player_state
FROM with_prev
WHERE is_active = TRUE OR prev_active IS NOT NULL
ORDER BY player_name, season;
