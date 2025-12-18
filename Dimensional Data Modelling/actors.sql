 CREATE TYPE films_stats AS (
                         film TEXT,
                         votes INTEGER,
                         rating INTEGER,
                         filmid TEXT
                       );


 CREATE TYPE quality_stats AS
     ENUM ('bad', 'average', 'good', 'star');


 CREATE TABLE actors  (
     actor TEXT,
     actorid TEXT,
     films films_stats[],
     quality_class quality_stats,
     is_active BOOLEAN,
     current_year INTEGER,
     PRIMARY KEY (actorid, current_year)
 );

 WITH params AS (
    SELECT
        MAX(current_year) AS last_year,
        MAX(current_year) + 1 AS this_year
    FROM actors
),
last_year AS (
    SELECT a.*
    FROM actors a
    JOIN params p ON a.current_year = p.last_year
),
this_year AS (
    SELECT
        af.actorid,
        af.actor,
        af.year,
        ARRAY_AGG(
          ROW(af.film, af.votes, af.rating, af.filmid)::films_stats
          ORDER BY af.filmid
        ) AS film_array,
        AVG(af.rating) AS avg_rating
    FROM actor_films af
    JOIN params p ON af.year = p.this_year
    GROUP BY af.actorid, af.actor, af.year
)

INSERT INTO actors
SELECT
    COALESCE(ly.actor, ty.actor) AS actor,
    COALESCE(ly.actorid, ty.actorid) AS actorid,
    COALESCE(ly.films, ARRAY[]::films_stats[]) || COALESCE(ty.film_array, ARRAY[]::films_stats[]) AS films,
    CASE
        WHEN ty.avg_rating IS NOT NULL THEN
            CASE
                WHEN ty.avg_rating >= 8 THEN 'star'
                WHEN ty.avg_rating >= 7 THEN 'good'
                WHEN ty.avg_rating >= 6 THEN 'average'
                ELSE 'bad'
            END::quality_stats
        ELSE ly.quality_class
    END AS quality_class,
    ty.year IS NOT NULL AS is_active,
    COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM last_year ly
FULL OUTER JOIN this_year ty
ON ly.actorid = ty.actorid;

 CREATE TABLE actors_history_scd   (
     actorid TEXT,
     actor TEXT,
     quality_class quality_stats,
     is_active BOOLEAN,
     start_year INTEGER,
     end_year INTEGER,
     current_year INTEGER,
     PRIMARY KEY (actorid, start_year)
 );

 -- 4.sql: backfill actors_history_scd from actors (parameterized)
WITH params AS (
    -- Derive the max year currently present in actors (the last year we have snapshots for)
    SELECT MAX(current_year) AS max_year
    FROM actors
),
with_previous AS (
    SELECT
        actorid,
        actor,
        current_year,
        quality_class,
        is_active,
        LAG(quality_class) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_quality_class,
        LAG(is_active) OVER (PARTITION BY actorid ORDER BY current_year)      AS previous_is_active
    FROM actors
    JOIN params p ON current_year <= p.max_year
),
with_indicators AS (
    SELECT *,
        CASE
          WHEN quality_class IS DISTINCT FROM previous_quality_class
            OR is_active IS DISTINCT FROM previous_is_active
          THEN 1 ELSE 0
        END AS change_indicator
    FROM with_previous
),
with_streaks AS (
    SELECT *,
        SUM(change_indicator) OVER (PARTITION BY actorid ORDER BY current_year) AS streak_identifier
    FROM with_indicators
)

INSERT INTO actors_history_scd (
  actorid,
  actor,
  quality_class,
  is_active,
  start_year,
  end_year,
  current_year
)
SELECT
    actorid,
    actor,
    quality_class,
    is_active,
    MIN(current_year) AS start_year,
    MAX(current_year) AS end_year,
    (SELECT max_year FROM params) AS current_year
FROM with_streaks
GROUP BY actorid, actor, streak_identifier, is_active, quality_class
ORDER BY actorid, streak_identifier;

-- 5.sql: incremental SCD update (parameterized)
CREATE TYPE IF NOT EXISTS scd_type AS (
  quality_class quality_stats,
  is_active BOOLEAN,
  start_year INTEGER,
  end_year INTEGER
);

WITH params AS (
    -- derive last processed SCD year and the new target year from actors table
    -- last_scd_year: the latest current_year present in actors_history_scd (if none, fallback to NULL)
    -- this_year: the next year we want to generate SCD for (usually last_scd_year + 1),
    -- but we derive it from the actors snapshot table so this ties to the actors pipeline.
    SELECT
        (SELECT MAX(current_year) FROM actors_history_scd) AS last_scd_year,
        (SELECT MAX(current_year) FROM actors) AS actors_max_year
),
-- Normalize parameters: if actors_max_year is greater than last_scd_year, that is our this_year.
-- We expect actors to have a snapshot for the intended this_year.
effective_params AS (
    SELECT
      COALESCE(p.last_scd_year, (SELECT MIN(current_year) - 1 FROM actors)) AS last_year,
      p.actors_max_year AS this_year
    FROM params p
),
last_year_scd AS (
    -- SCD records considered "open" at the end of the last SCD run
    SELECT *
    FROM actors_history_scd a
    JOIN effective_params ep ON a.current_year = ep.last_year
    WHERE a.end_year = ep.last_year
),
historical_scd AS (
    -- Historical SCD entries that ended before last_year (carry forward unchanged)
    SELECT a.actorid, a.actor, a.quality_class, a.is_active, a.start_year, a.end_year
    FROM actors_history_scd a
    JOIN effective_params ep ON a.current_year = ep.last_year
    WHERE a.end_year < ep.last_year
),
this_year_data AS (
    -- New snapshot for this_year produced by the actors pipeline
    SELECT *
    FROM actors a
    JOIN effective_params ep ON a.current_year = ep.this_year
),
unchanged_records AS (
    -- Actors whose attributes did NOT change vs last_year_scd: extend their end_year to this_year
    SELECT
      ty.actorid,
      ty.actor,
      ty.quality_class,
      ty.is_active,
      ly.start_year,
      ty.current_year AS end_year
    FROM this_year_data ty
    JOIN last_year_scd ly
      ON ty.actorid = ly.actorid
    WHERE ty.quality_class IS NOT DISTINCT FROM ly.quality_class
      AND ty.is_active        IS NOT DISTINCT FROM ly.is_active
),
changed_records AS (
    -- Actors with changed attributes: we will output two records (old closed + new opened)
    SELECT
      ty.actorid,
      ty.actor,
      unnest(ARRAY[
         ROW(ly.quality_class, ly.is_active, ly.start_year, ly.end_year)::scd_type,
         ROW(ty.quality_class, ty.is_active, ty.current_year, ty.current_year)::scd_type
      ]) AS records
    FROM this_year_data ty
    LEFT JOIN last_year_scd ly ON ty.actorid = ly.actorid
    WHERE (ty.quality_class IS DISTINCT FROM ly.quality_class)
       OR (ty.is_active        IS DISTINCT FROM ly.is_active)
),
unnested_changed_records AS (
    SELECT
      ty.actorid,
      ty.actor,
      (records::scd_type).quality_class AS quality_class,
      (records::scd_type).is_active    AS is_active,
      (records::scd_type).start_year   AS start_year,
      (records::scd_type).end_year     AS end_year
    FROM changed_records ty
),
new_records AS (
    -- Actors present in this_year_data but not in last_year_scd at all -> insert as new run
    SELECT
      ty.actorid,
      ty.actor,
      ty.quality_class,
      ty.is_active,
      ty.current_year AS start_year,
      ty.current_year AS end_year
    FROM this_year_data ty
    LEFT JOIN last_year_scd ly ON ty.actorid = ly.actorid
    WHERE ly.actorid IS NULL
),
missing_from_ty AS (
    -- Actors that were in last_year_scd but have no record in this_year_data:
    -- preserve their last SCD entry as-is (they "disappeared" this year)
    SELECT
      ly.actorid,
      ly.actor,
      ly.quality_class,
      ly.is_active,
      ly.start_year,
      ly.end_year,
      (SELECT this_year FROM effective_params) AS current_year
    FROM last_year_scd ly
    LEFT JOIN this_year_data ty ON ty.actorid = ly.actorid
    WHERE ty.actorid IS NULL
)

INSERT INTO actors_history_scd (
  actorid,
  actor,
  quality_class,
  is_active,
  start_year,
  end_year,
  current_year
)
-- historical_scd (older closed runs)
SELECT actorid, actor, quality_class, is_active, start_year, end_year, (SELECT this_year FROM effective_params) AS current_year
FROM historical_scd

UNION ALL

-- unchanged runs extended to this_year
SELECT actorid, actor, quality_class, is_active, start_year, end_year, (SELECT this_year FROM effective_params) AS current_year
FROM unchanged_records

UNION ALL

-- changed runs, unnested into old closed + new opened
SELECT actorid, actor, quality_class, is_active, start_year, end_year, (SELECT this_year FROM effective_params) AS current_year
FROM unnested_changed_records

UNION ALL

-- brand new actors for this year
SELECT actorid, actor, quality_class, is_active, start_year, end_year, (SELECT this_year FROM effective_params) AS current_year
FROM new_records

UNION ALL

-- preserve actors that disappeared this year (no new snapshot)
SELECT actorid, actor, quality_class, is_active, start_year, end_year, current_year
FROM missing_from_ty;