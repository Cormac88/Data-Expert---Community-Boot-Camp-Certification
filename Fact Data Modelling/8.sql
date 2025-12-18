INSERT INTO host_activity_reduced (user_id, month, host, hit_array, unique_visitors)
WITH daily_aggregate AS (
    SELECT
        e.user_id::text AS user_id,
        e.host AS host,
        DATE(e.event_time) AS date,
        COUNT(*) AS hit_count,
        COUNT(DISTINCT e.user_id) AS unique_visitors_count
    FROM events e
    WHERE DATE(e.event_time) = DATE '2023-01-01'
      AND e.user_id IS NOT NULL
      AND e.host IS NOT NULL
    GROUP BY e.user_id, e.host, DATE(e.event_time)
),
yesterday_array AS (
    SELECT *
    FROM host_activity_reduced
    WHERE month = DATE_TRUNC('month', DATE '2023-01-01')
),
combined AS (
    SELECT
        COALESCE(da.user_id, ya.user_id) AS user_id,
        COALESCE(da.host, ya.host, 'unknown_host') AS host,
        COALESCE(ya.month::date, DATE_TRUNC('month', da.date)::date) AS month,

        CASE
            WHEN ya.hit_array IS NOT NULL THEN
                ya.hit_array || ARRAY[COALESCE(da.hit_count, 0)]
            ELSE
                ARRAY_FILL(
                    0,
                    ARRAY[
                        GREATEST((EXTRACT(DAY FROM da.date) - 1)::int, 0)
                    ]
                ) || ARRAY[COALESCE(da.hit_count, 0)]
        END AS hit_array,

        CASE
            WHEN ya.unique_visitors IS NOT NULL THEN
                ya.unique_visitors || ARRAY[COALESCE(da.unique_visitors_count, 0)]
            ELSE
                ARRAY_FILL(
                    0,
                    ARRAY[
                        GREATEST((EXTRACT(DAY FROM da.date) - 1)::int, 0)
                    ]
                ) || ARRAY[COALESCE(da.unique_visitors_count, 0)]
        END AS unique_visitors

    FROM daily_aggregate da
    FULL OUTER JOIN yesterday_array ya
      ON da.user_id = ya.user_id
     AND da.host = ya.host
)
SELECT
    user_id,
    month::date AS month,
    host,
    hit_array,
    unique_visitors
FROM combined
ON CONFLICT (user_id, month, host)
DO UPDATE
SET hit_array = EXCLUDED.hit_array,
    unique_visitors = EXCLUDED.unique_visitors;