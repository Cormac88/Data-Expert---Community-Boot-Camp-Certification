WITH yesterday AS (
    SELECT *
    FROM hosts_cumulated
    WHERE date = DATE '2023-01-30'
),
today AS (
    SELECT
        CAST(e.user_id AS TEXT) AS user_id,
        e.host,
        DATE_TRUNC('day', e.event_time::timestamp)::date AS today_date,
        COUNT(1) AS num_events
    FROM events e
    WHERE DATE_TRUNC('day', e.event_time::timestamp)::date = DATE '2023-01-31'
      AND e.user_id IS NOT NULL
      AND e.host IS NOT NULL
    GROUP BY e.user_id, e.host, DATE_TRUNC('day', e.event_time::timestamp)::date
)
INSERT INTO hosts_cumulated
SELECT
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(t.host, y.host) AS host,
    COALESCE(y.host_activity_datelist, ARRAY[]::DATE[])
        || CASE
            WHEN t.user_id IS NOT NULL THEN ARRAY[t.today_date]
            ELSE ARRAY[]::DATE[]
           END AS host_activity_datelist,
    COALESCE(t.today_date, (y.date + INTERVAL '1 day')::date) AS date
FROM yesterday y
FULL OUTER JOIN today t
    ON t.user_id = y.user_id
   AND t.host = y.host;