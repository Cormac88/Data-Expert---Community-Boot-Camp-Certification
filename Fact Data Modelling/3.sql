WITH yesterday AS (
    SELECT *
    FROM user_devices_cumulated
    WHERE date = DATE '2023-01-30'
),
today AS (
    SELECT
        CAST(e.user_id AS TEXT) AS user_id,
        d.browser_type,
        DATE_TRUNC('day', e.event_time::timestamp)::date AS today_date,
        COUNT(1) AS num_events
    FROM events e
    INNER JOIN devices d
        ON d.device_id = e.device_id
    WHERE DATE_TRUNC('day', event_time::timestamp)::date = DATE '2023-01-31'
      AND e.user_id IS NOT NULL
      AND d.device_id IS NOT NULL
    GROUP BY e.user_id, d.browser_type, DATE_TRUNC('day', event_time::timestamp)::date
)
INSERT INTO user_devices_cumulated
SELECT
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(t.browser_type, y.browser_type) AS browser_type,
    COALESCE(y.device_activity_datelist , ARRAY[]::DATE[])
        || CASE
            WHEN t.user_id IS NOT NULL THEN ARRAY[t.today_date]
            ELSE ARRAY[]::DATE[]
           END AS device_activity_datelist ,
    COALESCE(t.today_date, (y.date + INTERVAL '1 day')::date) AS date
FROM yesterday y
FULL OUTER JOIN today t
    ON t.user_id = y.user_id
   AND t.browser_type = y.browser_type;
