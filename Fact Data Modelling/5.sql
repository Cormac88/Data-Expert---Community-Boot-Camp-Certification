CREATE TABLE hosts_cumulated  (
    user_id TEXT,
    host TEXT,
    host_activity_datelist DATE[],
    date DATE,
    PRIMARY KEY (user_id, date, host)
);