CREATE TABLE user_devices_cumulated (
    user_id TEXT,
    browser_type TEXT,
    device_activity_datelist DATE[],
    date DATE,
    PRIMARY KEY (user_id, date, browser_type)
);