CREATE TABLE host_activity_reduced (
    user_id TEXT,
    month DATE,
    host TEXT,
    hit_array INTEGER[],
    unique_visitors INTEGER[],
    PRIMARY KEY (user_id, month, host)
);