CREATE TABLE scheduler (
    id VARCHAR PRIMARY KEY,
    payload BYTEA,
    task_type VARCHAR(50) NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'pending',
    send_at TIMESTAMPTZ NOT NULL,
    repeat_times INT,
    repeat_every INT -- seconds
);