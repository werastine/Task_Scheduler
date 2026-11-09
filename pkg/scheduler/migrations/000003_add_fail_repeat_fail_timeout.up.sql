-- add fail_times and fail_timeout
ALTER TABLE scheduler
ADD COLUMN fail_times INT
ADD COLUMN fail_timeout INT -- seconds
ADD COLUMN fail_initial_attempts INT