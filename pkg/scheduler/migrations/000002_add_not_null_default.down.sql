ALTER TABLE scheduler 
    ALTER COLUMN repeat_times DROP DEFAULT,
    ALTER COLUMN repeat_times DROP NOT NULL,
    ALTER COLUMN repeat_every DROP DEFAULT,
    ALTER COLUMN repeat_every DROP NOT NULL;