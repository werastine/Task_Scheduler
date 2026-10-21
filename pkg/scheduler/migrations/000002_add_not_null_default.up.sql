-- Change all NULL to 0
UPDATE scheduler SET repeat_times = 0 WHERE repeat_times IS NULL;
UPDATE scheduler SET repeat_every = 0 WHERE repeat_every IS NULL;

ALTER TABLE scheduler
    ALTER COLUMN repeat_times SET DEFAULT 0,
    ALTER COLUMN repeat_times SET NOT NULL,
    ALTER COLUMN repeat_every SET DEFAULT 0,
    ALTER COLUMN repeat_every SET NOT NULL;