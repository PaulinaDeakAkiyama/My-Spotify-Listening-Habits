DROP EVENT IF EXISTS delete_filler_dates;

CREATE EVENT IF NOT EXISTS delete_filler_dates
ON SCHEDULE EVERY 1 HOUR
DO
  CALL clean_filler_dates();