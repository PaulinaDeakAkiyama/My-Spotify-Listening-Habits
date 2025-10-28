
DELIMITER //
CREATE TRIGGER set_default_progress_ms
BEFORE INSERT ON listening_two
FOR EACH ROW
BEGIN
  IF NEW.progress_ms IS NULL THEN
    SELECT progress_ms
    INTO @prev_progress
    FROM listening_two
    ORDER BY timestamp DESC
    LIMIT 1;
    SET NEW.progress_ms = @prev_progress;
  END IF;
END;
//
DELIMITER ;