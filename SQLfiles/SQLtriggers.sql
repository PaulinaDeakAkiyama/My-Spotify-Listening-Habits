

DELIMITER //
CREATE TRIGGER set_default_progress_ms
BEFORE INSERT ON listening_two
FOR EACH ROW
BEGIN
  IF NEW.progress_ms IS NULL THEN
    SELECT progress_ms
    INTO @prev_progress
    FROM listening_two
    ORDER BY time_stamp DESC
    LIMIT 1;
    SET NEW.progress_ms = @prev_progress;
  END IF;
END;
//
DELIMITER ;

/*
DROP TRIGGER IF EXISTS listening_playlist_fk;
DELIMITER //

CREATE TRIGGER listening_playlist_fk
BEFORE INSERT ON listening_two
FOR EACH ROW
BEGIN
    IF NEW.playlist_fk IS NULL AND NEW.context_type = 'playlist' THEN
        IF EXISTS (
            SELECT 1
            FROM playlists
            WHERE playlists.playlist_id = NEW.playlist_id
        ) THEN
            SET NEW.playlist_fk = (
                SELECT MAX(id)
                FROM playlists
                WHERE playlists.playlist_id = NEW.playlist_id
            );
        ELSE
            SET NEW.playlist_fk = 1;
        END IF;
    END IF;
END;
//
DELIMITER ;
*/

DELIMITER //
CREATE TRIGGER before_insert_playlist_tracks
BEFORE INSERT ON playlist_tracks
FOR EACH ROW
BEGIN
    DECLARE sub VARCHAR(255);
-- fill in pk
    IF NEW.id IS NULL OR NEW.id = '' THEN
        SET NEW.id = CONCAT(NEW.track_id, '_', NEW.playlist_id, '_', NEW.added_at);
    END IF;
-- fill in playlist id if missing
#    IF NEW.playlist_id IS NULL OR NEW.playlist_id = '' THEN
#	SELECT playlist_id INTO sub FROM listening_two WHERE track_id = NEW.track_id LIMIT 1;
#	SET NEW.playlist_id = IFNULL(sub, 'no_playlist_found');
#    END IF;
END;
//
DELIMITER ;