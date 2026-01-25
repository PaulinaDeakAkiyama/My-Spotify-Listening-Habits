
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
