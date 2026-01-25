USE myspotify;

DROP PROCEDURE IF EXISTS clean_and_backup_listening;
DELIMITER $$

CREATE PROCEDURE clean_and_backup_listening()
BEGIN
    START TRANSACTION;
    DELETE l
    FROM listening_two AS l
    JOIN (
        SELECT
            start_time,
            MAX(time_stamp) AS max_ts
        FROM listening_two
        GROUP BY start_time
    ) x
      ON l.start_time = x.start_time
    WHERE l.time_stamp <> x.max_ts;
    
    UPDATE listening_two
	SET progress_ms = TIMESTAMPDIFF(SECOND, start_time, time_stamp)*1000 
	WHERE track_id IS NULL
	AND progress_ms IS NULL;

    INSERT INTO listening_backup (
        time_stamp,
        start_time,
        progress_ms,
        duration_ms,
        track_id,
        context_id,
        context_type,
        playlist_fk,
        popularity,
        device_name,
        volume_percentage,
        is_new_group
    )
    SELECT
        t.time_stamp,
        t.start_time,
        t.progress_ms,
        t.duration_ms,
        t.track_id,
        t.context_id,
        t.context_type,
        t.playlist_fk,
        t.popularity,
        t.device_name,
        t.volume_percentage,
        t.is_new_group
    FROM listening_two t
    WHERE NOT EXISTS (
        SELECT 1
        FROM listening_backup b
        WHERE b.time_stamp = t.time_stamp
    );

    COMMIT;
END$$
DELIMITER ;



DROP PROCEDURE IF EXISTS fill_pause_time;
DELIMITER $$

CREATE PROCEDURE fill_pause_time()
BEGIN
    UPDATE listening_two 
    SET progress_ms = (
        SELECT TIMESTAMPDIFF(SECOND, time_stamp, progress_ms)/1000 
        FROM listening_two
        )
    WHERE progress_ms IS NULL 
    AND track_id IS NULL;

END$$
DELIMITER ;


#view
#add logging, change to only past hour worth of rows? only end time? procedure for getting all tracks since last seen date in listneing two and adjusting paused time



DROP PROCEDURE IF EXISTS merge_playlist_contents;
DELIMITER $$

CREATE PROCEDURE merge_playlist_contents(IN playlist_contents JSON,
                                         IN var_playlist_id VARCHAR(50),
                                         IN playlist_name VARCHAR(255),
                                         IN owner_id VARCHAR(50),
                                         IN total_tracks INT,
                                         IN track_ref JSON
                                         )
BEGIN
	
    TRUNCATE TABLE json_track_ref;
    INSERT INTO json_track_ref
    SELECT * 
    FROM JSON_TABLE(
        track_ref,
        '$[*]' COLUMNS(
                track_id VARCHAR(50) PATH '$.track_id',
                track_name VARCHAR(255) PATH '$.track_name',
                album_id VARCHAR(50) PATH '$.album_id',
                artist_id VARCHAR(50) PATH '$.artist_id',
                collab_artist VARCHAR(50) PATH '$.collab_artist'
                )
			) AS g;
    
    SET @new_tracks = ROW_COUNT();  
    SELECT concat('new tracks:', @new_tracks);

	IF @new_tracks > 0 THEN 
		 INSERT INTO artists(artist_id)
		 with cte as( 
					SELECT artist_id 
					FROM json_track_ref 
					UNION 
					SELECT collab_artist 
					FROM json_track_ref 
			)
		 SELECT c.* 
		 FROM cte c
		 LEFT JOIN artists a ON a.artist_id = c.artist_id
		 WHERE a.artist_id IS NULL
         AND c.artist_id IS NOT NULL;
				
		 INSERT INTO albums (album_id)
		 SELECT DISTINCT j.album_id 
		 FROM json_track_ref j
		 LEFT JOIN albums a ON a.album_id = j.album_id
		 WHERE a.album_id IS NULL;
		 
		 INSERT INTO track_reference(track_id, track_name, album_id, artist_id, collab_artist)
         SELECT * FROM json_track_ref;
    END IF;     
    
    TRUNCATE TABLE json_tracks;
    INSERT INTO json_tracks  
    SELECT * 
    FROM JSON_TABLE(
        playlist_contents,
        '$[*]' COLUMNS(
                track_id VARCHAR(50) PATH '$.track_id',
                playlist_id VARCHAR(50) PATH '$.playlist_id',
                added_at DATETIME PATH '$.added_at',
                added_by VARCHAR(50) PATH '$.added_by',
                downloaded BOOLEAN PATH '$.downloaded'
            )
        ) AS jt;    
        
     INSERT INTO deleted_tracks(added_at, added_by, num_tracks, playlist_id, downloaded)
         SELECT MAX(added_at), added_by, COUNT(*) AS c, playlist_id, downloaded FROM json_tracks
         WHERE track_id LIKE 'deleted%'
         GROUP BY playlist_id, added_by, downloaded;
		 
         SET @deleted = ROW_COUNT();
         SELECT CONCAT('number of deleted tracks from this playlist:',@deleted);   
        
--     create new row in playlist tracks for newly added songs
     INSERT INTO playlist_tracks (playlist_id, track_id, added_at, added_by, downloaded, playlist_fk, valid)
     SELECT t.playlist_id,
            t.track_id,
            t.added_at,
            t.added_by,
            t.downloaded,
            1,
            1
     FROM json_tracks t
     LEFT JOIN playlist_tracks p
   # ON p.id = CONCAT(t.track_id,'_', t.playlist_id,'_', t.added_at)
     ON p.track_id = t.track_id 
	 AND p.playlist_id = t.playlist_id
     AND p.added_at = t.added_at
     AND p.valid = 1
     WHERE p.id IS NULL 
     AND t.track_id NOT LIKE 'deleted%';
     
     SET @inserted_rows = ROW_COUNT();
     SELECT CONCAT('number of inserted rows:', @inserted_rows);
    
--     invalidate removed tracks and update downloaded and added_by
     UPDATE playlist_tracks p
     LEFT JOIN json_tracks t
     ON p.id = CONCAT(t.track_id, '_', t.playlist_id,'_', t.added_at)
     SET p.valid = CASE WHEN t.track_id IS NULL
                        THEN FALSE
                        ELSE TRUE END,
         p.downloaded = COALESCE(t.downloaded, p.downloaded),
         p.added_by = COALESCE(t.added_by, p.added_by)
	 WHERE p.playlist_id = var_playlist_id 
     AND valid = True
     AND
           (t.track_id IS NULL              
           OR t.downloaded != p.downloaded
           OR t.added_by != p.added_by); 
           
     SET @updated_rows = ROW_COUNT(); 
	 SELECT CONCAT('number of updated rows:', @updated_rows);
     
--     if something got inserted or updated in playlist_tracks update playlists version   
     SET @valid_from = (SELECT MAX(added_at) FROM json_tracks);  
     IF @inserted_rows > 0 OR @updated_rows > 0 THEN
         UPDATE playlists SET valid_to = @valid_from 
         WHERE playlist_id = var_playlist_id 
         AND valid_to = '3000-01-01 01:00:00';
         
		 INSERT INTO playlists (playlist_id, playlist_name, owner_id, total_tracks, valid_from)
			  VALUES (var_playlist_id, playlist_name, owner_id, total_tracks, @valid_from);
		
         UPDATE playlist_tracks 
         SET playlist_fk = (SELECT MAX(id) FROM playlists WHERE playlist_id = var_playlist_id)
         WHERE valid = TRUE 
         AND playlist_id = var_playlist_id;
     END IF;
     
     UPDATE listening_two l
		 SET playlist_fk = (SELECT MAX(id) FROM playlists WHERE playlist_id = var_playlist_id) 
		 WHERE playlist_fk = 1
		 AND context_id = var_playlist_id; 
		
     #DROP TABLE json_track_ref;   
     #DROP TABLE json_tracks;
END$$
	
