USE myspotify;
SET GLOBAL event_scheduler = ON;

DROP PROCEDURE IF EXISTS clean_filler_dates;

DELIMITER $$

CREATE PROCEDURE delete_filler_dates()
BEGIN
	WITH changes AS(
		SELECT 
			'timestamp', 
			track_name,
			CASE WHEN (track_name IS NULL AND LAG(track_name) OVER (ORDER BY timestamp) IS NULL)
			OR track_name = LAG(track_name) OVER (ORDER BY timestamp)
				THEN 0 ELSE 1
			END AS is_new_group
		FROM listening_copy
	),
	grouped AS(
		SELECT 
			'timestamp',
			track_name,
			SUM(is_new_group) OVER (ORDER BY timestamp ROWS UNBOUNDED PRECEDING) AS group_id
		FROM changes    
	),
	start_end AS(
		SELECT
		  MIN('timestamp') AS start_time,
		  MAX('timestamp') AS end_time,
		  track_name
		FROM grouped
		GROUP BY group_id, track_name
		ORDER BY start_time
	)

	DELETE listening_copy
	FROM listening_copy
	WHERE timestamp  NOT IN (SELECT start_time FROM start_end) 
        AND timestamp NOT IN (SELECT end_time FROM start_end);
END$$

DELIMITER ;


	   
		