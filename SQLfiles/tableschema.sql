CREATE DATABASE IF NOT EXISTS MySpotify;
USE MySpotify;

CREATE TABLE IF NOT EXISTS playlists (
    id INT AUTO_INCREMENT PRIMARY KEY,
    playlist_id VARCHAR(50) UNIQUE,
    playlist_name VARCHAR(50) NOT NULL,
    owner_id VARCHAR(50) NOT NULL,
    total_tracks INT NOT NULL,
    valid_to DATETIME DEFAULT '3000-01-01 01:00:00' NOT NULL,
    Valid_from DATETIME DEFAULT CURRENT_TIMESTAMP, #ON UPDATE CURRENT_TIMESTAMP,
    saved BOOLEAN
 );
select * from playlists;


UPDATE playlists p
JOIN (SELECT playlist_id, MAX(added_at) AS ma FROM playlist_tracks GROUP BY playlist_id) m
ON m.playlist_id = p.playlist_id
SET p.valid_from = m.ma;


 CREATE TABLE IF NOT EXISTS playlist_tracks (
    pk INT PRIMARY KEY AUTO_INCREMENT,
    id VARCHAR(255),
	track_id VARCHAR(50) NOT NULL,
    playlist_id VARCHAR(50) NULL,
    playlist_fk INT,
    added_at DATETIME NULL,
    added_by VARCHAR(50) NULL,
    downloaded BOOLEAN,
    timestamp TIMESTAMP DEFAULT current_timestamp,
    valid BOOLEAN,
    CONSTRAINT FOREIGN KEY (track_id) REFERENCES track_reference(track_id)
);



UPDATE playlist_tracks SET playlist_fk = (SELECT id FROM playlists WHERE playlists.playlist_id = playlist_tracks.playlist_id);


CREATE TABLE IF NOT EXISTS listening_two(
   time_stamp TIMESTAMP PRIMARY KEY DEFAULT current_timestamp,
   start_time TIMESTAMP NOT NULL,
   progress_ms INT NULL,
   duration_ms INT NULL,
   track_id VARCHAR(50) NULL,
   context_id VARCHAR(50) NULL,
   context_type VARCHAR(50),
   playlist_fk INT,
   popularity INT NULL,
   device_name VARCHAR(30) NULL,
   volume_percentage INT NULL,
   is_new_group BOOLEAN NOT NULL,
   CONSTRAINT FOREIGN KEY (playlist_fk) REFERENCES playlists(id),
   CONSTRAINT FOREIGN KEY (track_id) REFERENCES track_reference(track_id)
);


UPDATE listening_two l
SET l.start_time = (SELECT DATE_SUB(time_stamp, INTERVAL progress_ms*1000 MICROSECOND) AS start_time
                    FROM listening_two c
                    WHERE c.start_time = '0000-00-00 00:00:00' AND track_id IS NOT NULL);

UPDATE listening_two SET playlist_fk = (SELECT id FROM playlists WHERE playlists.playlist_id = listening_two.playlist_id ) WHERE playlist_fk IS NULL;
UPDATE listening_two SET playlist_fk = 1 WHERE listening_two.playlist_id NOT IN (SELECT playlist_id FROM playlists);

#ALTER TABLE listening_two
#ADD FOREIGN KEY (track_id) REFERENCES track_reference(track_id);


CREATE TABLE IF NOT EXISTS track_features(
    track_id VARCHAR(50) PRIMARY KEY,
    reccobeats_id VARCHAR(50) NULL,
    acousticness INT NULL,
    danceability INT NULL,
    energy INT NULL,
    instrumentalness INT NULL,
    key_ INT NULL,
    loudness INT NULL,
    mode_ INT NULL,
    speechiness INT NULL,
    tempo INT NULL,
    valence INT NULL,
    timestamp TIMESTAMP DEFAULT current_timestamp,
    CONSTRAINT FOREIGN KEY (track_id) REFERENCES track_reference(track_id)
    );



CREATE TABLE IF NOT EXISTS track_reference(
    track_id VARCHAR(50) PRIMARY KEY,
    track_name VARCHAR(255) NOT NULL,
    album_id VARCHAR(50) NULL,
    artist_id VARCHAR(50) NULL,
    collab_artist VARCHAR(255) NULL,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT FOREIGN KEY (album_id) REFERENCES albums(album_id),
    CONSTRAINT FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
    CONSTRAINT FOREIGN KEY (collab_artist) REFERENCES artists(artist_id)
    );

INSERT INTO track_reference VALUES('deleted_1', 'unknown', NULL, NULL, NULL);


CREATE TABLE IF NOT EXISTS albums(
    album_id VARCHAR(50) PRIMARY KEY,
    album_name VARCHAR(255),
    album_type VARCHAR(50),
    total_tracks INT,
    release_date VARCHAR(50),
    label VARCHAR(255),
    popularity INT,
    artist_id VARCHAR(50),
    collab_artist VARCHAR(50),
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
    CONSTRAINT FOREIGN KEY (collab_artist) REFERENCES artists(artist_id)
);

CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR(50) PRIMARY KEY,
    artist_name VARCHAR(255),
    followers INT,
    popularity INT,
    genres JSON,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

CREATE TABLE IF NOT EXISTS listening_history(
    track_id VARCHAR(50),
    popularity INT,
    date_played DATETIME PRIMARY KEY,
    duration_ms INT,
    context_id VARCHAR(50),
    context_type VARCHAR(50),
    downloaded BOOLEAN
    );

CREATE TABLE deleted_tracks(
		 pk INT AUTO_INCREMENT PRIMARY KEY,
		 added_at DATETIME,
		 added_by VARCHAR(50),
		 num_tracks INT,
		 playlist_id VARCHAR(50),
		 downloaded BOOLEAN);

CREATE TABLE IF NOT EXISTS logging(
	id INTEGER PRIMARY KEY auto_increment,
	message TEXT,
	level VARCHAR(30),
	stage VARCHAR(30),
	status VARCHAR(30),
	timestamp TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS procedure_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    procedure_name VARCHAR(100),
    playlist_id VARCHAR(100),
    action_taken VARCHAR(100),
    message TEXT,
    status ENUM('SUCCESS', 'FAILURE', 'NO_CHANGE'),
    log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

SELECT * FROM procedure_log;
 # fact table start end time calcutaled more accuratly
 select * from logging;


    
