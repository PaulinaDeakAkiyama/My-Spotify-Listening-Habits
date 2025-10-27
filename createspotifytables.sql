CREATE DATABASE IF NOT EXISTS MySpotify;
USE MySpotify;

CREATE TABLE IF NOT EXISTS playlists (
    playlist_id VARCHAR(50) PRIMARY KEY,
    playlist_name VARCHAR(50) NOT NULL,
    owner_id VARCHAR(50) NOT NULL,
    total_tracks INT NOT NULL,
    timestamp TIMESTAMP DEFAULT current_timestamp
 );
 
 CREATE TABLE IF NOT EXISTS my_tracks (
    id VARCHAR(50) PRIMARY KEY,
	track_id VARCHAR(50) NOT NULL,
    track_name VARCHAR(255) NOT NULL,
    album VARCHAR(255) NULL,
    artist_name VARCHAR(255) NOT NULL,
    collab_artist VARCHAR(255) NULL,
    playlist_id VARCHAR(50) NULL,
    added_at DATETIME NOT NULL,
    added_by VARCHAR(50) NULL,
    timestamp TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS listening_history(
    date_played DATETIME(4) PRIMARY KEY,
    track_id VARCHAR(50) NOT NULL,
    track_name VARCHAR(255) NOT NULL,
    duration_ms INT NOT NULL,
    popularity INT,
    timestamp TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS listening_stream(
   id INT PRIMARY KEY AUTO_INCREMENT, 
   track_id VARCHAR(50) NOT NULL,
   track_name VARCHAR(255) NOT NULL,
   date_started DATETIME(4) UNIQUE,
   start_time TIME(3) NOT NULL,
   progress_ms INT NOT NULL,
   duration_ms INT NOT NULL,
   end_time TIME(3) NOT NULL,
   popularity INT NULL,
   playlist_id VARCHAR(50) NULL DEFAULT NULL,
   volume_percentage INT,
   device_name VARCHAR(30)
);  


CREATE TABLE IF NOT EXISTS listening_two(
   timestamp TIMESTAMP PRIMARY KEY DEFAULT current_timestamp,
   progress_ms INT NULL,
   duration_ms INT NULL,
   track_id VARCHAR(50) NULL,
   track_name VARCHAR(255) NULL,
   playlist_id VARCHAR(50) NULL,
   popularity INT NULL,
   device_name VARCHAR(30) NULL,
   volume_percentage INT NULL
);  
CREATE TABLE IF NOT EXISTS listening_copy  AS (SELECT * FROM listening_two);


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
    timestamp TIMESTAMP DEFAULT current_timestamp
    );
      
 CREATE TABLE IF NOT EXISTS logging(
    id INTEGER PRIMARY KEY auto_increment,
    message TEXT,
    level VARCHAR(30),
    stage VARCHAR(30),
    status VARCHAR(30),
    timestamp TIMESTAMP DEFAULT current_timestamp
 )
 
 select * from logging;

    