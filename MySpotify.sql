CREATE DATABASE IF NOT EXISTS MySpotify;
USE MySpotify;

CREATE TABLE IF NOT EXISTS listening_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    track_id VARCHAR(50) NOT NULL,
    track_name VARCHAR(255) NOT NULL,
    artist_id VARCHAR(255) NOT NULL,
    artist VARCHAR(255) NOT NULL,
    album_id VARCHAR(255) NOT NULL,
    album VARCHAR(255) NOT NULL,
    genre VARCHAR(255),
    playlist_id VARCHAR(255),
    popularity INT,
    date_played DATETIME DEFAULT CURRENT_TIMESTAMP
);
