# My-Spotify-Listening-Habits
In this project I have created a spotify listening habits tracker in order to automate playlist creation, playlist personalisation and produce the popular spotify 'wrapped' feature regularly. 
Using the spotipy and reccobeats API, I create a pipeline to collect data and to regularly update sql tables. Then I conduct analysis on my listening habits and playlist vibes and make changes to my playlists. The project is a set of Python daemons and pipelines that run against a MySQL database (no web server).

Features
- Streaming daemon: continuously tracks currently playing songs and inserts listening events
- Playlist Albums population pipeline: fetches playlist contents and merges into SQL (merge_playlist_contents stored procedure) and inserts album contents
- Audio-features pipeline: uses reccobeats and Deezer previews when needed to populate track_features
- Automatic artist/album metadata enrichment (using Spotify API)

Table of contents
- Prerequisites
- Setup
- Environment variables
- How to run (scripts / entrypoints)
- What each script does (details)
- Database / schema notes
- Troubleshooting
- Contributing

Data workflow for the streaming daemon which tracks listening habits, and the update playlists daemon which inserts playlist contents and updates the slowly changing dimension playlists table.
<img width="1548" height="980" alt="myspotifytracker2-Page-1 drawio (1)" src="https://github.com/user-attachments/assets/b8e9d6ed-b90d-4ab2-af17-9c29e4566fd1" />

Data pipeline for getting audio features. As this endpoint has been depracated from the spotify api, ive used reccobeats and deezer.
<img width="866" height="820" alt="Untitled Diagram drawio" src="https://github.com/user-attachments/assets/68562c67-258d-4df5-bd6a-12103c05c263" />

Structure of the MySpotify relational database 
<img width="975" height="824" alt="sqlschema" src="https://github.com/user-attachments/assets/4b7d17eb-49f1-420c-8eed-dc975ca12113" />

next steps
Using airflow and AWS cloud services, streaming can run 24/7 optimally, and regular table updates can be scheduled.
creating a dashboard with tableau or power bi to visualise listening habits over time, most listened to genres, artists etc.
