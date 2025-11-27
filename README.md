# My-Spotify-Listening-Habits
Using the spotipy and reccobeats API, I create a pipeline to collect data and to regularly update sql tables. Then I conduct analysis on my listening habits and playlist vibes and automate updates and changes to my playlists. 

This is the data workflow for the streaming daemon which tracks listening habits, and the update playlists daemon which inserts playlist contents and updates the slowly changing dimension playlists table.
<img width="1548" height="980" alt="myspotifytracker2-Page-1 drawio (1)" src="https://github.com/user-attachments/assets/b8e9d6ed-b90d-4ab2-af17-9c29e4566fd1" />

This is the data pipeline for getting audio features. As this endpoint has been depracated from the spotify api, ive used reccobeats and deezer.
<img width="866" height="820" alt="Untitled Diagram drawio" src="https://github.com/user-attachments/assets/68562c67-258d-4df5-bd6a-12103c05c263" />

The structure of my spotify relational database 
<img width="975" height="824" alt="sqlschema" src="https://github.com/user-attachments/assets/4b7d17eb-49f1-420c-8eed-dc975ca12113" />
