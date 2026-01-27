# My-Spotify-Listening-Habits

[![Repo size](https://img.shields.io/github/repo-size/PaulinaDeakAkiyama/My-Spotify-Listening-Habits)](https://github.com/PaulinaDeakAkiyama/My-Spotify-Listening-Habits)
[![Language: Python](https://img.shields.io/badge/language-Python-blue)]()
[![License: MIT](https://img.shields.io/badge/license-MIT-green)]()

In this project I have created a spotify listening habits tracker in order to automate playlist creation, playlist personalisation and produce the popular spotify 'wrapped' feature regularly. 
Using the spotipy and reccobeats API, I create a pipeline to collect data and to regularly update sql tables. Then I conduct analysis on my listening habits and playlist vibes and make changes to my playlists. The project is a set of Python daemons and pipelines that run against a MySQL database (no web server).

TL;DR
- Clone, populate a MySQL database named `MySpotify` with the schema and stored procedure used by the project, fill .env, then:
  - python main.py — run the streaming tracker daemon
  - python populatetables.py — fetch playlists and merge playlist contents
  - python audiofeatures.py — run audio-features pipeline (requires ffmpeg and a deezer app) 
  - python tableupdate.py — playlist / my_tracks utils

Features
- Streaming daemon: continuously tracks currently playing songs and inserts listening events
- Playlist Albums population pipeline: fetches playlist contents and merges into SQL (merge_playlist_contents stored procedure) to update slowly changing dimension tables playlist(type 2) and playlist_tracks(type 1). After merging playlists, this pipeline inserts album contents.
- Audio features pipeline using reccobeats (primary) with Deezer previews + ffmpeg fallback for missing features. *update: deezer now provides 2FA but new apps cannot be created at the moment, so my audio features pipeline is momentarily unusable*
- Artist and album metadata enrichment.
- Resilient API wrappers: handles Spotify 429 rate limits and retries.

Why this looks impressive
- It's an end-to-end data pipeline (streaming daemon, ETL pipelines, enrichment and analysis).
- Integrates multiple APIs (Spotify, reccobeats, Deezer for previews).
- Shows production-minded patterns: safe API wrappers, retry/backoff for rate limits, SQL stored-procedure integration, and logging to SQL.
- Extensible: clear separation of entrypoints and helper modules for reuse and testing.

# Data workflow for the streaming daemon, and the update playlists daemon:
<img width="1548" height="980" alt="myspotifytracker2-Page-1 drawio (1)" src="https://github.com/user-attachments/assets/b8e9d6ed-b90d-4ab2-af17-9c29e4566fd1" />

# Data pipeline for getting audio features. As this endpoint has been depracated from the spotify api, I've used reccobeats and deezer:
<img width="866" height="820" alt="Untitled Diagram drawio" src="https://github.com/user-attachments/assets/68562c67-258d-4df5-bd6a-12103c05c263" />

# Structure of the MySpotify relational database:
<img width="975" height="824" alt="sqlschema" src="https://github.com/user-attachments/assets/4b7d17eb-49f1-420c-8eed-dc975ca12113" />

Next steps:
- Using airflow and a Virtual machine, streaming can run 24/7 optimally, and regular table updates can be scheduled.
- Creating a docker file to create better reproduceability
- Creating a dashboard with tableau or power bi to visualise listening habits over time, most listened to genres, artists etc.

Quickstart (development)
1. Clone
   git clone https://github.com/PaulinaDeakAkiyama/My-Spotify-Listening-Habits.git
   cd My-Spotify-Listening-Habits

2. Python environment
   python -m venv .venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
   pip install -r requirements.txt

   If you don't have a requirements.txt yet:
   pip install spotipy sqlalchemy mysql-connector-python requests pillow python-dotenv

3. Database
   - Create a MySQL database named `MySpotify` (or update db.py if you prefer a different name).
   - Run the SQL schema and stored-procedure script (add these to SQLfiles/ for reproducibility).

4. Add environment variables
   - Copy `.env.example` -> `.env` and fill values:
     SPOTIFY_CLIENT_ID="..."
     SPOTIFY_CLIENT_SECRET="..."
     REDIRECT_URI="http://localhost:8888/callback"
     SQL_USER="dbuser"
     SQL_PASSWORD="dbpassword"
     SQL_HOST="127.0.0.1"

   - Recommended: use python-dotenv (config/settings.py example is included in repo suggestions).

5. Install ffmpeg (required for audiofeatures pipeline)
   - Mac (homebrew): brew install ffmpeg
   - Ubuntu: sudo apt-get install ffmpeg


Environment variables (from .env.example)
- SPOTIFY_CLIENT_ID
- SPOTIFY_CLIENT_SECRET
- REDIRECT_URI
- SQL_USER
- SQL_PASSWORD
- SQL_HOST

Spotify scopes required (see oauth.py)
- user-library-read
- user-read-recently-played
- user-read-currently-playing
- user-read-playback-state
- playlist-read-private
- playlist-modify-public
- playlist-modify-private

How to run (entry scripts)
- python main.py
  - Starts the streaming tracker (background thread) that polls current playback and inserts rows into listening_two.

- python populatetables.py
  - Fetches playlist contents, builds track_reference and uses the DB stored procedure merge_playlist_contents to merge playlist data.

- python tableupdate.py
  - Utilities to populate my_tracks and update playlists; contains helpers to iterate playlists and update track rows.

- python audiofeatures.py
  - Full audio-features pipeline:
    1) identify tracks missing features,
    2) use reccobeats to fetch features,
    3) fallback to Deezer previews, convert with ffmpeg to WAV,
    4) upload WAVs to reccobeats analysis and insert track_features.

- python oauth.py
  - Convenience script that creates Spotify client and prints the oauth header for debugging.

What each main script does (quick summary)
- main.py: orchestrates the tracker thread and periodic table updates; logs to the logging table.
- tracker.py: implements get_current_track() and helpers to ensure artists/albums/track_reference exist before persisting listening events.
- populatetables.py: playlist ingestion pipeline; handles pagination, missing tracks, and calls merge_playlist_contents stored procedure.
- tableupdate.py: helper utilities for playlist/table maintenance.
- audiofeatures.py: staged pipeline to insert audio features into track_features; includes parallelism and retry logic.

Database notes
- db.py autoloads tables (SQLAlchemy) — your MySQL schema must exist prior to running scripts.
- merge_playlist_contents stored procedure is used by populatetables.py — include the SQL for this procedure in SQLfiles/ so other developers can reproduce your environment.
- Suggestion: export a schema.sql or a MySQL dump and add it to SQLfiles/.

Dependencies & system requirements
- Python 3.8+ (3.9+ recommended)
- MySQL server accessible to the app
- ffmpeg (system binary)
- Recommended Python packages (requirements.txt):
  spotipy
  sqlalchemy
  mysql-connector-python
  requests
  pillow
  python-dotenv
     
