from threading import *
from datetime import datetime, timedelta, timezone
import time
from oauth import create_spotify_client
from utils import log_to_sql, insert_into_sql, safe_spotipy_call
from db import artists, albums, track_reference, listening_two
from tracker import get_current_track, update_albums, update_artists, deal_with_artists_albums_reference
#from SCDplaylistsupdate import update_tracks_and_playlists
from logger import log

sp = create_spotify_client()

track_ref = []
tracks = safe_spotipy_call(sp.album_tracks, '7ENUU3mZBPsGVLmwoj1Sk5', limit=50)
for track in tracks['items']:
    print(track)
    print(track.keys())
    track_ref.append({
        'track_id': track['id'],
        'track_name': track["name"],
        'album_id': '7ENUU3mZBPsGVLmwoj1Sk5',
        'artist_id': track['artists'][0]['id'],
        'collab_artist': track['artists'][1]['id'] if len(track['artists']) > 1 else None
        #'duration_ms': track['duration_ms']
    })
    print(track_ref)