from threading import *
from datetime import datetime, timedelta, timezone
import time
from oauth import create_spotify_client
from utils import log_to_sql, insert_into_sql, safe_spotipy_call, get_existing_ids
from db import artists, albums, track_reference, listening_two, engine
from tracker import get_current_track, update_albums, update_artists, deal_with_artists_albums_reference
#from SCDplaylistsupdate import update_tracks_and_playlists
from logger import log
from sqlalchemy import select


sp = create_spotify_client()

info = {'artist_id': '0spHbv2fw49lDMkbOAdaqX', 'artist_name': 'WWE', 'artist_followers': 702350, 'artist_genres': [], 'artist_popularity': 64}

# track_ref = []
# tracks = safe_spotipy_call(sp.album_tracks, '7ENUU3mZBPsGVLmwoj1Sk5', limit=50)
# for track in tracks['items']:
#     print(track)
#     print(track)
#     print(track.keys())
#     track_ref.append({
#         'track_id': track['id'],
#         'track_name': track["name"],
#         'album_id': '7ENUU3mZBPsGVLmwoj1Sk5',
#         'artist_id': track['artists'][0]['id'],
#         'collab_artist': track['artists'][1]['id'] if len(track['artists']) > 1 else None
#         'duration_ms': track['duration_ms']
    # })
    # print(track_ref)
#
# with engine.begin() as conn:
#     ids = conn.execute(select(albums.c.album_id).limit(5)).scalars().all()
#
# corresponding = safe_spotipy_call(sp.albums, ids)
# for c in corresponding.get('albums'):
#     tracks = c['tracks']
#     for track in tracks['items']:
#         print(track['name'])