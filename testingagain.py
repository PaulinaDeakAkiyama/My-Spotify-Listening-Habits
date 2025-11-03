from threading import *
from datetime import datetime, timedelta
import time
from oauth import create_spotify_client
from tracker import save_track_to_listening, save_track_to_reference #, get_current_track
from utils import log_to_sql, safe_spotipy_call
from db import engine, artists, albums, track_reference
from sqlalchemy import text, update, insert

sp = create_spotify_client()

start_time = datetime.now()
def tracker():
    try:
        while datetime.now() < start_time + timedelta(hours=12):
            current_track = get_current_track()
            save_track_to_reference(current_track)
            save_track_to_listening(current_track)
            time.sleep(5)
    except Exception as e:
        print(f"Error: {type(e).__name__} {e}")
        log_to_sql('tracker','failed', f"Error: {type(e).__name__} {e}")
    except KeyboardInterrupt:
        print('stopped')

D = Thread(target = tracker, daemon=True)

def main():
    try:
        while True:
            print(f'Spotify song tracker started! {start_time}')
            D.start()

            if datetime.now() > start_time + timedelta(hours=1):

                pass
    except KeyboardInterrupt:
        print('Spotify tracker manually stopped')
    except Exception as e:
        print(f'whoops {e}')


#with engine.connect() as conn:
#    result = conn.execute(text("CALL delete_filler_dates()"))
#    conn.commit()

def get_current_track():
    current_info = safe_spotipy_call(sp.current_playback)
    if not current_info or not current_info.get("is_playing"):
        print(f"{datetime.now()}: no track playing")
        return {}

    artists = []
    albums = {}
    track_ref = {}
    streaming = {}
    current_track_info = {'artists':artists, 'albums':albums, 'track_reference':track_ref, 'listening_two':streaming}

    current_track = current_info.get("item")
    if not current_track:
        return {}
    context = current_info.get("context") or {}
    uri = context.get("uri", "")
    playlist_id_current = uri.split(":")[-1] if context.get("type") == "playlist" else None
    device_info = current_info.get("device", {})

    ar = current_track['artists']
    for a in ar:
        artists.append({
        'artist_id': a['id'],
        'artist_name': a['name']
        # followers, popularity, genres
        })

    al = current_track['album']
    albums.update({
        'album_id': al['id'],
        'album_name': al['name'],
        'artist_id': al['artists'][0]['id'],
        'collab_artist': al['artists'][1]['id'] if len(al['artists']) > 1 else None,
        'release_date': al['release_date'],
        'total_tracks': al['total_tracks'],
        #'album_type': al['album_type']
        # label, popularity
    })

    track_ref.update({
        'track_id': current_track['id'],
        'track_name': current_track["name"],
        'album_id': al['id'],
        'artist_id': ar[0]['id'],
        'collab_artist': ar[1]['id'] if len(ar) > 1 else None
    })

    streaming.update({
        'progress_ms': current_info['progress_ms'],
        'duration_ms': current_track['duration_ms'],
        'track_id': current_track['id'],
        'device_name': device_info['name'],
        #'device_type': device_info['type']
        'volume_percentage': device_info['volume_percent'],
        'popularity': current_track['popularity'],
        'playlist_id': playlist_id_current,
    })

    return current_track_info

t = get_current_track()

print(t['listening_two'])

def insert_into_sql(table_name, track_info):
    data = track_info[table_name]
    with engine.begin() as conn:
        conn.execute(insert(table_name), data)


def update_artists():
    with engine.begin() as conn:
        artist_id = conn.execute(text('SELECT artist_id FROM artists ORDER BY inserted_at LIMIT 1')).scalar()
    data = safe_spotipy_call(sp.artist, artist_id)
    info = {'followers':data['followers']['total'],'genres':data['genres'] ,'popularity':data['popularity']}
    print(info)
    with engine.begin() as conn:
        conn.execute(
         update(artists)
         .where(artists.c.artist_id == artist_id)
         .values(info)
        )

def update_albums():
    with engine.begin() as conn:
        album_id = conn.execute(text('SELECT album_id FROM albums ORDER BY inserted_at LIMIT 1')).scalar()
    data = safe_spotipy_call(sp.album, album_id)
    info = {'label':data['label'],'popularity':data['popularity']}
    print(info)
    with engine.begin() as conn:
        conn.execute(
         update(albums)
         .where(albums.c.album_id == album_id)
         .values(info)
        )

