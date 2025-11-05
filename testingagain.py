from threading import *
from datetime import datetime, timedelta
import time
from oauth import create_spotify_client
from utils import log_to_sql, safe_spotipy_call
from db import engine, artists, albums, track_reference, listening_two
from sqlalchemy import text, update, insert, select
from sqlalchemy.dialects.mysql import insert as mysql_insert

sp = create_spotify_client()

start_time = datetime.now()

#with engine.connect() as conn:
#    result = conn.execute(text("CALL delete_filler_dates()"))
#    conn.commit()

def get_current_track():
    current_info = safe_spotipy_call(sp.current_playback)
    if not current_info or not current_info.get("is_playing"):
        print(f"{datetime.now()}: no track playing")
        return {}

    artists = {}
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
        artists.update({
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
        #'device_type': device_info['type'],
        'volume_percentage': device_info['volume_percent'],
        'popularity': current_track['popularity'],
        'playlist_id': playlist_id_current,
    })

    return current_track_info

def insert_into_sql(table_name, track_info):
    if not track_info:
        data = {}
        with engine.begin() as conn:
            conn.execute(insert(table_name), data)
    else:
        data = track_info[table_name.name]
        stmt = mysql_insert(table_name).values(**data)
        stmt = stmt.on_duplicate_key_update(**{k: stmt.inserted[k] for k in data.keys()})
        with engine.begin() as conn:
            conn.execute(stmt)
        print(f'saved track to {table_name}! {track_info[table_name.name]}')

def update_artists():
    with engine.begin() as conn:
        artist_id = conn.execute(select(artists.c.artist_id).order_by(artists.c.inserted_at).limit(1)).scalar()
    data = safe_spotipy_call(sp.artist, artist_id)
    info = {'artist_id':artist_id,'followers': data['followers']['total'], 'genres': data['genres'], 'popularity': data['popularity']}
    stmt = mysql_insert(artists).values(**info)
    stmt = stmt.on_duplicate_key_update(**{k: stmt.inserted[k] for k in info.keys()})
    with engine.begin() as conn:
        conn.execute(stmt)


def update_albums():
    with engine.begin() as conn:
        album_id = conn.execute(select(albums.c.album_id).order_by(albums.c.inserted_at).limit(1)).scalar()
    data = safe_spotipy_call(sp.album, album_id)
    info = {'album_id':album_id,'label':data['label'],'popularity':data['popularity']}
    stmt = mysql_insert(albums).values(**info)
    stmt = stmt.on_duplicate_key_update(
        **{k: stmt.inserted[k] for k in info.keys()}
    )
    with engine.begin() as conn:
        conn.execute(stmt)


def tracker():
    print('tracker started')
    try:
        while datetime.now() < start_time + timedelta(hours=12):
            current_track = get_current_track()
            if not current_track:
                insert_into_sql(listening_two, current_track)
                print(f'saved track!! {current_track} {datetime.now()}')
                time.sleep(5)
                continue
            print(f'\n{datetime.now()}: playing: {current_track['track_reference']['track_name']}\n {current_track}')
            insert_into_sql(artists, current_track)
            update_artists()
            insert_into_sql(albums, current_track)
            update_albums()
            insert_into_sql(track_reference, current_track)
            insert_into_sql(listening_two, current_track)
            time.sleep(5)
    except Exception as e:
        print(f"hehe Error: {type(e).__name__} {e}")
        log_to_sql('tracker','failed', f"Error: {type(e).__name__} {e}")
    except KeyboardInterrupt:
        print('stopped')


def main():
    print(f'Spotify song tracker started! {start_time}')
    try:
        D = Thread(target=tracker, daemon=True)
        D.start()
        while True:
            print('main running')
            time.sleep(10)
    except KeyboardInterrupt:
        print('Spotify tracker manually stopped')
    except Exception as e:
        print(f'whoops {e}')


if __name__ == '__main__':
    main()