from oauth import create_spotify_client
from datetime import datetime, timezone, UTC
from sqlalchemy import insert, select, func
from db import engine, listening_two, track_reference, artists, albums, listening_history
from utils import  safe_spotipy_call
from sqlalchemy import text, update, insert, select
from sqlalchemy.dialects.mysql import insert as mysql_insert
sp = create_spotify_client()
from logger import log
import spotipy
import time

def get_current_track():
    current_info = safe_streaming_sp_call(sp.current_playback)
    if not current_info or not current_info.get("is_playing"):
        log.info(f"{datetime.now()}: no track playing")
        return {'listening_two':{'track_id': None}, 'track_reference':{'track_name': None}}

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
    album_id_current = uri.split(":")[-1] if context.get("type") == "album" else None
    artist_id_current = uri.split(":")[-1] if context.get("type") == "artist" else None
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
        'context_id': playlist_id_current or album_id_current or artist_id_current,
        'context_type': context['type']
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
        log.info(f'saved track to {table_name}! {track_info[table_name.name]}')

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


def save_last_50_tracks(after_ts):
    """loops from a specified datetime in milliseconds until there are no more tracks in current user recently played"""
    while True:

        with engine.connect() as conn:
            result = conn.execute(select(func.max(listening_history.c.date_played)))
            last_date_played = result.scalar()

        recent = safe_spotipy_call(sp.current_user_recently_played, limit=50, after=after_ts)
        items = recent.get("items", [])
        if not items:
            log.info('finished getting previous tracks')
            break

        new_tracks = []
        for item in items:
            downloaded = item['track']['is_local']
            track_id = item['track']['id']
            popularity = item['track']['popularity']
            date_played = datetime.fromisoformat(item['played_at'])
            naieve_date_played = date_played.replace(tzinfo=None)
            duration_ms = item['track']['duration_ms']
            context = item.get("context") or {}
            uri = context.get("uri", "")
            playlist_id_current = uri.split(":")[-1] if context.get("type") == "playlist" else None
            album_id_current = uri.split(":")[-1] if context.get("type") == "album" else None
            artist_id_current = uri.split(":")[-1] if context.get("type") == "artist" else None


            if (last_date_played is None) or (naieve_date_played > last_date_played):
                new_tracks.append({
                    'track_id' : track_id,
                    'popularity' : popularity,
                    'date_played' : date_played,
                    'duration_ms' : duration_ms,
                    'context_id': playlist_id_current or album_id_current or artist_id_current,
                    'context_type': context['type'],
                    'downloaded': downloaded
                })

        if new_tracks:
           try:
               with engine.begin() as conn:
                   conn.execute(insert(listening_history), new_tracks)
                   log.info(f"saved {len(new_tracks)} tracks to listening_history!!")
           except Exception as e:
               log.error(f"Error {e}")

        played_at_iso = items[-1]['played_at']
        played_at_ts = int(datetime.fromisoformat(played_at_iso.replace('Z', '+00:00')).timestamp() * 1000)
        after_ts = int(played_at_ts) + 1
        time.sleep(2)


def safe_streaming_sp_call(method, *args, max_retries=3, delay=3, **kwargs):
    for attempt in range(max_retries):
        try:
            return method(*args, **kwargs)
        except spotipy.SpotifyException as e:
            log.warning(f"Attempt {attempt + 1}: Spotify error: {e}")
            if e.http_status == 429:
                wait = int(e.headers.get("Retry-After", 5))
                log.warning(f'Spotify rate limit! wait time:{wait}')
                last_checkpoint = int(datetime.now().timestamp() * 1000)
                time.sleep(wait)
                log.info('finished waiting, going to try to retrieve past tracks')
                save_last_50_tracks(last_checkpoint)
            else:
                time.sleep(delay)
        except Exception as e:
            log.error(f"Attempt {attempt+1}: Unknown error: {e}")
            time.sleep(delay)
    log.fatal(f'safe spotipy call: {method.__name__}', 'failed', f'finished retries: {type(e).__name__} {str(e)}')
    return None