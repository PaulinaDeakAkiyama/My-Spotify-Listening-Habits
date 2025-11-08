from oauth import create_spotify_client
from datetime import datetime, timezone, UTC
from sqlalchemy import insert, select, func
from db import engine, listening_two, track_reference, artists, albums, listening_history
from utils import  safe_spotipy_call, insert_into_sql
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
    album_id_current = uri.split(":")[-1] if context.get("type") == "album" else None
    artist_id_current = uri.split(":")[-1] if context.get("type") == "artist" else None
    device_info = current_info.get("device", {})

    ar = current_track['artists']
    for a in ar:
        artists.append({
        'artist_id': a['id'],
        'artist_name': a['name']
        # followers, popularity, genres
        })

    al = current_track['album']
    alar = al['artists']
    for a in alar:
        artists.append({
            'artist_id':a['id'],
            'artist_name':a['name']
        })

    albums.update({
        'album_id': al['id'],
        'album_name': al['name'],
        'artist_id': al['artists'][0]['id'],
        'collab_artist': al['artists'][1]['id'] if len(al['artists'])>1 else None,
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
        'context_type': context.get('type')
    })
    return current_track_info



def update_artists():
    with engine.begin() as conn:
        artist_id = conn.execute(select(artists.c.artist_id).order_by(artists.c.inserted_at).limit(1)).scalar()
    data = safe_streaming_sp_call(sp.artist, artist_id)
    info = {'artist_id':artist_id,'followers': data['followers']['total'], 'genres': data['genres'], 'popularity': data['popularity']}
    stmt = mysql_insert(artists).values(**info)
    stmt = stmt.on_duplicate_key_update(**{k: stmt.inserted[k] for k in info.keys()})
    with engine.begin() as conn:
        conn.execute(stmt)


def update_albums():
    with engine.begin() as conn:
        album_id = conn.execute(select(albums.c.album_id).order_by(albums.c.inserted_at).limit(1)).scalar()
    data = safe_streaming_sp_call(sp.album, album_id)
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
                insert_into_sql(listening_history, new_tracks)
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


with engine.begin() as conn:
    existing_albums = set(conn.execute(select(albums.c.album_id)).scalars().all())
    existing_tracks = set(conn.execute(select(track_reference.c.track_id)).scalars().all())
    existing_artists = set(conn.execute(select(artists.c.artist_id)).scalars().all())


def deal_with_artists_albums_reference(track_info):
    """
    Insert or update artists, albums, and track_reference for a single track_info dict.
    Updates the existing_* sets to prevent duplicates.
    """
    # 1. Artists
    artist_list = track_info['artists']
    new_artists = [a for a in artist_list if a['artist_id'] not in existing_artists]
    if new_artists:
        insert_into_sql(artists, new_artists)
        update_artists()
        existing_artists.update(a['artist_id'] for a in new_artists)

    # 2. Album
    album_info = track_info.get('albums')
    if album_info and album_info['album_id'] not in existing_albums:
        insert_into_sql(albums, album_info)
        update_albums()
        existing_albums.add(album_info['album_id'])

    # 3. Track reference
    track_ref_info = track_info.get('track_reference')
    if track_ref_info and track_ref_info['track_id'] not in existing_tracks:
        insert_into_sql(track_reference, track_ref_info)
        existing_tracks.add(track_ref_info['track_id'])

    return