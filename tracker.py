from oauth import create_spotify_client
from datetime import datetime, timezone, UTC
from sqlalchemy import insert, select, func
from db import engine, listening_two, track_reference, artists, albums, listening_history
from utils import safe_spotipy_call, insert_into_sql, get_existing_ids
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

    current_track = current_info.get("item")
    if not current_track:
        log.warning('no track in current info?')
        return {}

    context = current_info.get("context") or {}
    uri = context.get("uri", "")
    playlist_id_current = uri.split(":")[-1] if context.get("type") == "playlist" else None
    album_id_current = uri.split(":")[-1] if context.get("type") == "album" else None
    artist_id_current = uri.split(":")[-1] if context.get("type") == "artist" else None

    device_info = current_info.get("device", {})
    ar = current_track['artists']
    al = current_track['album']
    alar = al['artists']

    artists_ids = set()
    for a in ar:
        artists_ids.add(a['id'])
    for a in alar:
        artists_ids.add(
            a['id']
        )

    albums_ids = [al['id']]

    track_ref = {}
    track_ref.update({
        'track_id': current_track['id'],
        'track_name': current_track["name"],
        'album_id': al['id'],
        'artist_id': ar[0]['id'],
        'collab_artist': ar[1]['id'] if len(ar) > 1 else None
    })

    streaming = {}
    streaming.update({
        'progress_ms': current_info['progress_ms'],
        'duration_ms': current_track['duration_ms'],
        'track_id': current_track['id'],
        'device_name': device_info['name'],
        #'device_type': device_info['type'],
        'volume_percentage': device_info['volume_percent'],
        'popularity': current_track['popularity'],
        'context_id': playlist_id_current or album_id_current or artist_id_current,
        'context_type': context.get('type'),
        'playlist_fk': 1
    })

    current_track_info = {'artists': artists_ids, 'albums': albums_ids, 'track_reference': track_ref, 'listening_two': streaming}
    return current_track_info


def update_artists(artist_ids):
    if not artist_ids:
        log.warning('no artist ids')
        return
    try:
        result = safe_streaming_sp_call(sp.artists, artist_ids)
        artist_info = []
        for data in result['artists']:
            info = {
                'artist_id':data['id'],
                'artist_name':data['name'],
                'followers': data['followers']['total'],
                'genres': data['genres'],
                'popularity': data['popularity']
            }
            artist_info.append(info)
        insert_into_sql(artists, artist_info)
    except Exception as e:
        log.error(e)


def update_albums(album_ids):
    if not album_ids:
        log.warning('no album ids')
        return
    try:
        result = safe_streaming_sp_call(sp.albums, album_ids)
        albums_list = [a for a in (result.get('albums') or []) if a is not None]
        if not albums_list:
            log.warning('no valid albums returned')
            return False
        info = []
        for data in result['albums']:
            info.append({
                'album_id':data['id'],
                'album_name': data['name'],
                'artist_id': data['artists'][0]['id'],
                'collab_artist': data['artists'][1]['id'] if len(data['artists'])>1 else None,
                'release_date': data['release_date'],
                'total_tracks': data['total_tracks'],
                'album_type': data['album_type'],
                'label':data['label'],'popularity':data['popularity']
            })
        insert_into_sql(albums, info)
        return True
    except Exception as e:
        log.error(e)

existing_tracks = get_existing_ids(track_reference)


def deal_with_artists_albums_reference(track_info):
    """
    Insert or update artists, albums, and track_reference for a single track_info dict.
    Updates the existing_* sets to prevent duplicates.
    """
    #first check if the song already exists
    track_ref_info = track_info.get('track_reference')
    if track_ref_info['track_id'] in existing_tracks:
        return True
    else:
        existing_albums = get_existing_ids(albums)
        existing_artists = get_existing_ids(artists)
        artist_set = track_info['artists']
        album_ids = track_info.get('albums')

        new_albums = [a for a in album_ids if a not in existing_albums]
        new_artists = [a for a in artist_set if a not in existing_artists]

        if new_artists:
            update_artists(new_artists)
            existing_artists.update(a for a in new_artists)

        if new_albums:
            check = update_albums(new_albums)
            if check is False:
                track_ref_info['album_id'] = None
            else:
                existing_albums.update(i for i in album_ids)

        check = insert_into_sql(track_reference, track_ref_info)
        if check is True:
            existing_tracks.add(track_ref_info['track_id'])
            return True
        else:
            return False


def check_track_up_to_date():
    with engine.connect() as conn:
        sql_last_50 = conn.execute(
            select(
                listening_two.c.start_time
            ).where(
                listening_two.c.track_id.isnot(None)
            ).order_by(
                listening_two.c.start_time.desc()
            ).limit(1)
        ).scalar()
    last_checkpoint = int(sql_last_50.timestamp() * 1000)

    #while True:
    recent = safe_spotipy_call(sp.current_user_recently_played, limit=50)
    items = recent.get("items", [])
    new_tracks = []
    for item in items:
        date_played = datetime.fromisoformat(item['played_at'])
        naieve_date_played = date_played.replace(tzinfo=None)
        if naieve_date_played <= sql_last_50:
            log.info('up to date')
            break
        track_id = item['track']['id']
        downloaded = item['track']['is_local']
        popularity = item['track']['popularity']
        duration_ms = item['track']['duration_ms']
        context = item.get("context") or {}
        uri = context.get("uri", "")
        playlist_id_current = uri.split(":")[-1] if context.get("type") == "playlist" else None
        album_id_current = uri.split(":")[-1] if context.get("type") == "album" else None
        artist_id_current = uri.split(":")[-1] if context.get("type") == "artist" else None

        new_tracks.append({
            'track_id': track_id,
            'popularity': popularity,
            'date_played': date_played,
            'duration_ms': duration_ms,
            'context_id': playlist_id_current or album_id_current or artist_id_current,
            'context_type': context['type'],
            'downloaded': downloaded
        })

    if new_tracks:
        try:
            insert_into_sql(listening_history, new_tracks)
            log.info(f'inserted {len(new_tracks)} back up tracks to listening_history')
        except Exception as e:
            log.error(f"Error {e}")

check_track_up_to_date()


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
                log.info(f'inserted {len(new_tracks)} back up tracks to listening_history')
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
                wait = int(e.headers.get("Retry-After", 8))
                wait += 60000
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
