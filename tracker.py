from oauth import create_spotify_client
from datetime import datetime, timezone, UTC
from sqlalchemy import insert, select, func
from db import engine, listening_history, listening_two

sp = create_spotify_client()

def get_current_track():

    current_info = sp.current_playback()
    current_track_info = {}

    if current_info and current_info["is_playing"]:
        device_info = current_info['device']
        volume_percent = device_info['volume_percent']
        device_name = device_info['name']
        current_track = current_info["item"]
        track_id = current_track["id"]
        popularity = current_track['popularity']
        track_name = current_track["name"]
        duration_ms = current_track['duration_ms']
        progress_ms = current_info["progress_ms"]
        if current_info and current_info.get("context") and current_info["context"].get("uri"):
            playlist_id_current = current_info["context"]["uri"]
        else:
            playlist_id_current = None
            print(f'no playlist available{track_name}')
        current_track_info.update({
            'progress_ms': progress_ms,
            'duration_ms': duration_ms,
            'track_id': track_id,
            'track_name': track_name,
            'device_name': device_name,
            'volume_percentage': volume_percent,
            'popularity':popularity,
            'playlist_id':playlist_id_current
        })
    else:
        print(f'{datetime.now()}: no tracks are playing')
        return {}
    return current_track_info


def save_track(track_info):
    try:
        with engine.begin() as conn:
            conn.execute(insert(listening_two), track_info)
            print(f'saved track!! {track_info} {datetime.now()}')
    except Exception as e:
        print(f'Something went wrong with inserting track: {track_info}\n Error: {e}')

