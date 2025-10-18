from oauth import get_spotify_auth
import time
from datetime import datetime, timezone, UTC
from sqlalchemy import insert, select, func
from db import engine, listening_history, listening_stream

sp = get_spotify_auth()


def get_last_track():
    last_track_id = None
    last_progress_ms = None
    last_start_time = None
    last_track_playlist = None
    last_track_name = None

    while True:
        track_info = sp.current_user_playing_track()

        if track_info and track_info["is_playing"]:
            current_track = track_info["item"]
            date_played = datetime.now(timezone.utc)
            track_id = current_track["id"]
            track_name = current_track["name"]
            progress_ms = track_info["progress_ms"]
            if track_info and track_info.get("context") and track_info["context"].get("uri"):
                playlist_id_current = track_info["context"]["uri"]
            else:
                playlist_id_current = None
                print(f'no playlist available{track_name}')

            if not track_id == last_track_id:
                if last_track_id is None:
                    pass
                else:
                    print(f'track playing:{track_name}')
                    try:
                        with engine.begin() as conn:
                            conn.execute(insert(listening_stream).values(
                                track_id=last_track_id,
                                track_name=last_track_name,
                                date_started=last_start_time,
                                progress_ms=last_progress_ms,
                                playlist_id=last_track_playlist
                            ))
                            print(f"saved track!! {last_track_name} ")
                    except Exception as e:
                        print(f"Error {e}")
            time.sleep(5)

            last_track_id = track_id
            last_track_name = track_name
            last_track_playlist = playlist_id_current
            last_start_time = date_played
            last_progress_ms = progress_ms
        else:
            time.sleep(10)


#
def save_last_50_tracks():
    #
    with engine.connect() as conn:
        result = conn.execute(select(func.max(listening_history.c.date_played)))
        last_date_played = result.scalar()

    recent = sp.current_user_recently_played(limit=50)
    items = recent['items']

    new_tracks = []
    for item in items:
        track_id = item['track']['id']
        track_name = item['track']['name']
        popularity = item['track']['popularity']
        date_played = datetime.fromisoformat(item['played_at'])
        naieve_date_played = date_played.replace(tzinfo=None)
        duration = item['track']['duration_ms']
        if (last_date_played is None) or (naieve_date_played > last_date_played):
            new_tracks.append({
                'track_id' : track_id,
                'track_name' : track_name,
                'popularity' : popularity,
                'date_played' : date_played,
                'duration_ms' : duration
            })
            print(f"{track_name} was played at: {naieve_date_played}")
    if new_tracks:

        try:
            with engine.begin() as conn:
                conn.execute(insert(listening_history), new_tracks)
                print(f"saved {len(new_tracks)} tracks to listening_history!!")
        except Exception as e:
            print(f"Error {e}")


def get_current_track():

    current_info = sp.current_playback()
    current_track_info = {}

    if current_info and current_info["is_playing"]:
        device_info = current_info['device']
        volume_percent = device_info['volume_percent']
        device_name = device_info['name']
        current_track = current_info["item"]
        time_played = datetime.now().strftime('%H:%M:%S')
        date_played = datetime.now()
        track_id = current_track["id"]
        track_name = current_track["name"]
        duration_ms = current_track['duration_ms']
        progress_ms = current_info["progress_ms"]
        if current_info and current_info.get("context") and current_info["context"].get("uri"):
            playlist_id_current = current_info["context"]["uri"]
        else:
            playlist_id_current = None
            print(f'no playlist available{track_name}')
        current_track_info.update({
            'date_played':date_played,
            'progress_ms': progress_ms,
            'duration_ms': duration_ms,
            'track_id': track_id,
            'track_name': track_name,
            'device_name': device_name,
            'volume_percentage': volume_percent
        })
    else:
        print(f'{datetime.now()}: no tracks are playing')
        return
    return current_track_info



def save_track(track_info):
    try:
        with engine.begin() as conn:
            conn.execute(insert(listening_stream).values(track_info))
            print(f'saved track!! {track_info['track_name']}')
    except Exception as e:
        print(f'Something went wrong with inserting track: {track_info}\n Error: {e}')

