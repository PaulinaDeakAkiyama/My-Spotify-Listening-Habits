from oauth import get_spotify_auth
from db import engine, listening_stream, playlists, my_tracks, listening_history #track_features, artists, fact
from sqlalchemy import insert, select, func
from datetime import datetime, time, timezone
import time

import requests

sp = get_spotify_auth()

current = sp.current_playback()
current_track = sp.current_user_playing_track()
print(current.keys())
print(current['repeat_state'])



        # if not track_id == last_track_id:
        #     if last_track_id is None:
        #         time.sleep(5)
        #
        #     else:
        #         print(f'track playing:{track_name}')
        #         try:
        #             with engine.begin() as conn:
        #                 conn.execute(insert(listening_stream).values(
        #                     track_id=last_track_id,
        #                     track_name=last_track_name,
        #                     date_started=last_start_time,
        #                     progress_ms=last_progress_ms,
        #                     playlist_id=last_track_playlist
        #                 ))
        #                 print(f"saved track!! {last_track_name} ")
        #         except Exception as e:
        #             print(f"Error {e}")
        # time.sleep(5)
        #
        # last_track_id = track_id
        # last_track_name = track_name
        # last_track_playlist = playlist_id_current
        # last_start_time = date_played
        # last_progress_ms = progress_ms
        # last_volume_percent = volume_percent
        # last_device_name = device_name
    # else:
    #     time.sleep(20)


def get_current_track():

    current_info = sp.current_playback()
    current_track_info = []

    if current_info and current_info["is_playing"]:
        device_info = current_info['device']
        volume_percent = device_info['volume_percent']
        device_name = device_info['name']
        current_track = current_info["item"]
        date_played = datetime.now()
        track_id = current_track["id"]
        track_name = current_track["name"]
        progress_ms = current_info["progress_ms"]
        if current_info and current_info.get("context") and current_info["context"].get("uri"):
            playlist_id_current = current_info["context"]["uri"]
        else:
            playlist_id_current = None
            print(f'no playlist available{track_name}')
        current_track_info.append({
            'date_played': date_played,
            'track_id': track_id,
            'track_name': track_name,
            'progress_ms': progress_ms,
            'device_name': device_name,
            'volume_percent': volume_percent
        })
    else:
        print(f'{datetime.now()}: no tracks are playing')
    return current_track_info


try:
    previous_track = get_current_track()
    previous_track_id = previous_track[0]['track_id']
    print(f'currently playing: {previous_track[0]['track_name']}')
    while True:

        current_track = get_current_track()
        current_track_id = current_track[0]['track_id']

        if not current_track_id == previous_track_id:
            print(f'finished song: {previous_track[0]['track_name']}\n Now playing: {current_track[0]['track_name']}')
            previous_track = get_current_track()
            previous_track_id = previous_track[0]['track_id']
        else:
            time.sleep(10)

        time.sleep(10)

except KeyboardInterrupt:
    print('Stopped')
