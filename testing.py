from oauth import get_spotify_auth
from db import engine, listening_stream, playlists, my_tracks, listening_history #track_features, artists, fact
from sqlalchemy import insert, select, func
from datetime import datetime, time, timezone, timedelta
import time
from tracker import save_last_50_tracks

import requests

sp = get_spotify_auth()

current = sp.current_playback()
current_track = sp.current_user_playing_track()
print(current.keys())
print(current['repeat_state'])


