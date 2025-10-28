from oauth import get_spotify_auth
from db import engine, listening_history
from sqlalchemy import insert
from datetime import datetime
import time

sp = get_spotify_auth()


def save_last_50_tracks():
    """ get last 50 tracks and store it in MySQL"""
    recent = sp.current_user_recently_played(limit=50)
    items = recent['items']

    for item in items:
        track_id = item['track']['id']
        track_name = item['track']['name']
        popularity = item['track']['popularity']
        date_played = datetime.fromisoformat(item['played_at'])
        duration = item['track']['duration_ms']

        try:
            with engine.begin() as conn:
                conn.execute(insert(listening_history).values(
                    track_id=track_id,
                    track_name=track_name,
                    popularity=popularity,
                    date_played=date_played,
                    duration_ms = duration
                ))
            print(f"saved track!! {track_name} ")
        except Exception as e:
            print(f"Error {e}")


# artist_id = item['artists'][0]['id']
# artist = item['artists'][0]['name']
# artist_data = sp.artist(artist_id)
# genre = artist_data['genres'][0] if artist_data['genres'] else None
# album_id = item['track']['album']['id']
# album_name = item['track']['album']['name']