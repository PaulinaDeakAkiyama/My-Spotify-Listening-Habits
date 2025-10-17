import http.client
import time
import requests
from db import engine, listening_stream, listening_history, my_tracks, track_features
from sqlalchemy import create_engine, insert, select
from oauth import get_spotify_auth

sp = get_spotify_auth()

with engine.connect() as conn:
    my_track_ids = set(
        conn.execute(
            select(my_tracks.c.track_id)
            .where(my_tracks.c.track_id.notin_(
                select(track_features.c.track_id))
            )
        ).scalars().all()
    )

def safe_request(
    method: str,
    url: str,
    headers=None,
    params=None,
    data=None,
    max_retries: int = 3,
    backoff_factor: float = 2.0,
    timeout: int = 10
):
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.request(
                method, url,
                headers=headers,
                params=params,
                data=data,
                timeout=timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"[safe_request] Attempt {attempt}/{max_retries} failed: {e}")
            if attempt < max_retries:
                sleep_time = backoff_factor ** attempt
                print(f"Retrying in {sleep_time:.1f}s...")
                time.sleep(sleep_time)
            else:
                print(f"[safe_request] All {max_retries} attempts failed for {url}")
                return None



def chunked(iterable, size):
    lst = list(iterable)
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def get_track_mp3(track_ids):
    tracks = sp.tracks(track_ids)
    if not tracks:
        print('no track')
    ids = [track['id'] for track in tracks['tracks'] if track['id']]
    return ids


def get_reccobeats_id(spotify_ids):

    url = f"https://api.reccobeats.com/v1/track?ids={','.join(spotify_ids)}"
    headers = {'Accept': 'application/json'}
    method = 'GET'
    tracks = safe_request(method, url, headers=headers)

    if not tracks or 'content' not in tracks or not tracks['content']:
        print('no corresponding reccobeat_id')
        return[]

    reccobeats_ids = [track['id'] for track in tracks['content'] if track['id']]
    return reccobeats_ids


def get_track_features(reccobeats_ids):
    url = f"https://api.reccobeats.com/v1/audio-features?ids={','.join(reccobeats_ids)}"
    headers = {'Accept': 'application/json'}
    batch_features = safe_request("GET", url, headers=headers)

    if not batch_features or 'content' not in batch_features or not batch_features['content']:
        print('no features')
        return []

    batch_track_features = []
    for features in batch_features['content']:
        try:
            batch_track_features.append({
                "track_id": features['href'][31:],
                "reccobeats_id": features['id'],
                "acousticness": features['acousticness'],
                "danceability": features['danceability'],
                "energy": features['energy'],
                "instrumentalness": features['instrumentalness'],
                "key_": features['key'],
                "loudness": features['loudness'],
                "mode_": features['mode'],
                "speechiness": features['speechiness'],
                "tempo": features['tempo'],
                "valence": features['valence']
            })
            print(batch_track_features)
        except Exception as e:
            print(f'Error:{e}')
            continue
    return batch_track_features



def save_track_features(my_track_ids):
    new_track_features = []
    for spotify_ids in chunked(my_track_ids, 40):
        print(f'going through spotify ids: {spotify_ids}')
        try:
            reccobeats_ids = get_reccobeats_id(spotify_ids)
            if not reccobeats_ids:
                continue
        except Exception as e:
            print(f'Error!?:{e}')
            continue
        print(f'\ngoing through reccobeats ids: {reccobeats_ids}\n')

        try:
            features = get_track_features(reccobeats_ids)
            print('got features successfully')
            new_track_features.extend(features)
        except Exception as e:
            print(f'Error?:{e}')
            continue

        time.sleep(0.10)

    if new_track_features:
        print(f'going to try to insert new values...\n{new_track_features}')
        try:
            with engine.begin() as conn:
                conn.execute(insert(track_features), new_track_features)
            print('nice.')
        except Exception as e:
            print(f'couldnt insert into table {e}')
    else:
        print('no new track feature info')

save_track_features(my_track_ids)

