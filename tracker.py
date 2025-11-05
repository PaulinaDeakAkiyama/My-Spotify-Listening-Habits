from oauth import create_spotify_client
from datetime import datetime, timezone, UTC
from sqlalchemy import insert, select, func
from db import engine, listening_two, track_reference
from utils import  safe_spotipy_call

sp = create_spotify_client()

def get_current_track():

    current_info = safe_spotipy_call(sp.current_playback)
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

        album = current_track['album']
        album_id = album['id']
        album_name = album['name']
        album_artist = album['artists'][0]['id']
        album_collab_artist = album['artists'][1]['id'] if len(album['artists']) > 1 else None
        album_release_date = album['release_date']
        album_total_tracks = album['total_tracks']
        album_type = album['type']
        # label, popularity

        artists = current_track['artists']
        artist_id = artists[0]['id']
        artist_name = artists[0]['name']
        # artist followers, popularity, genres
        collab_artist = artists[1]['id'] if len(artists) > 1 else None
        if current_info and current_info.get("context") and current_info["context"].get("uri"):
            context = current_info['context']
        if context['type'] == 'playlist':
            playlist_id_current = context["uri"]
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
            'popularity': popularity,
            'playlist_id': playlist_id_current,
            'album_id': album_id,
            'album_name': album_name,
            'artist_id': artist_id,
            'artist_name': artist_name,
            'collab_artist': collab_artist
        })
    else:
        print(f'{datetime.now()}: no tracks are playing')
        return {}
    return current_track_info

# first artist, then albums, track reference, playlists

def save_track_to_reference(track_info):
    if not track_info:
        return
    with engine.begin() as conn:
        existing_ids = set(conn.execute(select(track_reference.c.track_id)).scalars())
    if track_info['track_id'] in existing_ids:
        return
    data = dict((k, track_info[k]) for k in [
        'track_id', 'track_name', 'album_id','album_name','artist_id','artist_name','collab_artist']
                if k in track_info)
    try:
        with engine.begin() as conn:
            conn.execute(insert(track_reference), data)
            print(f'saved track in to track_reference {data} {datetime.now()}')
    except Exception as e:
        print(f'Something went wrong with inserting track: {track_info}\n Error: {e}')


def save_track_to_listening(track_info):
    data = dict((k, track_info[k]) for k in [
        'track_id', 'track_name', 'progress_ms','duration_ms','playlist_id','popularity','device_name','volume_percentage']
                if k in track_info)
    try:
        with engine.begin() as conn:
            conn.execute(insert(listening_two), data)
            print(f'saved track!! {data} {datetime.now()}')
    except Exception as e:
        print(f'Something went wrong with inserting track: {track_info}\n Error: {e}')

