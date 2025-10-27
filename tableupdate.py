from oauth import get_spotify_auth
from db import engine, playlists, my_tracks, listening_stream, listening_two  # track_features, artists, fact
from sqlalchemy import insert, select, func
from datetime import datetime, time
import time

sp = get_spotify_auth()

def update_playlists():
    user_playlists = sp.current_user_playlists()
    my_playlists = [id for id in user_playlists['items']['id']]
    with engine.connect() as conn:
        sql_playlist_id = conn.execute(select(playlists.c.id)).scalar()

    new_playlist = []
    for item in user_playlists['items']:
        playlist_id = item['id'],
        playlist_name = item['name'],
        owner_id = item['owner']['id'],
        total_tracks = item['tracks']['total']
        if playlist_id in sql_playlist_id:
            print(f'{playlist_name} already in database')
            continue
        new_playlist.append({
            'playlist_id' : playlist_id,
            'playlist_name' : playlist_name,
            'owner_id' : owner_id,
            'total_tracks' : total_tracks
        })
        print(f'{playlist_name} added to new_playlist')
        time.sleep(1)

    if new_playlist:
        try:
            with engine.begin() as conn:
                    conn.execute(insert(playlists),new_playlist)
            print(f"updated playlist table ")
        except Exception as e:
            print(f"Error {e}")
    return my_playlists


def update_tracks_in_playlist(playlist_id, offset):
    playlist_info = sp.playlist_items(playlist_id=playlist_id, limit=100, offset=offset)
    items = playlist_info.get('items', [])

    if not items:
        print(f"No tracks returned for playlist {playlist_id}")
        return

    with engine.connect() as conn:
        existing_ids = set(
            conn.execute(
                select(my_tracks.c.id).where(my_tracks.c.playlist_id == playlist_id)
            ).scalars()
        )

    new_tracks = []
    for item in items:
        track = item.get('track')
        if not track:
            continue

        track_id = item['track']['id']
        track_name = item['track']['name']
        added_at = datetime.fromisoformat(item['added_at'].replace('Z', '+00:00'))
        added_by = item['added_by']['id']
        album = item['track']['album']['name']
        artists = item['track']['artists']
        artist_name = artists[0]['name']
        collab_artist = artists[1]['name'] if len(artists) > 1 else None
        track_primary_key = f"{track_id}{playlist_id}{added_at.strftime('%Y-%m-%d%H:%M:%S')}"

        if track_primary_key in existing_ids:
            print(f"skipped duplicate: {track_name}")
            continue

        new_tracks.append({
            'id' : track_primary_key,
            'track_id' : track_id,
            'track_name' : track_name,
            'album' : album,
            'artist_name' : artist_name,
            'collab_artist' : collab_artist,
            'playlist_id' : playlist_id,
            'added_at' : added_at,
            'added_by' : added_by
        })

    if new_tracks:
        try:
            with engine.begin() as conn:
                conn.execute(insert(my_tracks), new_tracks)
            print(f'nice. {playlist_id} is populated with {track_name}')
        except Exception as e:
            print(f"error {e}")
    else:
        print(f'No new tracks to insert.')
    time.sleep(1)


def update_tracks_and_playlists():
    my_playlists = update_playlists()

    for playlist_id in my_playlists:
        print(f'going through playlist:{playlist_id}...')
        playlist_info = sp.playlist_items(playlist_id=playlist_id)
        track_total = playlist_info['total']

        with engine.connect() as conn:
            track_count = conn.execute(
                select(func.count().label("track_count")).where(my_tracks.c.playlist_id == playlist_id)
            ).scalar()

        print(f"playlist has: {track_total}, DB has: {track_count} tracks")

        offset=0
        safety_limit = 1000
        while track_count<track_total and safety_limit > 0:
            print(f"Updating playlist {playlist_id} from offset {offset}")
            update_tracks_in_playlist(playlist_id,offset)
            offset += 100
            with engine.connect() as conn:
                track_count = conn.execute(
                    select(func.count().label("track_count")).where(my_tracks.c.playlist_id == playlist_id)
                ).scalar()
            safety_limit -=100
            time.sleep(0.25)
        print(f"playlist {playlist_id} is up to date, number of exceptions = {track_total - track_count}")


def chunked(iterable, size):
    lst = list(iterable)
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def get_stream_tracks():
    with engine.connect() as conn:
        result = conn.execute(
            select(listening_two.c.track_id)
            .where(listening_two.c.track_id.notin_(
                select(my_tracks.c.track_id.coalesce()))
            )
        ).all()
    return result

def update_my_tracks(track_ids):
    new_tracks = []
    for ids in chunked(track_ids, 50):
        items = sp.tracks(ids)

        for item in items:
            track = item.get('track')
            if not track:
                continue

            track_id = item['track']['id']
            track_name = item['track']['name']
            added_at = datetime.fromisoformat(item['added_at'].replace('Z', '+00:00'))
            added_by = item['added_by']['id']
            album = item['track']['album']['name']
            artists = item['track']['artists']
            artist_name = artists[0]['name']
            collab_artist = artists[1]['name'] if len(artists) > 1 else None

            new_tracks.append({
                'track_id' : track_id,
                'track_name' : track_name,
                'album' : album,
                'artist_name' : artist_name,
                'collab_artist' : collab_artist,
                'added_at' : added_at,
                'added_by' : added_by
            })

        if new_tracks:
            try:
                with engine.begin() as conn:
                    conn.execute(insert(my_tracks), new_tracks)
                print(f'nice. my tracks is populated with {track_name}')
            except Exception as e:
                print(f"error {e}")
        else:
            print(f'No new tracks to insert.')
        time.sleep(1)

ids = get_stream_tracks()
print(ids)
update_my_tracks(ids)


