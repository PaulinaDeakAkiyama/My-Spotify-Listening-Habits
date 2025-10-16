from oauth import get_spotify_auth
from db import engine, playlists, my_tracks #track_features, artists, fact
from sqlalchemy import insert, select, func
from datetime import datetime, time
import time

sp = get_spotify_auth()

my_playlists = ['1OaWf0HFRP3lLvFA4BXJz5','3JQkFXE6vd3PrfQ65pROii', '5edoupiQlrtttXB7I98k4I','0aL6LHYPDAPUzc6DKd6Mcl',
                '4jwvrD4z5GAecPUW7AzXP7','2o0FKku9O2tjExEeerzqCe','7Ky31gNqczvQtvrS3PVgqv','3G4M431nwRzUTR02f7vaWX',
                '1cbj1vjxaxS7en46sPRt6U','1av7t6d69ICcNPCHKjfH6k','2E8gChN3Rkb4ZHwb0vHJ83','06OWuVrIQrkaL6fn3hFFyx',
                '7E4E45op7Sqgbs4LoKKm2i','1IHFc1rFmhXzHTf23ZZ50N','2FSNmMCnmzPVn61OoVjX9G','29lNlOMIHQgV4s84ICuQG4',
                '2lgnvhfMsoVPagpVpZJFnI','0twR2pLyqROKghFHRCmOde','3gz8syXUq9v0HrPzZ6inzl','5Ynx9k28bHDDH5b6k18Zix',
                '653yA6roZctGEpkJfPi1iX','23UUYp9qnWjbVEos5IPyvK','1Aq8lofwjw0IAWxigIKsLf','6YgRySvPiqRmmsDDyu98cb',
                '5lMMlqyjWKghX7glN2A7NT','1WltH2DVwD4ZXcxXmSxQcW']

def update_playlists():
    user_playlists = sp.current_user_playlists()
    with engine.connect() as conn:
        result = conn.execute(select(playlists.c.id))
        sql_playlist_id = result.scalar()

    new_playlist = []
    for item in user_playlists['items']:
        playlist_id = item['id'],
        playlist_name = item['name'],
        owner_id = item['owner']['id'],
        total_tracks = item['tracks']['total']
        if playlist_id in sql_playlist_id:
            print(f'{playlist_name} already in database')
            continue
        my_playlists.append(playlist_id)
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


def update_tracks(playlist_id, offset):
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
        update_tracks(playlist_id,offset)
        offset += 100
        with engine.connect() as conn:
            track_count = conn.execute(
                select(func.count().label("track_count")).where(my_tracks.c.playlist_id == playlist_id)
            ).scalar()
        safety_limit -=100
        time.sleep(0.25)
    print(f"playlist {playlist_id} is up to date, number of exceptions = {track_total - track_count}")



