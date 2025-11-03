from utils import safe_spotipy_call, chunked, log_to_sql
from db import engine, track_reference, listening_two, albums, artists, playlist_tracks
from sqlalchemy import select, insert, func, update, distinct
from oauth import create_spotify_client
import time

sp = create_spotify_client()

# First get playlist contents and create playlist_tracks

#---------------------------------- Insert into track reference table ------------------------------------------

def get_playlist_tracks():
    with engine.begin() as conn:
        result = conn.execute(
            select(distinct(playlist_tracks.c.track_id))
            .where(playlist_tracks.c.track_id.not_in(select(func.coalesce(track_reference.c.track_id, 0))))
        ).scalars().all()
    return result

def get_missing_tracks():
    with engine.begin() as conn:
        result = conn.execute(
            select(distinct(listening_two.c.track_id))
            .where(
                listening_two.c.track_id.not_in(
                    select(func.coalesce(track_reference.c.track_id, 0))
                )
            )
        ).scalars().all()
    return result

def update_track_reference(track_ids):
    new_tracks = []
    for ids in chunked(track_ids, 50):
        print(f'going through {ids}')
        items = safe_spotipy_call(sp.tracks, ids)
        for item in items['tracks']:

            track_id = item['id']
            track_name = item['name']
            album = item['album']['name']
            album_id = item['album']['id']
            artists = item['artists']
            artist_name = artists[0]['name']
            artist_id = artists[0]['id']
            collab_artist = artists[1]['name'] if len(artists) > 1 else None

            new_tracks.append({
                'track_id' : track_id,
                'track_name' : track_name,
                'album' : album,
                'album_id' : album_id,
                'artist_name' : artist_name,
                'artist_id' : artist_id,
                'collab_artist' : collab_artist
            })
        time.sleep(1)
    if new_tracks:
        try:
            with engine.begin() as conn:
                conn.execute(insert(track_reference), new_tracks)
            print(f'nice. my tracks is populated with {new_tracks}')
        except Exception as e:
            print(f"error {e}")
            log_to_sql('inserting to track reference','failed', f"Error: {type(e).__name__} {e}")
    else:
        print(f'No new tracks to insert.')
        return


#---------------------------------------   Insert into album table ----------------------------------------
def get_album_ids():
    with engine.connect() as conn:
        existing_ids = conn.execute(
            select(distinct(track_reference.c.album_id))
            .where(
                track_reference.c.album_id.not_in(
                    select(func.coalesce(albums.c.album_id, 0))
                )
            )
        ).scalars().all()
    return existing_ids

def get_album_info():
    my_albums = get_album_ids()
    if not my_albums:
        print('no albums')
        return

    album_info = []
    for ids in chunked(my_albums, 20):
        print(f'going through chunk: {ids}')
        corresponding = safe_spotipy_call(sp.albums, ids)
        for album in corresponding['albums']:
            album_id = album['id']
            album_type = album['album_type']
            total_tracks = album['total_tracks']
            album_name = album['name']
            release_date = album['release_date']
            artist_id = album['artists'][0]['id']
            collab_artist = album['artists'][1]['name'] if len(album['artists']) > 1 else None
            label = album['label']
            popularity = album['popularity']

            album_info.append({
                'album_id':album_id,
                'album_name':album_name,
                'album_type':album_type,
                'total_tracks':total_tracks,
                'release_date':release_date,
                'artist_id':artist_id,
                'collab_artist':collab_artist,
                'label':label,
                'popularity':popularity
            })
    if not album_info:
        print('no album info')
        return
    try:
        with engine.begin() as conn:
            conn.execute(insert(albums), album_info)
            print(f'nice, albums is done')
    except Exception as e:
        print(f'ayayay {e}')
        log_to_sql('inserting to albums', 'failed', f"Error: {type(e).__name__} {e}")



#---------------------------------------   Insert into artist table ---------------------------------------
def get_artist_ids():
    with engine.connect() as conn:
        stmt = select(distinct(track_reference.c.artist_id)).where(
            track_reference.c.artist_id.not_in(select(artists.c.artist_id))
        ).union(
            select(distinct(track_reference.c.collab_artist)).where(
                track_reference.c.collab_artist.not_in(select(artists.c.artist_id))
            )
        )
        existing_ids = conn.execute(stmt).scalars().all()
    return existing_ids

def get_album_artist_ids():
    with engine.connect() as conn:
        stmt = select(distinct(albums.c.artist_id)).where(
            albums.c.artist_id.not_in(select(artists.c.artist_id))
        ).union(
            select(distinct(albums.c.collab_artist)).where(
                albums.c.collab_artist.not_in(select(artists.c.artist_id))
            )
        )
        existing_ids = conn.execute(stmt).scalars().all()
    return existing_ids


def get_artist_info():
    my_artists = get_artist_ids() + get_album_artist_ids()
    if not my_artists:
        print("No artists to update.")
        return

    artist_info = []
    for ids in chunked(my_artists, 50):
        print(f"Processing chunk: {ids}")
        corresponding = safe_spotipy_call(sp.artists, ids)
        for artist in corresponding["artists"]:
            artist_info.append({
                "artist_id": artist["id"],
                "artist_name": artist["name"],
                "artist_followers": artist["followers"]["total"],
                "artist_genres": artist["genres"],
                "artist_popularity": artist["popularity"],
            })

    if not artist_info:
        print("Nothing to update.")
        return

    try:
        with engine.begin() as conn:
            conn.execute(insert(artists), artist_info)
            print(f'nice, artists is done')
    except Exception as e:
        print(f'ayayay {e}')
        log_to_sql('inserting to artists', 'failed', f"Error: {type(e).__name__} {e}")

def populate_tables_pipeline():
    my_playlist_tracks = get_playlist_tracks()
    update_track_reference(my_playlist_tracks)
    streamed_tracks = get_missing_tracks()
    update_track_reference(streamed_tracks)

    get_album_info()
    get_artist_info()


if __name__ == '__main__':
    populate_tables_pipeline()

# missing_artists = get_missing_artists()
# missing_albums = get_missing_albums()
#
# album_ids = {}
# artist_ids = {}
#
# for ids in chunked(missing_albums, 50):
#     track_info = safe_spotipy_call(sp.tracks, ids)
#     album_ids.update({t['id']: t['album']['id'] for t in track_info['tracks']})
#
# for ids in chunked(missing_artists, 50):
#     artist_info = safe_spotipy_call(sp.tracks, ids)
#     artist_ids.update({t['id']: t['artists'][0]['id'] for t in artist_info['tracks']})
#
# print(f'going to insert album ids: {album_ids}\n artist ids: {artist_ids}')
# try:
#     with engine.begin() as conn:
#         for track_id, album_id in album_ids.items():
#             conn.execute(
#                 update(track_reference)
#                 .where(track_reference.c.track_id == track_id)
#                 .values(album_id=album_id)
#             )
#         for track_id, artist_id in artist_ids.items():
#             conn.execute(
#                 update(track_reference)
#                 .where(track_reference.c.track_id == track_id)
#                 .values(artist_id=artist_id)
#             )
# except Exception as e:
#     print(f'oops {e}')
# print('done!')


# with engine.begin() as conn:
#     track_id = conn.execute(select(track_reference.c.track_id)).scalars().all()
#     collab = {}
#     for chunk in chunked(track_id, 50):
#         info = safe_spotipy_call(sp.tracks, chunk)
#         collab.update({track['id']:track['artists'][1]['id'] if len(track['artists']) > 1 else None for track in info['tracks']})
#         print(collab)
#     try:
#         with engine.begin() as conn:
#             for track_id, collab_id in collab.items():
#                 conn.execute(
#                     update(track_reference)
#                     .where(track_reference.c.track_id == track_id)
#                     .values(collab_artist=collab_id)
#                 )
#             print('yay')
#     except Exception as e:
#         print(f'oh no {e}')