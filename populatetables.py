from utils import safe_spotipy_call, chunked, log_to_sql, insert_into_sql
from db import engine, track_reference, listening_two, albums, artists, playlist_tracks, playlists
from sqlalchemy import select, insert, func, update, distinct, and_
from oauth import create_spotify_client
import time
from SCDplaylistsupdate import get_playlist_contents
from logger import log

sp = create_spotify_client()

# First get playlist contents and create playlist_tracks

#------------------------------------------get new playlist info--------------------------------------------------
def get_unknown_playlist():
    with engine.begin() as conn:
        result = conn.execute(
            select(listening_two.c.context_id)
            .where(
                and_(
                    listening_two.c.context_type == 'playlist',
                    ~listening_two.c.context_id.in_(
                        select(playlists.c.playlist_id)
                    )
                )
            )
            .distinct()
        ).scalars().all()
    return result


#----------------------------------------get track reference info--------------------------------------
def get_track_reference_info(track_ids):
    with engine.begin() as conn:
        existing_tracks = set(conn.execute(select(track_reference.c.track_id)).scalars().all())
        filtered_track_ids = [tid for tid in track_ids if tid not in existing_tracks]
    new_tracks = []
    for ids in chunked(filtered_track_ids, 50):
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
        return new_tracks
    else:
        log.warning(f'No new tracks to insert.')
        return


#---------------------------------------   Insert into album table --------------------------------------------------
#---------------------------------------  album ids in track reference ------------------------------------------------
def get_album_ids():
    with engine.connect() as conn:
        stmt = select(distinct(track_reference.c.album_id)).where(
            track_reference.c.album_id.not_in(select(albums.c.album_id)))
        existing_ids = conn.execute(stmt).scalars().all()
    return existing_ids

def get_album_info(my_albums):
    try:
        if not my_albums:
            print('no albums')
            return

        with engine.begin() as conn:
            existing_albums = set(conn.execute(select(albums.c.album_id)).scalars().all())
            filtered_album_ids = [aid for aid in my_albums if aid not in existing_albums]

        album_info = []
        for ids in chunked(filtered_album_ids, 20):
            print(f'going through chunk: {ids}')
            corresponding = safe_spotipy_call(sp.albums, ids)
            for album in corresponding['albums']:
                album_id = album['id']
                album_type = album['album_type']
                total_tracks = album['total_tracks']
                album_name = album['name']
                release_date = album['release_date']
                ar = album['artists']
                artist_ids = []
                for a in ar:
                    artists.append({
                        'artist_id': a['id']
                    })
                collab_artist = album['artists'][1]['name'] if len(album['artists']) > 1 else None
                label = album['label']
                popularity = album['popularity']

                album_info.append({
                    'album_id':album_id,
                    'album_name':album_name,
                    'album_type':album_type,
                    'total_tracks':total_tracks,
                    'release_date':release_date,
                    'artist_id':artist_ids,
                    'collab_artist':collab_artist,
                    'label':label,
                    'popularity':popularity
                })
        if not album_info:
            print('no album info')
            return
        return album_info
    except Exception as e:
        log.warning(f'something happend in get artist info {e}')



#---------------------------------------   Insert into artist table -------------------------------------------------
# --------------------------------- artist ids in track reference and in albums -------------------------------------
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


def get_artist_info(my_artists):
    try:
        if not my_artists:
            print("No artists to update.")
            return

        with engine.begin() as conn:
            existing_artists = set(conn.execute(select(artists.c.artist_id)).scalars().all())
            filtered_artist_ids = [aid for aid in my_artists if aid not in existing_artists]

        artist_info = []
        for ids in chunked(filtered_artist_ids, 50):
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
        return artist_info
    except Exception as e:
        log.warning(f'something happend in get artist info {e}')

def populate_tables_pipeline():
    """Look for new playlists in listening_two, get track ids with playlists contents then album and artist ids with
    get track reference info. insert into artists, albums, track reference, playlists and playlist tracks in order"""
    playlist_ids = get_unknown_playlist()
    with engine.begin() as conn:
        existing_playlist_ids = set(conn.execute(select(playlists.c.playlist_id)).scalars().all())

    for i in playlist_ids:
        if i in existing_playlist_ids:
            log.info(f'skipping playlist{i}')
            continue

        data = get_playlist_contents(i)
        p_track_ids = [t['track_id'] for t in data if 'track_id' in t]
        track_reference_info = get_track_reference_info(p_track_ids)
        tref_album_ids = [i['album_id'] for i in track_reference_info if 'album_id' in i]
        tref_artist_ids = []
        album_ids = tref_album_ids + get_album_ids()
        album_info = get_album_info(album_ids)
        album_artist_ids = [aid for i in album_info for aid in i['artist_ids']]
        artist_ids = get_artist_ids() + get_album_artist_ids() + album_artist_ids + tref_artist_ids
        artist_info = get_artist_info(artist_ids)

        insert_into_sql(artists, artist_info)
        insert_into_sql(albums, album_info)
        insert_into_sql(track_reference, track_reference_info)


    playlist_content = get_new_playlist_info()
    pc_track_ids = [track['id'] for track in playlist_content]
    #got track ids
    #artist_ids
    #album_ids
    #insert
    update_track_reference(pc_track_ids)
    #update_tracks_and_playlists()

    #ADD ALBUM CONTENTS TO TRACK REFERENCE



if __name__ == '__main__':
    populate_tables_pipeline()



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