import json
from datetime import datetime
from PIL.ImageChops import offset
from utils import safe_spotipy_call, chunked, log_to_sql, insert_into_sql
from db import engine, track_reference, listening_two, albums, artists, playlist_tracks, playlists
from sqlalchemy import select, insert, func, update, distinct, and_, text
from oauth import create_spotify_client
import time
#from SCDplaylistsupdate import get_playlist_contents, get_playlists_info, get_all_playlists
from tracker import existing_artists, existing_tracks, existing_albums
from logger import log

sp = create_spotify_client()

# First get playlist contents and create playlist_tracks

#------------------------------------------get new playlist info--------------------------------------------------
#----------------------------------------get track reference info--------------------------------------
#---------------------------------------   Insert into album table --------------------------------------------------
#---------------------------------------  album ids in track reference ------------------------------------------------

def get_all_playlists():
    try:
        q = select(listening_two.c.context_id).where(
            and_(
                listening_two.c.context_type == 'playlist',
                listening_two.c.context_id.isnot(None)
            )
        ).union(
            select(playlists.c.playlist_id)
        )
        with engine.connect() as conn:
            sql_p_ids = [row[0] for row in conn.execute(q)]
        sp_user_p = []
        ofs = 0
        while True:
            page = safe_spotipy_call(sp.current_user_playlists, limit=50, offset=ofs)
            if not page or not page.get('items'):
                break
            sp_user_p.extend(p['id'] for p in page['items'])
            if page['next'] is None:
                break
            ofs += 50

        return set(sp_user_p + sql_p_ids)
    except Exception as e:
        log.error(f'tried to get all playlist ids {e}')
#                                                       insert new playlists and return playlist info for sql procedure
def get_playlists_info(playlist_ids):
    playlist_info = {}
    for i in playlist_ids:
        playlist = safe_spotipy_call(sp.playlist, i)

        if not playlist or "items" not in playlist:
            log.warning(f"No playlist info retrieved from Spotify. {i}")
            continue

        pid = playlist["id"]
        name = playlist["name"]
        owner = playlist["owner"]["id"]
        total = playlist["tracks"]["total"]

        playlist_info.update({
            pid:{
                "playlist_name": name,
                "owner_id": owner,
                "total_tracks": total}
        })

    return playlist_info


def get_playlist_contents(playlist_id):
    #with engine.begin() as conn:
    #    existing_track_ref = set(conn.execute(select(track_reference.c.track_id)).scalars().all())

    log.info(f'going to get tracks for playlist: {playlist_id}')
    ofs = 0
    playlist_contents = []
    seen_tracks = set()
    track_ref_info = []
    artist_ids = set()
    album_ids = set()
    while True:
        log.info(f"getting playlist {playlist_id} tracks from offset {ofs}")
        items = safe_spotipy_call(
            sp.playlist_items, playlist_id=playlist_id, limit=100, offset=ofs).get('items',[])

        if not items:
            log.info(f"No more tracks. {playlist_id} has {len(playlist_contents)} items")
            table_info = {'playlist_tracks': playlist_contents,
                          'track_reference':track_ref_info,
                          'artists':artist_ids,
                          'albums':album_ids}
            return table_info

        for item in items:
            track = item.get('track')
            if not track:
                log.warning(f'No track in playlist id {playlist_id}?')
                continue
            if track not in existing_tracks:
                if track not in seen_tracks:
                    track_id = track['id']
                    track_name = track['name']
                    album = track['album']['name']
                    album_id = track['album']['id']
                    artists_v = [i['id']for i in track['artists']]

                    artist_ids.update(i for i in artists_v if i not in existing_artists)
                    if album_id not in existing_albums:
                        album_ids.add(album_id)

                    track_ref_info.append({
                        'track_id' : track_id,
                        'track_name' : track_name,
                        'album' : album,
                        'album_id' : album_id,
                        'artist_id' : artists_v[0],
                        'collab_artist' : artists_v[1] if len(artists_v) > 1 else None
                    })
                    seen_tracks.add(track_id)

            playlist_contents.append({
                'track_id': track['id'],
                'added_at':str(datetime.fromisoformat(item['added_at'].replace('Z', '+00:00'))),
                'added_by':item['added_by']['id'],
                'playlist_id':playlist_id,
                'downloaded':item['is_local']
            })
        ofs += 100
        time.sleep(0.5)


def get_album_info(my_albums):
    try:
        if not my_albums:
            print('no albums')
            return []

        with engine.begin() as conn:
            existing_albums = set(conn.execute(select(albums.c.album_id)).scalars().all())
            filtered_album_ids = [aid for aid in my_albums if aid not in existing_albums]

        album_info = []
        for ids in chunked(filtered_album_ids, 20):
            corresponding = safe_spotipy_call(sp.albums, ids)
            albums_list = [a for a in (corresponding.get('albums') or []) if a is not None]
            if not albums_list:
                log.warning(f'no albums from sp: {ids}')
                continue
            for album in corresponding['albums']:

                album_id = album['id']
                album_type = album['album_type']
                total_tracks = album['total_tracks']
                album_name = album['name']
                release_date = album['release_date']
                ar = album['artists']
                artist_ids = [a['id']for a in ar]
                collab_artist = album['artists'][1]['id'] if len(album['artists']) > 1 else None
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

            time.sleep(2)
        if not album_info:
            log.warning('no album info')
            return
        log.info(f'returned {len(album_info)} album info')
        return album_info
    except Exception as e:
        log.error(f'something happened in get album info {e}')



#---------------------------------------   Insert into artist table -------------------------------------------------
# --------------------------------- artist ids in track reference and in albums -------------------------------------

def get_artist_info(my_artists):
    try:
        if not my_artists:
            log.warning("No artists to update.")
            return []

        with engine.begin() as conn:
            existing_artists = set(conn.execute(select(artists.c.artist_id)).scalars().all())
            filtered_artist_ids = [aid for aid in my_artists if aid not in existing_artists if aid is not None]

        artist_info = []
        for ids in chunked(filtered_artist_ids, 50):
            log.info(f"Processing chunk: {ids}")
            corresponding = safe_spotipy_call(sp.artists, ids)
            for artist in corresponding["artists"]:
                artist_info.append({
                    "artist_id": artist["id"],
                    "artist_name": artist["name"],
                    "artist_followers": artist["followers"]["total"],
                    "artist_genres": artist["genres"],
                    "artist_popularity": artist["popularity"],
                })
            time.sleep(2)
        if not artist_info:
            log.warning("Nothing to update in artists.")
            return
        log.info(f'returning {len(artist_info)} artist info')
        return artist_info
    except Exception as e:
        log.error(f'something happend in get artist info {e}')


def populate_tables_with_playlists_pipeline():
    """Look for new playlists in listening_two, get track ids with playlists contents then album and artist ids with
    get track reference info. insert into artists, albums, track reference, playlists and playlist tracks in order"""
    try:
        playlist_ids = get_all_playlists()
        p_info = get_playlists_info(playlist_ids)

        for i in playlist_ids:
            table_data = get_playlist_contents(i)
            if table_data is None:
                log.warning(f'no playlist contents for playlist {i}')
                continue

            if table_data.get('track_reference'):
                track_reference_info = table_data['track_reference']
                tref_artist_ids = table_data['artists']
                album_ids = table_data['albums']

                if tref_artist_ids or album_ids:
                    album_info = get_album_info(album_ids)
                    if not album_info:
                        log.warning('album_ids passed to spotify but no info')
                        artist_ids = tref_artist_ids
                    else :
                        album_artist_ids = [aid for i in album_info for aid in i['artist_id']]
                        artist_ids = set(album_artist_ids + tref_artist_ids)

                    log.info(f'artist_ids: {artist_ids}')
                    artist_info = get_artist_info(artist_ids)

                    for album in album_info:
                        album['artist_id'] = album['artist_id'][0]
                    for track in track_reference_info:
                        track['artist_id'] = track['artist_id'][0]

                    log.info('going to try to insert new artists and albums')
                    insert_into_sql(artists, artist_info)
                    insert_into_sql(albums, album_info)
                log.info('going to try to insert track ref')
                insert_into_sql(track_reference, track_reference_info)

            playlist_contents = table_data['playlist_tracks']
            playlist_id = i
            playlist_name = p_info[i]['name']
            owner_id = p_info[i]['owner']
            total_tracks = p_info[i]['total']
            with engine.connect() as conn:
                log.info('starting sql procedure')
                stmt = text(
                    "CALL merge_playlist_contents(:playlist_contents, :playlist_id, :playlist_name, :owner_id, :total_tracks)")
                conn.execute(
                    stmt,
                    {
                        "playlist_contents": json.dumps(playlist_contents),
                        "playlist_id": playlist_id,
                        "playlist_name": playlist_name,
                        "owner_id": owner_id,
                        "total_tracks": total_tracks
                    }
                )
                conn.commit()
                log.info('finished sql procedure commit')

    except KeyboardInterrupt:
        log.info('cancelled')
    except Exception as e:
        log.error(e)


def populate_track_ref_with_album_contents():
    log.info('populate_track_ref_with_album_contents began')
    try:
        with engine.begin() as conn:
            my_albums = set(conn.execute(select(albums.c.album_id)).scalars().all())
            existing_tracks = set(conn.execute(select(track_reference.c.track_id)).scalars().all())
            existing_full_albums = set(conn.execute(text("select a.album_id, a.total_tracks, count(t.track_id) as c\
                                                         from albums a left join track_reference on t.album_id = a.album_id\
                                                         group by a.album_id\
                                                         having c == a.total_tracks")))

        all_artists = set()
        new_track_filter = set()
        new_track_ref = []
        for i in my_albums:
            if i in existing_full_albums:
                log.info(f'skipping {i}! already has full contents')
                continue
            log.info(f'going to get album contents for: {i}')
            tracks = safe_spotipy_call(sp.album_tracks, i, limit=50)
            if not tracks.get('items'):
                log.warning(f'no tracks in {i}')

            if len(tracks) > 50:
                ofs = 50
                all_tracks = []
                while True:
                    log.info(f"getting playlist {i} tracks from offset {ofs}")
                    additional_tracks = safe_spotipy_call(sp.album_tracks, i, limit=50, offset=ofs)
                    all_tracks.append(additional_tracks)
                    if not additional_tracks:
                        log.info(f"No more tracks. {i} has {len(all_tracks)} items")
                        break
                tracks = all_tracks

            for track in tracks['items']:
                if track['id'] in existing_tracks:
                    continue
                if track['id'] in new_track_filter:
                    continue
                new_track_filter.add(track['id'])
                new_track_ref.append({
                    'track_id': track['id'],
                    'track_name': track["name"],
                    'album_id': i,
                    'artist_id': track['artists'][0]['id'],
                    'collab_artist': track['artists'][1]['id'] if len(track['artists']) > 1 else None
                    # 'duration_ms': track['duration_ms']
                })
                artist_ids = [i['artist_id'] for i in track if 'artist_id' in i]
                all_artists.update(artist_ids)
            time.sleep(2)

        if all_artists:
            log.info(f'all artists has {len(all_artists)} ids')
            artist_info = get_artist_info(all_artists)
            insert_into_sql(artists, artist_info)
        if new_track_ref:
            log.info(f'new track reference has {len(new_track_ref)} records')
            insert_into_sql(track_reference, new_track_ref)
    except KeyboardInterrupt:
        log.info('stopped!')
    except Exception as e:
        log.error(e)



if __name__ == '__main__':
    populate_tables_with_playlists_pipeline()
    populate_track_ref_with_album_contents()



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