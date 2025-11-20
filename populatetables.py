import json
from datetime import datetime
from PIL.ImageChops import offset
from utils import safe_spotipy_call, chunked, log_to_sql, insert_into_sql, safe_request, get_existing_ids
from db import engine, track_reference, listening_two, albums, artists, playlist_tracks, playlists
from sqlalchemy import select, insert, func, update, distinct, and_, text
from oauth import create_spotify_client
import time
from logger import log

sp = create_spotify_client()

def get_album_info(album_ids):
    try:
        if not album_ids:
            print('no albums')
            return []

        album_info = []
        for ids in chunked(album_ids, 20):
            corresponding = safe_spotipy_call(sp.albums, ids)
            albums_list = [a for a in (corresponding.get('albums') or []) if a is not None]
            if not albums_list:
                log.warning(f'no albums from this batch of ids: {ids}')
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
        log.info(f'returning {len(album_info)} album info')
        return album_info
    except Exception as e:
        log.error(f'something happened in get album info {e}')


def get_artist_info(artist_ids):
    try:
        if not artist_ids:
            log.warning("No artists to update.")
            return []

        artist_info = []
        for ids in chunked(artist_ids, 50):
            log.info(f"Processing chunk: {ids}")
            corresponding = safe_spotipy_call(sp.artists, ids)
            for artist in corresponding["artists"]:
                artist_info.append({
                    "artist_id": artist["id"],
                    "artist_name": artist["name"],
                    "followers": artist["followers"]["total"],
                    "genres": artist["genres"],
                    "popularity": artist["popularity"],
                })
            time.sleep(2)
        if not artist_info:
            log.warning("Nothing to update in artists.")
            return
        log.info(f'returning {len(artist_info)} artist info')
        return artist_info
    except Exception as e:
        log.error(f'something happend in get artist info {e}')

def get_all_playlists():
    try:
        sp_user_p = []
        ofs = 0
        while True:
            page = safe_spotipy_call(sp.current_user_playlists, limit=50, offset=ofs)
            if not page or not page.get('items'):
                break
            sp_user_p.extend(p['id'] for p in page['items'])
            if page.get("next") is None:
                break
            ofs += 50
        current_user_playlists = set(sp_user_p)

        q = (
            select(playlists.c.playlist_id)
            .where(
                and_(
                    playlists.c.valid_to == '3000-01-01 01:00:00',
                    playlists.c.saved == '1'
                )
            )
        ).union(
            select(listening_two.c.context_id)
            .where(
                and_(
                    listening_two.c.context_type == 'playlist',
                    listening_two.c.context_id.isnot(None),
                    listening_two.c.playlist_fk == 1
                )
            )
        )

        with engine.connect() as conn:
            sql_p_ids = set(row[0] for row in conn.execute(q))

        not_saved = set(sql_p_ids - current_user_playlists)

        return sql_p_ids - set('foreign_playlist_id'), not_saved
    except Exception as e:
        log.error(f'tried to get all playlist ids {e}')

def get_playlists_info(playlist_id):
    try:
        existing_tracks = get_existing_ids(track_reference)
        playlist_contents = []
        playlist_info = {}
        track_ref_info = []
        none_tracks = 0
        seen_tracks = set()
        next_ = True
        url = f'https://api.spotify.com/v1/playlists/{playlist_id}'
        method = 'GET'

        while next_:
            log.info(f"getting playlist {playlist_id} tracks")
            playlist = safe_request(method=method, url=url)

            if not playlist:
                return False

            if not playlist.get('tracks', {}).get('items',{}):
                #nonetype object has no attribute get
                items = playlist['items']
                next_url = playlist['next']
            else:
                items = playlist['tracks']['items']
                next_url = playlist['tracks']['next']

                playlist_info.update({
                    playlist_id: {
                        "name": playlist['name'],
                        "owner": playlist['owner']['id'],
                        "total": playlist['tracks']['total']
                    }
                })

            if not next_url:
                log.info('No more next... last batch of tracks')
                next_ = False


            for item in items:
                if not item.get('track', {}) or item.get('track').get('id', []) is None:
                    log.warning(f'Missing track in playlist id {playlist_id}\n {item}?')
                    none_tracks += 1
                    deleted_track_id = f'deleted_{none_tracks}'
                    playlist_contents.append({
                        'track_id': deleted_track_id,
                        'added_at': item['added_at'],
                        'added_by': item['added_by']['id'],
                        'playlist_id': playlist_id,
                        'downloaded': item['is_local']
                    })
                    log.warning(deleted_track_id)
                    continue

                track = item.get('track')
                if track['id'] not in existing_tracks:
                    if track['id'] not in seen_tracks:
                        log.info('New track ref info!')
                        artists_v = [a['id'] for a in track['artists']]
                        track_ref_info.append({
                            'track_id': track['id'],
                            'track_name': track['name'],
                            'album_id': track['album']['id'],
                            'artist_id': artists_v[0],
                            'collab_artist': artists_v[1] if len(artists_v) > 1 else None
                        })
                        seen_tracks.add(track['id'])
                        log.info(f'added track to new ref! {track['name']}')

                playlist_contents.append({
                    'track_id': item['track']['id'],
                    'added_at': str(datetime.fromisoformat(item['added_at'].replace('Z', '+00:00'))),
                    'added_by': item['added_by']['id'],
                    'playlist_id': playlist_id,
                    'downloaded': item['is_local']
                })

            url = next_url
            log.info('getting next tracks')
            time.sleep(1)

        playlist_info[playlist_id]['total'] = playlist_info[playlist_id]['total'] - none_tracks
        table_data = {'playlist_info': playlist_info, 'playlist_contents': playlist_contents, 'track_reference':track_ref_info}
        log.info(f'Done getting tracks for playlist {playlist_id}')
        return table_data
    except Exception as e:
        log.error(e)

def populate_playlists_pipeline():
    """Look for new playlists in listening_two, get track ids with playlists contents then album and artist ids with
    get track reference info. insert into artists, albums, track reference, playlists and playlist tracks in order"""
    try:
        playlist_ids, not_saved_p_ids = get_all_playlists()

        for i in playlist_ids:
            table_data = get_playlists_info(i)

            if not table_data:
                q = text(f""" 
                    UPDATE playlists SET valid_to = :valid_to
                    WHERE playlist_id = :playlist_id
                    AND valid_to = '3000-01-01 01:00:00'
                                """)
                with engine.begin() as conn:
                    conn.execute(q, {'playlist_id': i, 'valid_to': datetime.now()})

                log.warning(f'invalidated playlist that no longer exists {i}')
                continue

            p_info = table_data['playlist_info']

            track_reference_info = table_data.get('track_reference', [])
            playlist_contents = table_data['playlist_contents']
            playlist_id = i
            playlist_name = p_info[i]['name']
            owner_id = p_info[i]['owner']
            total_tracks = p_info[i]['total']
            log.info(f'sending to procedure\n playlist contents:{len(playlist_contents)}\n pid:{playlist_id}\n name: {playlist_name}\n owner: {owner_id}\n total: {total_tracks}\n tracks: {len(track_reference_info)}')
            try:
                raw_conn = engine.raw_connection()
                cursor = raw_conn.cursor()
                log.info('starting sql procedure')
                sql = (
                    "CALL merge_playlist_contents("
                    "%s, %s, %s, %s, %s, %s)"
                )
                cursor.execute(
                    sql,
                    (
                        json.dumps(playlist_contents),
                        playlist_id,
                        playlist_name,
                        owner_id,
                        total_tracks,
                        json.dumps(track_reference_info)
                    )
                )
                while True:
                    results = cursor.fetchall()
                    if results:
                        print("Result set:")
                        for row in results:
                            print(row)

                    if not cursor.nextset():
                        break

                raw_conn.commit()
                log.info('finished sql procedure commit')
            except Exception as e:
                log.error(f'procedure error {type(e).__name__} {str(e)}')
                raw_conn.rollback()
                log.warning('rollback')
            finally:
                cursor.close()
                raw_conn.close()

            update_missing_albums_artists()

        if not_saved_p_ids:
            for i in not_saved_p_ids:
                q = text(f""" 
                    UPDATE playlists SET saved = 0
                    WHERE playlist_id = :playlist_id
                    AND valid_to = '3000-01-01 01:00:00'
                """)
                with engine.begin() as conn:
                    conn.execute(q, {'playlist_id':i})

                log.warning(f'found playlist that isnt saved {i}')

    except KeyboardInterrupt:
        log.info('cancelled')
    except Exception as e:
        log.error(f'error! {type(e).__name__} {str(e)}')


def update_missing_albums_artists():
    log.info('going to start updating album info and artist info')
    try:
        with engine.begin() as conn:
            only_album_ids = set(conn.execute(
                 select(albums.c.album_id)
                 .where(albums.c.album_name == None)
                 ).scalars().all()
            )
            only_artist_ids = set(conn.execute(
                select(artists.c.artist_id)
                .where(artists.c.followers == None)
                ).scalars().all()
            )

        if only_artist_ids:
            artist_info = get_artist_info(only_artist_ids)
            update_stmt = text("""
                    UPDATE artists
                    SET artist_name = :name,
                        followers = :followers,
                        popularity = :popularity,
                        genres = :genres,
                        updated_at = :updated_at
                    WHERE artist_id = :artist_id
                """)
            with engine.begin() as conn:
                for a in artist_info:
                    r = conn.execute(
                        update_stmt,
                            {
                            "artist_id": a['artist_id'],
                            "name": a['artist_name'],
                            "followers": a['followers'],
                            "genres": json.dumps(a["genres"]),
                            "popularity": a["popularity"],
                            "updated_at": datetime.now()
                            }
                    )
                    print(r.rowcount)
                    print(a['artist_id'], a['artist_name'])
                log.info('updated artists')

        existing_artists = get_existing_ids(artists)

        if only_album_ids:
            album_info = get_album_info(only_album_ids)
            album_artists = set()
            for a in album_info:
                artist_list = a.get("artist_id", [])
                for artist_id in artist_list:
                    if artist_id not in existing_artists:
                        album_artists.add(artist_id)
                a['artist_id'] = a['artist_id'][0]

            if album_artists:
                artist_info = get_artist_info(album_artists)
                insert_into_sql(artists, artist_info)

            update_stmt = text("""
                            UPDATE albums
                            SET album_name = :album_name,
                                album_type = :album_type,
                                total_tracks = :total_tracks,
                                release_date = :release_date,
                                label = :label,
                                popularity = :popularity,
                                artist_id = :artist_id,
                                collab_artist = :collab_artist
                            WHERE album_id = :album_id
                        """)
            with engine.begin() as conn:
                for a in album_info:
                    conn.execute(
                        update_stmt,
                        {
                            'album_id':a['album_id'],
                            'album_name':a['album_name'],
                            'album_type':a['album_type'],
                            'total_tracks':a['total_tracks'],
                            'release_date':a['release_date'],
                            'artist_id':a['artist_id'],
                            'collab_artist':a['collab_artist'],
                            'label':a['label'],
                            'popularity':a['popularity']
                        }
                    )
    except Exception as e:
        log.error(f'error:{type(e).__name__} {str(e)}')


def populate_track_ref_with_album_contents():
    try:
        with engine.begin() as conn:
            # my_albums = set(conn.execute(select(albums.c.album_id)).scalars().all())
            existing_incomplete_albums = set(conn.execute(
                text(
                    "select a.album_id, a.total_tracks, count(t.track_id) as c\
                     from albums as a left join track_reference as t on t.album_id = a.album_id\
                     group by a.album_id\
                     having c != a.total_tracks"
                )).scalars().all()
            )
        print(len(existing_incomplete_albums))
        new_track_filter = set()
        if not existing_incomplete_albums:
            log.warning('all albums have full contents in sql')
            return
        for chunk in chunked(existing_incomplete_albums, 50):

            existing_tracks = get_existing_ids(track_reference)
            existing_artists = get_existing_ids(artists)
            log.info(f'Processing album contents from new chunk')
            all_artists = set()
            new_track_ref = []
            for i in chunk:
                tracks = safe_spotipy_call(sp.album_tracks, i, limit=50)
                if not tracks.get('items'):
                    log.warning(f'no tracks in {i}')
                tracks_items = tracks['items']

                if len(tracks['items']) >= 50:
                    ofs = 50
                    while True:
                        log.info(f"album has more than 50 tracks. getting album tracks from offset {ofs}")
                        additional_tracks = safe_spotipy_call(sp.album_tracks, i, limit=50, offset=ofs)
                        tracks_items.extend(additional_tracks['items'])
                        ofs += 50
                        if not additional_tracks.get('items'):
                            log.info(f"No more tracks. {i} has {len(tracks['items'])} items")
                            break
                else:
                    log.info(f'album{i} has {len(tracks['items'])}')
                c = 0
                o = 0
                for track in tracks_items:
                    if track['id'] in existing_tracks:
                        print(track['id'])
                        c += 1
                        continue
                    if track['id'] in new_track_filter:
                        o +=1
                        continue
                    new_track_filter.add(track['id'])
                    artists_ = track['artists']
                    new_track_ref.append({
                        'track_id': track['id'],
                        'track_name': track["name"],
                        'album_id': i,
                        'artist_id': artists_[0]['id'],
                        'collab_artist': artists_[1]['id'] if len(artists_) > 1 else None
                        # 'duration_ms': track['duration_ms']
                    })
                    artist_ids = [i['id'] for i in artists_ if i['id'] not in existing_artists]
                    all_artists.update(artist_ids)

                log.warning(f'existing {c}?')
                log.warning(f'filtered? {o}')
                time.sleep(1)

            if all_artists:
                log.info(f'all artists has {len(all_artists)} ids')
                artist_info = get_artist_info(all_artists)
                insert_into_sql(artists, artist_info)
            if new_track_ref:
                log.info(f'new track reference has {len(new_track_ref)} records')
                insert_into_sql(track_reference, new_track_ref)
            log.info('completed chunk 0f 20 albums')

    except KeyboardInterrupt:
        log.info('stopped!')
    except Exception as e:
        log.error(e)

if __name__ == '__main__':
    populate_playlists_pipeline()
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