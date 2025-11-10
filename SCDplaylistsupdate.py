import json

from oauth import create_spotify_client
from db import engine, playlists, playlist_tracks, track_reference, listening_two, artists, albums
from sqlalchemy import insert, select, func, update, text, union, and_
from datetime import datetime, timezone
import time, logging

from tracker import existing_artists, existing_tracks, existing_albums
from utils import safe_spotipy_call, chunked, log_to_sql, insert_into_sql
from logger import log

#snapshot_id

sp = create_spotify_client()


def db_fetch_scalar(query):
    with engine.connect() as conn:
        return conn.execute(query).scalars().all()


def db_execute(stmt):
    with engine.begin() as conn:
        conn.execute(stmt)
# -------------------------------- Playlist logic (Type 2 SCD)-----------------------------------------------------

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
        sp_user_info = safe_spotipy_call(sp.current_user_playlists)
        sp_user_p = [p['id'] for p in sp_user_info['items']]
        return sp_user_p + sp_user_info

    except Exception as e:
        log.error(f"tried to get all ids: {e}")
        return []
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
    offset = 0
    playlist_contents = []
    seen_tracks = set()
    track_ref_info = []
    artist_ids = set()
    album_ids = set()
    while True:
        log.info(f"getting playlist {playlist_id} tracks from offset {offset}")
        items = safe_spotipy_call(
            sp.playlist_items, playlist_id=playlist_id, limit=100, offset=offset).get('items',[])

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
        offset += 100
        time.sleep(0.5)



def update_tracks_and_playlists():
    playlist_ids = get_all_playlists()
    p_info = insert_new_playlists(playlist_ids)                                  #update playlist ids in sql                          #get all playlist ids in sql. make sure artists and albums are already populated
    for playlist_id in playlist_ids:                                    #check for changes for each id in a procedure
        log.info(f'going through playlist:{playlist_id}...')
        table_info = get_playlist_contents(playlist_id)
        track_ref_info = table_info.get('track_reference', [])
        if track_ref_info is not None:
            insert_into_sql(albums, table_info.get('albums'))
            insert_into_sql(artists, table_info.get('artists'))
            insert_into_sql(track_reference, track_ref_info)

        playlist_contents = table_info['playlist_tracks']
        playlist_name = p_info['name']
        owner_id = p_info['owner']
        total_tracks = p_info['total']
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




