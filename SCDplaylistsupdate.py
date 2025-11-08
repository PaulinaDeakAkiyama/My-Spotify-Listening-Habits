import json

from oauth import create_spotify_client
from db import engine, playlists, playlist_tracks, track_reference, listening_two  # track_features, artists, fact
from sqlalchemy import insert, select, func, update, text
from datetime import datetime, timezone
import time, logging
from utils import safe_spotipy_call, chunked, log_to_sql
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

def get_existing_playlists():
    try:
        q = select(playlists.c.playlist_id, playlists.c.playlist_name,
                   playlists.c.owner_id, playlists.c.total_tracks,
                   playlists.c.valid_to).where(playlists.c.valid_to == '3000-01-01 01:00:00')
        with engine.connect() as conn:
            return {row.playlist_id: row for row in conn.execute(q)}
    except Exception as e:
        log.error(f'tried to get existing ids: {e}')


#                                                       insert new playlists and return playlist info for sql procedure
def update_playlists():
    now = datetime.now(timezone.utc)
    user_playlists = safe_spotipy_call(sp.current_user_playlists)
    if not user_playlists or "items" not in user_playlists:
        log.warning("No playlists retrieved from Spotify.")
        return []

    current_db = get_existing_playlists()
    playlist_info = {}
    new_playlists = []
    for item in user_playlists["items"]:
        pid = item["id"]
        name = item["name"]
        owner = item["owner"]["id"]
        total = item["tracks"]["total"]

        db_row = current_db.get(pid)
        if not db_row:
            new_playlists.append({
                "playlist_id": pid,
                "playlist_name": name,
                "owner_id": owner,
                "total_tracks": total
            })

        playlist_info.update({
             pid:{
            "playlist_name": name,
            "owner_id": owner,
            "total_tracks": total}
        })
    if new_playlists:
        db_execute(insert(playlists).values(new_playlists))
        log.info(f"Inserted {len(new_playlists)} new playlists.")
    return playlist_info


def get_playlist_contents(playlist_id):
    log.info(f'going to get tracks for playlist: {playlist_id}')
    offset = 0
    playlist_contents = []
    while True:
        log.info(f"getting playlist {playlist_id} tracks from offset {offset}")
        items = safe_spotipy_call(
            sp.playlist_items, playlist_id=playlist_id, limit=100, offset=offset).get('items',[])

        if not items:
            log.info(f"No more tracks. {playlist_id} has {len(playlist_contents)} items")
            return playlist_contents

        for item in items:
            track = item.get('track')
            if not track:
                log.warning(f'No track in playlist id {playlist_id}?')
                continue

            playlist_contents.append({
                'track_id': item['track']['id'],
                'added_at':str(datetime.fromisoformat(item['added_at'].replace('Z', '+00:00'))),
                'added_by':item['added_by']['id'],
                'playlist_id':playlist_id,
                'downloaded':item['is_local']
            })
        offset += 100
        time.sleep(0.5)


def update_tracks_and_playlists():
    playlist_ids = get_existing_playlists()
    playlist_info = update_playlists()                                  #update playlist ids in sql
    #playlist_ids = get_existing_playlists()                             #get all playlist ids in sql
    for playlist_id in playlist_ids:                                    #check for changes for each id in a procedure
        log.info(f'going through playlist:{playlist_id}...')
        if not playlist_info.get(playlist_id):
            log.fatal(f'no playlist info parameter, couldnt get. skipping {playlist_id}')
            continue
        info = playlist_info.get(playlist_id)
        playlist_name = info['playlist_name']
        owner_id = info['owner_id']
        total_tracks = info['total_tracks']
        playlist_contents = get_playlist_contents(playlist_id)
        print(playlist_contents)
        print(playlist_name)

        with engine.connect() as conn:
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



update_tracks_and_playlists()


