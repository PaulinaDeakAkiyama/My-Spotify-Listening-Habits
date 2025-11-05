from oauth import create_spotify_client
from db import engine, playlists, playlist_tracks, track_reference, listening_two  # track_features, artists, fact
from sqlalchemy import insert, select, func, update, text
from datetime import datetime, timezone
import time, logging
from utils import safe_spotipy_call, chunked, log_to_sql

log = logging.getLogger(__name__)
sp = create_spotify_client()

# -----------------------------update playlists and playlist contents------------------------------------------------
def db_fetch_scalar(query):
    with engine.connect() as conn:
        return conn.execute(query).scalars().all()


def db_execute(stmt):
    with engine.begin() as conn:
        conn.execute(stmt)


# -------------------------------- Playlist logic (Type 2 SCD)-----------------------------------------------------

def get_existing_playlists():
    q = select(playlists.c.playlist_id, playlists.c.playlist_name,
               playlists.c.owner_id, playlists.c.total_tracks,
               playlists.c.valid_to).where(playlists.c.valid_to.is_(None))
    with engine.connect() as conn:
        return {row.playlist_id: row for row in conn.execute(q)}


def update_playlists():
    now = datetime.now(timezone.utc)
    user_playlists = safe_spotipy_call(sp.current_user_playlists)
    if not user_playlists or "items" not in user_playlists:
        log.warning("No playlists retrieved from Spotify.")
        return []

    current_db = get_existing_playlists()
    new_records = []
    changed_records = []

    for item in user_playlists["items"]:
        pid = item["id"]
        name = item["name"]
        owner = item["owner"]["id"]
        total = item["tracks"]["total"]

        db_row = current_db.get(pid)
        if not db_row:
            new_records.append({
                "playlist_id": pid,
                "playlist_name": name,
                "owner_id": owner,
                "total_tracks": total,
                "valid_from": now,
                "valid_to": None,
            })
        else:
            changed = (
                db_row.playlist_name != name
                or db_row.total_tracks != total
            )
            if changed:
                changed_records.append(pid)
                db_execute(
                    update(playlists)
                    .where(playlists.c.playlist_id == pid, playlists.c.valid_to.is_(None))
                    .values(valid_to=now)
                )
                new_records.append({
                    "playlist_id": pid,
                    "playlist_name": name,
                    "owner_id": owner,
                    "total_tracks": total,
                    "valid_from": now,
                    "valid_to": None,
                })

    if new_records:
        db_execute(insert(playlists).values(new_records))
        log.info(f"Inserted {len(new_records)} playlist version(s).")

    return [item["id"] for item in user_playlists["items"]]


def get_playlist_contents(playlist_id):
    print(f'going to get tracks for playlist: {playlist_id}')
    playlist_info = safe_spotipy_call(sp.playlist_items, playlist_id=playlist_id)
    track_total = playlist_info['total']

    offset = 0
    playlist_contents = []
    while True:
        print(f"getting playlist {playlist_id} tracks from offset {offset}")
        items = safe_spotipy_call(
            sp.playlist_items, playlist_id=playlist_id, limit=100, offset=offset).get('items',[])

        if not items:
            print(f"No more tracks. {playlist_id} has {len(playlist_contents)} items")
            return playlist_contents

        print(f"playlist has: {track_total}")

        for item in items:
            track = item.get('track')
            if not track:
                continue

            playlist_contents.append({
                'track_id': item['track']['id'],
                'added_at':datetime.fromisoformat(item['added_at'].replace('Z', '+00:00')),
                'added_by':item['added_by']['id'],
                'playlist_id':playlist_id
            })

        offset += 100
        time.sleep(0.25)


def insert_new_tracks(new_tracks):
    try:
        with engine.begin() as conn:
            conn.execute(insert(playlist_tracks), new_tracks)
    except Exception as e:
        print(f"Error inserting tracks: {e}")


def update_tracks_and_playlists():
    my_playlists = update_playlists()

    all_playlist_contents = []
    for playlist_id in my_playlists:
        print(f'going through playlist:{playlist_id}...')
        playlist_contents = get_playlist_contents(playlist_id)
        all_playlist_contents.append(playlist_contents)

        with engine.connect() as conn:
            result = conn.execute(text(f"CALL merge_playlist_content({playlist_contents, playlist_id})"))



        track_ids = {id['track_id'] for id in playlist_contents}

        #new_tracks = [t for t in playlist_contents if t['track_id'] not in existing_tracks]
        #to_delete = [t for t in existing_tracks if t not in playlist_ids]






