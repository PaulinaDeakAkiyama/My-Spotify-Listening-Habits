import threading
import time, re, requests
from db import engine
from sqlalchemy import insert, select, func, text
import spotipy
from logger import log
from sqlalchemy.dialects.mysql import insert as mysql_insert
from oauth import oauth_header




thread_local = threading.local()

def log_to_sql(stage, status, message):
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO logging (stage, status, message) VALUES (:s, :st, :m)"),
            {"s": stage, "st": status, "m": str(message)[:5000]}
        )
def chunked(iterable, size):
    lst = list(iterable)
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def chunked_d(d, size):
    items = list(d.items())
    for i in range(0, len(items), size):
        yield dict(items[i:i + size])

def sanitize_filename(name):
    """Removes characters not allowed in filenames."""
    return re.sub(r'[\\/*?:"<>|]', '_', name)


def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session

def safe_request(method: str,url: str,headers=None,params=None,data=None,max_retries: int = 3,delay: float = 3.0):
    session = get_session()
    if headers is None:
        headers = oauth_header
    for attempt in range(max_retries):
        try:
            response = session.request(
                method, url,
                headers=headers,
                params=params,
                data=data
            )
            if response.status_code == 200:
                return response.json()
        except requests.RequestException as e:
            log.error(f"safe_request:{method.__name__}", "fail", f"{url} failed: {type(e).__name__} {str(e)}")
            time.sleep(3)
            continue
    return None


def safe_spotipy_call(method, *args, max_retries=3, delay=3, **kwargs):
    for attempt in range(max_retries):
        try:
            return method(*args, **kwargs)
        except spotipy.SpotifyException as e:
            log.warning(f"Attempt {attempt + 1}: Spotify error: {e}")
            if e.http_status == 429:
                wait = int(e.headers.get("Retry-After", 5))
                log.warning(f'Spotify rate limit! wait time:{wait}')
                time.sleep(wait)
            else:
                time.sleep(delay)
        except Exception as e:
            log.error(f"Attempt {attempt+1}: Unknown error: {e}")
            time.sleep(delay)
    log.fatal(f'safe spotipy call: {method.__name__}', 'failed', f'finished retries: {type(e).__name__} {str(e)}')
    return None


def insert_into_sql(table_name, info):
    try:
        if not info:
            if table_name.name == 'listening_two':
                log.warning('no start time?')
                data = {}
                with engine.begin() as conn:
                    conn.execute(insert(table_name), data)
            else:
                log.warning('no info to insert')
                return False

        elif isinstance(info, list):
            if not info:
                return
            with engine.begin() as conn:
                conn.execute(insert(table_name), info)
            log.info(f'saved {len(info)} records to {table_name.name}!')
            return True

        else:
            data = info.get(table_name.name, info) or info[table_name.name]
            if isinstance(data, list):
                with engine.begin() as conn:
                    conn.execute(insert(table_name), data)
                log.info(f'saved info {len(info)} to {table_name}!')
                return True

            stmt = mysql_insert(table_name).values(**data)
            stmt = stmt.on_duplicate_key_update(**{k: stmt.inserted[k] for k in data.keys()})
            with engine.begin() as conn:
                conn.execute(stmt)
            log.info(f'saved info to {table_name}!')
            return True

    except Exception as e:
        log.fatal(f'couldnt insert into {table_name.name}. {e}')
        log_to_sql('inserting to artists', 'failed', f"Error: {type(e).__name__} {e}")
        return False

def get_existing_ids(table):
    with engine.begin() as conn:
        if table.name == 'track_reference':
            table_id = 'track_id'
            stmt = text(f"SELECT {table_id} FROM {table.name} WHERE album_id IS NOT NULL")
        elif table.name == 'albums':
            table_id = 'album_id'
            stmt = text(f"SELECT {table_id} FROM {table.name}")
        elif table.name == 'artists':
            table_id = 'artist_id'
            stmt = text(f"SELECT {table_id} FROM {table.name}")

        existing_ids = set(conn.execute(stmt).scalars().all())

    return existing_ids

