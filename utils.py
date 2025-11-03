import threading
import time, re, requests
from db import engine
from sqlalchemy import insert, select, func, text
import spotipy

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
            log_to_sql(f"safe_request:{method.__name__}", "fail", f"{url} failed: {type(e).__name__} {str(e)}")
            time.sleep(3)
            continue
    return None

def safe_spotipy_call(method, *args, max_retries=3, delay=3, **kwargs):
    for attempt in range(max_retries):
        try:
            return method(*args, **kwargs)
        except spotipy.SpotifyException as e:
            print(f"Attempt {attempt+1}: Spotify error: {e}")
            time.sleep(delay)
        except Exception as e:
            print(f"Attempt {attempt+1}: Unknown error: {e}")
            time.sleep(delay)
    log_to_sql(f'safe spotipy call: {method.__name__}', 'failed', f'finished retries: {type(e).__name__} {str(e)}')
    return None
