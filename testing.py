import concurrent.futures
from oauth import get_spotify_auth
from db import engine, my_tracks, track_features
from sqlalchemy import insert, select, func
from datetime import datetime, time, timezone, timedelta
import time, os
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from tracker import save_last_50_tracks

import requests

sp = get_spotify_auth()
os.makedirs("previews", exist_ok=True)

def chunked(iterable, size):
    lst = list(iterable)
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def chunked_d(d, size):
    items = list(d.items())
    for i in range(0, len(items), size):
        yield dict(items[i:i + size])

def get_missing_names_artists():
    with engine.connect() as conn:
        result = conn.execute(
            select(my_tracks.c.track_name, my_tracks.c.artist_name)
            .where(my_tracks.c.track_id.notin_(
                select(track_features.c.track_id))
            )
        ).all()
        track_names_and_artists = {a: b for a, b in result}
    print(f'Found {len(track_names_and_artists)} missing tracks')
    return(track_names_and_artists)


def get_preview_url():
    tracks_artists = get_missing_names_artists()

    for chunk in chunked_d(tracks_artists, 10):
        preview_urls = []
        for key in chunk.keys():
            value = chunk[key]
            url = f'https://api.deezer.com/search?q=artist:"{value}" track:"{key}"'

            try:
                response = requests.get(url, timeout=10).json()
                if response:
                    if response.get('data') and response['data'][0].get('preview'):
                        preview_url = response['data'][0]['preview']
                        preview_urls.append(preview_url)
                        print(f'got preview url for {key}: {preview_url}')
                    else:
                        print(f'no data for {key}:{value}')
                else:
                    print(f'no response for {key}:{value}')
            except Exception as e:
                print(f'oops {e}')
            time.sleep(0.5)
        yield preview_urls


def download_preview(url):
    try:
        with requests.Session() as session:
            response = session.get(url, timeout=20)
            if response.status_code == 200:
                filename = os.path.join('previews', url.split('/')[-1][:36])
                if os.path.exists(filename):
                    print(f"⏭ Already exists: {filename}")
                    return

                with open(filename, 'wb') as f:
                    f.write(response.content)
                print(f'downloaded successfully{url}\n file path = {filename}')
            else:
                print(f'failed! response code: {response.status_code},{response.text},{url}')
    except Exception as e:
        print(f'error! {e}')



def download_simultaneously(urls, max_workers=10):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(download_preview, url) for url in urls]
        concurrent.futures.wait(futures)


def get_audio_files():
    for urls in get_preview_url():
        download_simultaneously(urls)

get_audio_files()
# url = 'https://cdnt-preview.dzcdn.net/api/1/1/0/e/b/0/0eb9a0f6ef2b30b639b2865f7a918e90.mp3?hdnea=exp=1760890054~acl=/api/1/1/0/e/b/0/0eb9a0f6ef2b30b639b2865f7a918e90.mp3*~data=user_id=0,application_id=42~hmac=912ecb9c6078b1f844180c39b034758c7e447d4f51f69577b3d72f691ff61dc6'
# print(f'{os.path.join('previews', url.split('/')[-1][:36])}')




