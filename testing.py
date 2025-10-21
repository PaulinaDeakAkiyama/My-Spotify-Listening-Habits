import concurrent.futures
from oauth import get_spotify_auth
from db import engine, my_tracks, track_features
from sqlalchemy import insert, select, func
from datetime import datetime, time, timezone, timedelta
import time, os, subprocess, re
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
            select(my_tracks.c.track_id,my_tracks.c.track_name, my_tracks.c.artist_name)
            .where(my_tracks.c.track_id.notin_(
                select(track_features.c.track_id))
            )
        ).all()
        track_names_and_artists = {a:[b,c] for a,b,c in result}
    print(f'Found {len(track_names_and_artists)}')
    return(track_names_and_artists)


def get_preview_url():
    tracks_artists = get_missing_names_artists()

    for chunk in chunked_d(tracks_artists, 10):
        preview_urls = {}
        for id, (track, artist) in chunk.items():
            url = f'https://api.deezer.com/search?q=artist:"{artist}" track:"{track}"'
            try:
                response = requests.get(url, timeout=10).json()
                if response:
                    if response.get('data') and response['data'][0].get('preview'):
                        preview_url = response['data'][0]['preview']
                        preview_urls.update({id:[track, preview_url]})
                        print(f'got preview url for {track}: {preview_url}')
                    else:
                        print(f'no data for {track}:{artist}')
                else:
                    print(f'no response for {track}:{artist}')
            except Exception as e:
                print(f'oops {e}')
            time.sleep(0.5)
        yield preview_urls

def sanitize_filename(name):
    """Removes characters not allowed in filenames."""
    return re.sub(r'[\\/*?:"<>|]', '_', name)

def download_preview(track_id, track, url):
    safe_name = sanitize_filename(track)
    try:
        with requests.Session() as session:
            response = session.get(url, timeout=20)
            if response.status_code == 200:
                filename = os.path.join('previews', f'{safe_name}_{id}')
                if os.path.exists(filename):
                    print(f"Already exists: {filename}")
                    return

                with open(filename, 'wb') as f:
                    f.write(response.content)
                print(f'downloaded successfully{url}\n file path = {filename}')
            else:
                print(f'failed! response code: {response.status_code},{response.text},{url}')
    except Exception as e:
        print(f'error! {e}')


def download_simultaneously(preview_items, max_workers=10):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(download_preview, track_id, track, url)
            for track_id, (track, url) in preview_items.items()
        ]
        concurrent.futures.wait(futures)


def get_audio_files():
    for preview_items in get_preview_url():
        download_simultaneously(preview_items)

# url = 'https://cdnt-preview.dzcdn.net/api/1/1/0/e/b/0/0eb9a0f6ef2b30b639b2865f7a918e90.mp3?hdnea=exp=1760890054~acl=/api/1/1/0/e/b/0/0eb9a0f6ef2b30b639b2865f7a918e90.mp3*~data=user_id=0,application_id=42~hmac=912ecb9c6078b1f844180c39b034758c7e447d4f51f69577b3d72f691ff61dc6'
# print(f'{os.path.join('previews', url.split('/')[-1][:36])}')




import imageio_ffmpeg as ffmpeg

def get_features_from_wav():
    preview_folder = 'previews'
    file_paths = [os.path.join(preview_folder, f) for f in os.listdir(preview_folder)]
    wav_file = os.path.join(preview_folder, 'trimmed.wav')
    features = []
    for path in file_paths:
        subprocess.run([
            'ffmpeg', "-y", "-i", path,
            "-t", "28",  # trim to 30 seconds
            "-ar", "44100",  # sample rate 44.1kHz
            "-ac", "2",  # stereo
            "-c:a", "pcm_s16le",  # 16-bit PCM
            wav_file
        ])
        size = os.stat(wav_file).st_size
        print(f"Converted WAV size: {size} bytes ({size / 1_000_000:.2f} MB)")
        if not size < 5000000:
            subprocess.run([
                'ffmpeg', "-y", "-i", path,
                "-t", "27",  # trim to 30 seconds
                "-ar", "44100",  # sample rate 44.1kHz
                "-ac", "2",  # stereo
                "-c:a", "pcm_s16le",  # 16-bit PCM
                wav_file
            ])
            print(f"Size wasn't under 5mb, converted again\nConverted WAV size: {size} bytes ({size / 1_000_000:.2f} MB)")

        url = "https://api.reccobeats.com/v1/analysis/audio-features"
        with open(wav_file, "rb") as f:
            files = {"audioFile": ("trimmed.wav", f, "audio/wav")}
            try:
                response = requests.post(url, files=files)
                if response.status_code == 200:
                    print("Success:", response.json())
                    dict_response = dict(response.json())
                    dict_response['track_id'] = 'id'
                    dict_response['key'] = ''
                    dict_response['mode_'] = ''
                    features.append(dict_response)
                else:
                    print(f"Error {response.status_code}: {response.text}")
            except Exception as e:
                print("Request failed:", e)


def upload_simultaneously(urls, max_workers=10):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(get_features_from_wav(), url) for url in urls]
        concurrent.futures.wait(futures)


# preview_folder = 'previews'
# file_paths = [os.path.join(preview_folder, f) for f in os.listdir(preview_folder)]
# ffmpeg_path = ffmpeg.get_ffmpeg_exe()
# mp3_file = os.path.join(preview_folder, '00616718063a5211433291bc885282ee.mp3')
# wav_file = os.path.join(preview_folder, 'trimmed.wav')
#
#
# subprocess.run([
#     ffmpeg_path, "-y", "-i", mp3_file,
#     "-t", "28",          # trim to 30 seconds
#     "-ar", "44100",      # sample rate 44.1kHz
#     "-ac", "2",          # stereo
#     "-c:a", "pcm_s16le", # 16-bit PCM
#     wav_file
# ])
#

# size = os.stat(wav_file).st_size
# print(f"Converted WAV size: {size} bytes ({size/1_000_000:.2f} MB)")
#
# url = "https://api.reccobeats.com/v1/analysis/audio-features"
# files = {"audioFile": ("trimmed.wav", open(wav_file, "rb"), "audio/wav")}
#
# try:
#     response = requests.post(url, files=files)
#     if response.status_code == 200:
#         print("Success:", response.json())
#         print(dict(response.json()))
#     else:
#         print(f"Error {response.status_code}: {response.text}")
# except Exception as e:
#     print("Request failed:", e)
#