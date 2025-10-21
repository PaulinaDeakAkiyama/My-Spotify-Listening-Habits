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
                filename = os.path.join('previews', f'{safe_name}_{track_id}')
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

def get_wav_from_previews():
    preview_folder = 'previews'
    file_paths = [os.path.join(preview_folder, f) for f in os.listdir(preview_folder)]
    print(f'{file_paths}')
    wav_files = []

    for path in file_paths:
        # First conversion
        wav_file = os.path.join(preview_folder, os.path.splitext(os.path.basename(path))[0] + '.wav')
        subprocess.run([
            'ffmpeg', '-hide_banner','-y', '-i', path,
            '-t', '28',
            '-ar', '44100',
            '-ac', '2',
            '-c:a', 'pcm_s16le',
            wav_file
        ], check=True)

        size = os.stat(wav_file).st_size
        print(f"Converted WAV size: {size} bytes ({size / 1_000_000:.2f} MB)")

        # Re-convert if >5MB
        if size >= 5_000_000:
            subprocess.run([
                'ffmpeg', '-y', '-i', path,
                '-t', '27',
                '-ar', '44100',
                '-ac', '2',
                '-c:a', 'pcm_s16le',
                wav_file
            ], check=True)
            new_size = os.stat(wav_file).st_size
            print(f"Re-converted (was too large). New size: {new_size / 1_000_000:.2f} MB")
        wav_files.append(wav_file)
    return wav_files

def upload_wav_get_features(wav_file):
    url = "https://api.reccobeats.com/v1/analysis/audio-features"
    with open(wav_file, 'rb') as f:
        files = {'audioFile': ('trimmed.wav', f, 'audio/wav')}
        try:
            response = requests.post(url, files=files)
            if response.status_code == 200:
                data = response.json()
                file_name = os.path.basename(wav_file)
                name_part = os.path.splitext(wav_file)[0]
                track_id = name_part.split('_')[-1]
                data['track_id'] = track_id
                print("Uploaded successfully")
                time.sleep(0.5)
                return data
            else:
                print(f"Error {response.status_code}: {response.text}")
                return None
        except Exception as e:
            print("Request failed:", e)
    return None

def feature_simultaneously(files, max_workers=10):
    features = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(upload_wav_get_features, file)for file in files]
        concurrent.futures.wait(futures)
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                features.append(result)
        return features


def insert_features_from_wav_file(file_path):
file_paths = os.listdir('previews')
wav_files = [
    os.path.join('previews', f)
    for f in file_paths
    if f.lower().endswith('.wav')
]
while len(wav_files) > 1:
    for wav_files_ten in chunked(wav_files, 10):
        try:
            tracks = len(wav_files)
            print(f'\nThere are {tracks} left. {datetime.now()}')
            features = []
            for attempt in range(5):
                try:
                    print(f'\ngoing to attempt uploading tracks {wav_files_ten}\n')
                    ten_feature = feature_simultaneously(wav_files_ten)
                    features.extend(ten_feature)
                    break
                except Exception as e:
                    print(f'{e}\n going to retry in 10 seconds')
                    time.sleep(10)
                    if attempt == 4:
                        print('max retried reached, skipping')
                        break
                time.sleep(5)

            if features:
                print(f'\ngoing to try to insert 10 new values...\n{features}\n')
                try:
                    with engine.begin() as conn:
                        conn.execute(insert(track_features), features)
                    print('nice.')
                    for wav_file in wav_files_ten:
                        os.remove(wav_file)
                        print(f"{wav_file} has been removed successfully")
                except Exception as e:
                    print(f'couldnt insert into table {e}')
        except KeyboardInterrupt:
            print('Stopped!')
    if len(wav_files) == 0:
        print('no more wav files!')
        break