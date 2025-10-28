import time, os, subprocess, re, requests, concurrent.futures
from db import engine, my_tracks, track_features
from sqlalchemy import insert, select, func, text
from oauth import create_spotify_client
from datetime import datetime, time as dtime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

thread_local = threading.local()
sp = create_spotify_client()
os.makedirs("previews", exist_ok=True)

#--------------------------------------------------------HELPERS---------------------------------------------------------

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
            log_to_sql("safe_request", "fail", f"{url} failed: {type(e).__name__} {str(e)}")
            time.sleep(3)
            continue
    return None

#-----------------------------------STAGE ONE, get missing audio features-------------------------------

def get_missing_tracks():
    """Return track_id:[track_name, track_artist] for all tracks with no features"""
    with engine.connect() as conn:
        result = conn.execute(
            select(my_tracks.c.track_id, my_tracks.c.track_name, my_tracks.c.artist_name)
            .where(
                my_tracks.c.track_id.not_in(
                    select(func.coalesce(track_features.c.track_id, 0))
                )
            )
        ).all()
        id_track_artist = {a: [b, c] for a, b, c in result}
    print(f'Found {len(id_track_artist)}')
    return id_track_artist

#----------------------------------------STAGE TWO find features with reccobeats-----------------------------------------------

def get_reccobeats_id(spotify_ids):

    url = f"https://api.reccobeats.com/v1/track?ids={','.join(spotify_ids)}"
    headers = {'Accept': 'application/json'}
    method = 'GET'
    tracks = safe_request(method, url, headers=headers)

    if not tracks or 'content' not in tracks or not tracks['content']:
        print('no corresponding reccobeat_id')
        return[]

    reccobeats_ids = [track['id'] for track in tracks['content'] if track['id']]
    return reccobeats_ids


def get_track_features(reccobeats_ids):
    url = f"https://api.reccobeats.com/v1/audio-features?ids={','.join(reccobeats_ids)}"
    headers = {'Accept': 'application/json'}
    batch_features = safe_request("GET", url, headers=headers)

    if not batch_features or 'content' not in batch_features or not batch_features['content']:
        print('no features')
        return []

    batch_track_features = []
    for features in batch_features['content']:
        batch_track_features.append({
            "track_id": features['href'][31:],
            "reccobeats_id": features['id'],
            "acousticness": features['acousticness'],
            "danceability": features['danceability'],
            "energy": features['energy'],
            "instrumentalness": features['instrumentalness'],
            "key_": features['key'],
            "loudness": features['loudness'],
            "mode_": features['mode'],
            "speechiness": features['speechiness'],
            "tempo": features['tempo'],
            "valence": features['valence']
        })
        print(batch_track_features)
    return batch_track_features


def save_track_features(id_track_artist):
    my_track_ids = [id for id in id_track_artist.keys()]
    new_track_features = []
    no_reccobeats_ids = []
    for spotify_ids in chunked(my_track_ids, 40):
        print(f'going through spotify ids: {spotify_ids}')

        reccobeats_ids = get_reccobeats_id(spotify_ids)
        if not reccobeats_ids:
            log_to_sql('save_track_features', 'failed', 'no reccobeats ids')
            no_reccobeats_ids.append(reccobeats_ids)
            continue

        print(f'\ngoing through reccobeats ids: {reccobeats_ids}\n')
        features = get_track_features(reccobeats_ids)
        print('got features successfully')
        new_track_features.extend(features)
        #time.sleep(0.10)
        break

    if new_track_features:
        print(f'going to try to insert new values...\n{new_track_features}')
        with engine.begin() as conn:
            conn.execute(insert(track_features), new_track_features)
        print('nice.')
        log_to_sql("save_track_features", "success", f"Inserted {len(new_track_features)} features")
        return
    else:
        print('no new track feature info')
        return no_reccobeats_ids

def save_track_features_wrapper():
    id_track_artist = get_missing_tracks()
    if not id_track_artist:
        log_to_sql("save_track_features", "info", "No new tracks found")
        return
    save_track_features(id_track_artist)

#-------------------------------STAGE 3 get preview url and download as mp3 for still missing tracks---------------------------------------

def get_preview_url():
    """requests a track preview url from deezer using the track name and artist"""
    tracks_artists = get_missing_tracks()

    for chunk in chunked_d(tracks_artists, 10):
        print(len(chunk))
        preview_urls = {}
        for id, (track, artist) in chunk.items():
            url = f'https://api.deezer.com/search?q=artist:"{artist}" track:"{track}"'
            response = safe_request('get', url)
            if response:
                if response.get('data') and response['data'][0].get('preview'):
                    preview_url = response['data'][0]['preview']
                    preview_urls.update({id: [track, preview_url]})
                    print(f'got preview url for {track}: {preview_url}')

                else:
                    print(f'no data for {track}:{artist}')
                    log_to_sql('get_preview_url', 'failed', 'no corresponding preview urls')
        yield preview_urls


def download_preview(track_id, track, url):
    """downloads a preview url if not already exists, saves it as the track id and track name"""
    safe_name = sanitize_filename(track)
    filename = os.path.join('previews', f'{safe_name}_{track_id}.mp3')

    if os.path.exists(filename):
        print(f"Already exists: {filename}")
        return
    try:
        session = get_session()
        response = session.get(url, timeout=20)

        if not response:
            print(f'failed to download {url}')
            log_to_sql('download_preview', 'fail', f'{track_id}|{url}')

        if response.status_code == 200:
            with open(filename, 'wb') as f:
                f.write(response.content)
            print(f'downloaded successfully{url}\n file path = {filename}')

    except Exception as e:
        print(f'error! {e}')


def download_previews_simultaneously(preview_items, max_workers=10):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(download_preview, track_id, track, url)
            for track_id, (track, url) in preview_items.items()
        ]
        concurrent.futures.wait(futures)

def get_mp3_files_from_missing_features():
    for preview_items in get_preview_url():
        download_previews_simultaneously(preview_items)

#---------------------------------------------STAGE 4 convert mp3 to wav------------------------------------------------------

def convert_to_wav(path):
    preview_folder = 'previews'
    wav_file = os.path.join(preview_folder, os.path.splitext(os.path.basename(path))[0]+'.wav')
    subprocess.run([
        'ffmpeg', '-hide_banner', '-y', '-i', path,
        '-t', '28', '-ar', '44100', '-ac', '2', '-c:a', 'pcm_s16le', wav_file
    ], check=True)
    return wav_file

def get_wavs_from_all_mp3():
    """will convert preview mp3s to wav files using ffmpeg and check that the file is under 5MB"""
    preview_folder = 'previews'
    file_paths = [os.path.join(preview_folder, f) for f in os.listdir(preview_folder)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        wav_files = list(executor.map(convert_to_wav, file_paths))
        log_to_sql("convert_to_wav", "success", "Converted 10 files")
        return wav_files

def convert_all_mp3_to_wav():
    try:
        wavs = get_wavs_from_all_mp3()
        log_to_sql("convert_to_wav", "success", f"{len(wavs)} wavs created")
    except Exception as e:
        log_to_sql("convert_to_wav", "fail", str(e))

#--------------------------------STAGE 5 upload wav and get audio features from reccobeats---------------------------------------

def upload_wav_get_features(wav_file):
    """Will get a response from reccobeats audio analysis api for one file and this will return a features json"""
    url = "https://api.reccobeats.com/v1/analysis/audio-features"
    with open(wav_file, 'rb') as f:
        files = {'audioFile': ('trimmed.wav', f, 'audio/wav')}
        try:
            response = requests.post(url, files=files)
            if response.status_code == 200:
                data = response.json()
                name_part = os.path.splitext(wav_file)[0]
                track_id = name_part.split('_')[-1]
                data['track_id'] = track_id
                print("Uploaded successfully")
                return data
            else:
                print(f"Error {response.status_code}: {response.text}")
                return None
        except Exception as e:
            print("Request failed:", e)
    return None


def feature_simultaneously(files, max_workers=10):
    """calls upload wav get features 10 times to speed up the process and saves features in a list of dictionaries"""
    features = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(upload_wav_get_features, file) for file in files]
        concurrent.futures.wait(futures)
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                features.append(result)
        return features


def insert_features_from_wav_file(wav_files):
    """continues calling feature_simultaneously for all wav files passed as an argument"""
    left_to_go = len(wav_files)
    try:
        for wav_files_ten in chunked(wav_files, 10):
            tracks = len(wav_files)
            print(f'\nThere are {tracks} left. {datetime.now()}')
            features = []
            for attempt in range(5):
                try:
                    print(f'\ngoing to attempt uploading tracks {wav_files_ten}\n')
                    ten_feature = feature_simultaneously(wav_files_ten)
                    features.extend(ten_feature)
                    time.sleep(5)
                    break
                except (TimeoutError, ConnectionError) as e:
                    print(f'{e}\n going to retry in 10 seconds')
                    time.sleep(10)
                    if attempt == 4:
                        print('max retried reached, skipping')
                        break

            if features:
                print(f'\ngoing to try to insert 10 new values...\n{features}\n')
                try:
                    with engine.begin() as conn:
                        conn.execute(insert(track_features), features)
                    print('nice.')
                    for wav_file in wav_files_ten:
                        os.remove(wav_file)
                        print(f"{wav_file} has been removed successfully")
                        print(f'{left_to_go} number of files left to go through')
                        left_to_go -= 10
                except Exception as e:
                    print(f'couldnt insert into table {e}')
    except KeyboardInterrupt:
        print('Stopped!')

def insert_features_from_all_wavs():
    file_paths = os.listdir("previews")
    wav_files = [os.path.join("previews", f) for f in file_paths if f.lower().endswith(".wav")]
    if not wav_files:
        log_to_sql("insert_features", "info", "No wav files found")
        return
    insert_features_from_wav_file(wav_files)


#----------------------------------------------------PIPELINE-----------------------------------------------------------------

def run_pipeline():
    stages = [
        ("get_missing_tracks", get_missing_tracks),
        ("save_track_features", save_track_features_wrapper),
        ("get_mp3_files", get_mp3_files_from_missing_features),
        ("convert_to_wav", convert_all_mp3_to_wav),
        ("insert_features", insert_features_from_all_wavs)
    ]

    with engine.connect() as conn:
        completed_stages = {r[0] for r in conn.execute(text("SELECT stage FROM logging WHERE status='success'"))}

    for name, func in stages:
        if name in completed_stages:
            print(f'skipping {name}, already success')
            continue

        log_to_sql(name, "running", f"Starting stage {name}")
        try:
            func()
            log_to_sql(name, "success", f"Stage {name} completed")
        except Exception as e:
            log_to_sql(name, "fail", f"Stage {name} failed: {e}")
            break

if __name__ == "__main__":
    log_to_sql("pipeline", "start", "Audio feature pipeline initiated")
    run_pipeline()
    log_to_sql("pipeline", "end", "Pipeline completed")


