import time, os, subprocess, re, requests, concurrent.futures
from db import engine, playlist_tracks, track_features, track_reference
from sqlalchemy import insert, select, func, text
from oauth import create_spotify_client
from datetime import datetime, time as dtime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import chunked, chunked_d, safe_request, sanitize_filename, log_to_sql, get_session, safe_spotipy_call
from logger import log

sp = create_spotify_client()
os.makedirs("previews", exist_ok=True)


#-----------------------------------STAGE ONE, get missing audio features-------------------------------

def get_missing_tracks():
    """Return track_id:[track_name, track_artist] for all tracks with no features"""
    with engine.connect() as conn:
        result = conn.execute(
            select(track_reference.c.track_id, track_reference.c.track_name, track_reference.c.artist_name)
            .where(
                track_reference.c.track_id.not_in(
                    select(func.coalesce(track_features.c.track_id, 0))
                )
            )
        ).all()
        id_track_artist = {a: [b, c] for a, b, c in result}
    log.info(f'Found {len(id_track_artist)}')
    return id_track_artist

#----------------------------------------STAGE TWO find features with reccobeats-----------------------------------------------

def get_reccobeats_id(spotify_ids):

    url = f"https://api.reccobeats.com/v1/track?ids={','.join(spotify_ids)}"
    headers = {'Accept': 'application/json'}
    method = 'GET'
    tracks = safe_request(method, url, headers=headers)

    if not tracks or 'content' not in tracks or not tracks['content']:
        log.warning('no corresponding reccobeat_id')
        return[]

    reccobeats_ids = [track['id'] for track in tracks['content'] if track['id']]
    return reccobeats_ids


def get_track_features(reccobeats_ids):
    url = f"https://api.reccobeats.com/v1/audio-features?ids={','.join(reccobeats_ids)}"
    headers = {'Accept': 'application/json'}
    batch_features = safe_request("GET", url, headers=headers)

    if not batch_features or 'content' not in batch_features or not batch_features['content']:
        log.warning('no features corresponding to reccobeats ids')
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
    log.info(f'found {len(batch_track_features)} track features')
    return batch_track_features


def save_track_features(id_track_artist):
    if not id_track_artist:
        return
    my_track_ids = [id for id in id_track_artist.keys()]
    new_track_features = []
    no_reccobeats_ids = []
    for spotify_ids in chunked(my_track_ids, 40):
        log.info(f'going through spotify ids: {spotify_ids}')

        reccobeats_ids = get_reccobeats_id(spotify_ids)
        if not reccobeats_ids:
            log.warning('save_track_features','no reccobeats ids in batch, skipping')
            no_reccobeats_ids.append(reccobeats_ids)
            continue

        log.info(f'\ngoing through reccobeats ids: {reccobeats_ids}\n')
        features = get_track_features(reccobeats_ids)
        log.info('got features successfully')
        new_track_features.extend(features)
        time.sleep(0.10)
        break

    if new_track_features:
        log.info(f'going to try to insert new values...\n{new_track_features}')
        with engine.begin() as conn:
            conn.execute(insert(track_features), new_track_features)
        log_to_sql("save_track_features", "success", f"Inserted {len(new_track_features)} features")
        log.info(f"Nice. Inserted {len(new_track_features)} features in to sql")
        return
    else:
        log.warning('no new track feature info, returning remaining spotify ids with no corresponding reccobeats id')
        return no_reccobeats_ids

def save_track_features_wrapper():
    id_track_artist = get_missing_tracks()
    if not id_track_artist:
        log.warning("No missing tracks found")
        return
    save_track_features(id_track_artist)

#-------------------------------STAGE 3 get preview url and download as mp3 for still missing tracks---------------------------------------

def get_preview_url():
    """requests a track preview url from deezer using the track name and artist"""
    log.info('going to start download process for remaining missing tracks')
    tracks_artists = get_missing_tracks()

    for chunk in chunked_d(tracks_artists, 10):
        preview_urls = {}
        for id, (track, artist) in chunk.items():
            url = f'https://api.deezer.com/search?q=artist:"{artist}" track:"{track}"'
            response = safe_request('get', url)
            if response:
                if response.get('data') and response['data'][0].get('preview'):
                    preview_url = response['data'][0]['preview']
                    preview_urls.update({id: [track, preview_url]})
                    log.info(f'got preview url for {track}: {preview_url}')

                else:
                    log.warning(f'no data for {track}:{artist}')
        yield preview_urls


def download_preview(track_id, track, url):
    """downloads a preview url if not already exists, saves it as the track id and track name"""
    safe_name = sanitize_filename(track)
    filename = os.path.join('previews', f'{safe_name}_{track_id}.mp3')

    if os.path.exists(filename):
        log.warning(f"Already exists: {filename}. skipping download")
        return
    try:
        session = get_session()
        response = session.get(url, timeout=20)

        if not response:
            log.error(f'failed to download {url}')

        if response.status_code == 200:
            with open(filename, 'wb') as f:
                f.write(response.content)
            log.info(f'downloaded successfully{url}\n file path = {filename}')

    except Exception as e:
        log.error(f'error! {e}')


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
        return wav_files

def convert_all_mp3_to_wav():
    try:
        wavs = get_wavs_from_all_mp3()
    except Exception as e:
        log.fatal(f"convert_to_wav failed {e}")

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
                log.info("Uploaded successfully to reccobeats analysis audio features")
                return data
            else:
                log.error(f"Error {response.status_code}: {response.text}")
                return None
        except Exception as e:
            log.fatal("Request to reccobeats analysis audio features failed:", e)
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
    try:
        for wav_files_ten in chunked(wav_files, 10):
            total = len(wav_files)
            log.info(f'\nThere are {total} wav files. {datetime.now()}')
            features = []
            for attempt in range(5):
                try:
                    log.info(f'\ngoing to attempt uploading 10 tracks {wav_files_ten}\n')
                    ten_feature = feature_simultaneously(wav_files_ten)
                    features.extend(ten_feature)
                    time.sleep(5)
                    break
                except (TimeoutError, ConnectionError) as e:
                    log.warning(f'timeout. {e}\n going to retry in 10 seconds')
                    time.sleep(10)
                    if attempt == 4:
                        print('max retried reached, skipping')
                        break

            if features:
                log.info(f'\ngoing to try to insert 10 new values into sql...\n{features}\n')
                try:
                    with engine.begin() as conn:
                        conn.execute(insert(track_features), features)
                    log.info('nice.')
                    for wav_file in wav_files_ten:
                        os.remove(wav_file)
                        log.info(f"{wav_file} has been removed successfully")
                        log.info(f'{total - 10} number of files left to go through')
                        total -= 10
                except Exception as e:
                    log.error(f'couldnt insert into table {e}')
    except KeyboardInterrupt:
        log.info('Stopped!')

def insert_features_from_all_wavs():
    file_paths = os.listdir("previews")
    wav_files = [os.path.join("previews", f) for f in file_paths if f.lower().endswith(".wav")]
    if not wav_files:
        log.warning("insert_features, No wav files found")
        return
    insert_features_from_wav_file(wav_files)


#----------------------------------------------------PIPELINE-----------------------------------------------------------------

def run_audio_features_pipeline():
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
            log.info(f'skipping {name}, already success')
            continue

        log.info(f'audio features pipeline running. starting stage {name}')
        log_to_sql(name, "running", f"Starting stage {name}")
        try:
            func()
            log.info(f'Success! stage {name} completed')
            log_to_sql(name, "success", f"Stage {name} completed")
        except Exception as e:
            log.fatal(f'Whoops! stage {name} failed!')
            log_to_sql(name, "fail", f"Stage {name} failed: {e}")
            break

if __name__ == "__main__":
    log.info('run audio features pipeline started!')
    log_to_sql("pipeline", "start", "Audio feature pipeline initiated")
    run_audio_features_pipeline()
    log.info('run audio features pipeline completed!')
    log_to_sql("pipeline", "end", "Pipeline completed")


