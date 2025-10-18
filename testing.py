from oauth import get_spotify_auth
from db import engine, listening_stream, playlists, my_tracks, track_features #track_features, artists, fact
from sqlalchemy import insert, select, func
from datetime import datetime, time, timezone, timedelta
import time
import requests
from tracker import save_last_50_tracks

import requests

sp = get_spotify_auth()

def chunked(iterable, size):
    lst = list(iterable)
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

with engine.connect() as conn:
    my_track_ids = set(
        conn.execute(
            select(my_tracks.c.track_id)
            .where(my_tracks.c.track_id.notin_(
                select(track_features.c.track_id))
            )
        ).scalars().all()
    )

def get_track_mp3(track_ids):
    try:
        tracks = sp.tracks(track_ids)
        if not tracks:
            print('no track')
        track_names = [track['name'] for track in tracks['tracks'] if track['name']]
        track_artists = [track['artists'][0]['name'] for track in tracks['tracks'] if track['artists']]
        # print(f'{track_names}\n number of track names: {len(track_names)}')
        # print(f'{track_artists}\n number of artist names: {len(track_artists)}')
        track_artist_dic = dict(zip(track_names, track_artists))

        return(track_artist_dic)

    except Exception as e:
        print(f'Error! {e}')

track_artist = {}
for chunk in chunked(my_track_ids, 50):
    track_artist_dic = get_track_mp3(chunk)
    track_artist.update(track_artist_dic)
    time.sleep(1)
print(track_artist)

preview_urls = []
for key in track_artist.keys():
    value = track_artist[key]
    url = f'https://api.deezer.com/search?q=artist:"{value}" track:"{key}"'
    method = 'GET'

    try:
        response = requests.request(method, url).json()
        if response:
            if response.get('data') and response['data'][0].get('preview'):
                preview_url = response['data'][0]['preview']
                preview_urls.append(preview_url)
                print(preview_url)

            else:
                print(f'no data for {key}:{value}')
        else:
            print(f'no response for {key}:{value}')
    except Exception as e:
        print(f'oops {e}')
    time.sleep(0.50)

"https://cdnt-preview.dzcdn.net/api/1/1/0/8/2/0/08232759fb80c6a3e2b93e74400ae666.mp3?hdnea=exp=1760786848~acl=/api/1/1/0/8/2/0/08232759fb80c6a3e2b93e74400ae666.mp3*~data=user_id=0,application_id=42~hmac=4154f66b9c987be89f5d651365951af5afb44797144e9291e2455294b523e3ba"
"https://cdnt-preview.dzcdn.net/api/1/1/6/7/0/0/6708f065789fd7e151e7ac952940d5a5.mp3?hdnea=exp=1760786849~acl=/api/1/1/6/7/0/0/6708f065789fd7e151e7ac952940d5a5.mp3*~data=user_id=0,application_id=42~hmac=739aef2cde20f5f00ad59b2fd2dcfcafe7f96c1d2949f3d9c4228296f88844b6"
"https://cdnt-preview.dzcdn.net/api/1/1/1/6/4/0/164469066a4e9dc23b0e4a9c43f62475.mp3?hdnea=exp=1760786850~acl=/api/1/1/1/6/4/0/164469066a4e9dc23b0e4a9c43f62475.mp3*~data=user_id=0,application_id=42~hmac=53355b3f50263636d3d168512af48b0a76ef417308be6882349c723ff54d15e7"