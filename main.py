from threading import *
from datetime import datetime, timedelta, timezone
import time
from oauth import create_spotify_client
from utils import log_to_sql, insert_into_sql
from db import artists, albums, track_reference, listening_two
from tracker import get_current_track, deal_with_artists_albums_reference, save_last_50_tracks
#from SCDplaylistsupdate import update_tracks_and_playlists
from logger import log


def tracker():
    log.info('tracker started')
    try:
        previous_id = None
        start_time = datetime.now()
        while True:
            current_track = get_current_track()

            if current_track['listening_two']['track_id'] != previous_id:
                start_time = datetime.now(timezone.utc)
                is_new_group = True
            else:
                is_new_group = False

            current_track['listening_two']['start_time'] = start_time
            current_track['listening_two']['is_new_group'] = is_new_group

            log.info(f'\n{datetime.now()}{start_time}: playing: {current_track['track_reference']['track_name']}\n')

            if current_track['listening_two']['track_id'] is not None:
                time.sleep(1)
                check = deal_with_artists_albums_reference(current_track)
                if check is True:
                    insert_into_sql(listening_two, current_track)
                    previous_id = current_track['listening_two']['track_id']
                    time.sleep(4)
                else:
                    log.warning(f'deal with albums artists reference check: {check}')
                    time.sleep(2)
                    continue
            else:
                insert_into_sql(listening_two, current_track)
                previous_id = current_track['listening_two']['track_id']
                time.sleep(10)

    except Exception as e:
        log.error(f"hehe Error: {type(e).__name__} {e}")
        log_to_sql('tracker','failed', f"Error: {type(e).__name__} {e}")
    except KeyboardInterrupt:
        print('stopped')



def main():
    start_time = datetime.now()
    log.info(f'Spotify song tracker started! {start_time}')
    log_to_sql('tracker', 'started', 'ongoing')
    try:
        save_last_50_tracks()
        D = Thread(target=tracker, daemon=True)
        D.start()
        while True:
            log.info(' main running')
            time.sleep(30)
            if datetime.now() > start_time + timedelta(hours=1):
                log.info('Going to update tables')
                start_time = datetime.now()
                #update_tracks_and_playlists()
    except KeyboardInterrupt:
        log.info('Spotify tracker manually stopped')
        log_to_sql('Spotify tracker', 'stopped', 'nothing running')
    except Exception as e:
        log.fatal(f'whoops {e}')



if __name__ == '__main__':
    main()