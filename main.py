import time
from oauth import get_spotify_auth
from datetime import datetime, timedelta
import time
from tracker import save_last_50_tracks, get_current_track, save_track

def main():
    print("Spotify Listening Tracker started...")
    print("your last 50 played tracks will be logged to MySQL every 30 minutes..")

    try:
        save_last_50_tracks()
        last_save_time = datetime.now()
        print(f'{last_save_time}\n')
    except Exception as e:
        print(f'{datetime.now()} Error getting last 50 tracks: {e}')

    try:
        while True:
            first_track = get_current_track()
            if not first_track:
                print('nothing playing')
                time.sleep(300)
                continue
            first_track_id = first_track['track_id']
            start_time = datetime.now()
            print(f'currently playing: {first_track['track_name']}')
            time.sleep(1)
            try:
                while True:
                    current_track = get_current_track()
                    if not current_track:
                        print('nothing playing')
                        time.sleep(300)
                        continue
                    current_track_id = current_track['track_id']
                    time.sleep(1)

                    if not current_track_id == first_track_id:
                        print(f'Finished song: {first_track['track_name']}, Now playing: {current_track['track_name']}\n')
                        print(f'Start time:{start_time}\nProgress_ms = {first_track['progress_ms'] / 60000}')
                        print(f'\nend time?{start_time + timedelta(milliseconds=first_track['progress_ms'])}')
                        print(f'end time? {datetime.now()}')
                        first_track['start_time'] = start_time
                        first_track['end_time'] = start_time + timedelta(milliseconds=first_track['progress_ms'])

                        try:
                            save_track(first_track)
                        except Exception as e:
                            print(f'Error, couldnt insert track! {e}')

                        time.sleep(1)
                        break

                    else:
                        first_track = current_track
                        time.sleep(1)

                    elapsed_minutes = (datetime.now() - last_save_time).total_seconds() / 60
                    if elapsed_minutes >= 30:
                        try:
                            print(f'\n{datetime.now()} saving last 50 played tracks...')
                            save_last_50_tracks()
                            last_save_time = datetime.now()
                            print(f'\n{datetime.now()} last 50 saved successfully')
                        except Exception as e:
                            print(f'\n{datetime.now()} Error getting last 50 tracks: {e}')
                    time.sleep(2)

            except KeyboardInterrupt:
                print('Stopped')
            except Exception as e:
                print(f'Error!: {e}')

    except KeyboardInterrupt:
        print('stopped')

if __name__ == "__main__":
    main()