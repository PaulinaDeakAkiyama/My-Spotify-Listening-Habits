import time
from oauth import get_spotify_auth
from datetime import datetime, timedelta
import time
from tracker import save_last_50_tracks, get_current_track, save_track

def main():
    """ tracks will start streaming using get_current_track which will refresh every second. This will differentiate between
    tracks that were skipped, and tracks that played on repeat and will pause for 5 minutes if nothing is playing.
    additionally spotify's track history function will get the last 50 tracks every hour"""
    print("Spotify Listening Tracker started...")
    print("your last 50 played tracks will be logged to MySQL every hour..")

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
            first_track_start = datetime.now()
            print(f'currently playing: {first_track['track_name']}\n')
            try:
                while True:
                    current_track = get_current_track()
                    if not current_track:
                        print('nothing playing')
                        time.sleep(180)
                        continue
                    current_track_id = current_track['track_id']

                    if not current_track_id == first_track_id: #if the track playing has changed
                        print(f'Finished song: {first_track['track_name']}, Now playing: {current_track['track_name']}\n')

                        if (datetime.now() - timedelta(milliseconds=2000)) > (
                                first_track_start + timedelta(milliseconds=first_track['duration_ms'])): # if the track has been playing longer than its duration
                            print(f'track {first_track['track_name']} has been played multiple times\nStart time: {first_track_start}, End time:{datetime.now()}')
                            first_track['start_time'] = first_track_start
                        else:
                            first_track['start_time'] = datetime.now() - timedelta(milliseconds=first_track['progress_ms'])

                        first_track['end_time'] = datetime.now()
                        try:
                            save_track(first_track) # insert track in to sql database
                        except Exception as e:
                            print(f'Error, couldnt insert track! {e}')
                        time.sleep(1)
                        break
                    else:
                        first_track = current_track
                        time.sleep(2)

                    elapsed_minutes = (datetime.now() - last_save_time).total_seconds() / 60
                    if elapsed_minutes >= 80: #if it's been 80mins get the last 50 tracks
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