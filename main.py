import time
from datetime import datetime
import time
from tracker import get_last_track, save_last_50_tracks


def main():
    print("Spotify Listening Tracker started...")
    print("your last 50 played tracks will be logged to MySQL every 30 minutes..")

    try:
        while True:
            try:
                save_last_50_tracks()
                last_save_time = datetime.now()
                print(f'{last_save_time}\n')
            except Exception as e:
                print(f'{datetime.now()} Error getting last 50 tracks: {e}')

            try:
                get_last_track()
                print(f'\nlast_saved_time: {last_save_time}, current_time:{datetime.now()}\n')
            except Exception as e:
                print(f'{datetime.now()} Error checking current track: {e}')

            elapsed_minutes = (datetime.now() - last_save_time).total_seconds()/60
            print(f'elapsed minutes: {elapsed_minutes}')
            if elapsed_minutes >= 30:
                try:
                    print(f'{datetime.now()} saving last 50 played tracks...')
                    save_last_50_tracks()
                    last_save_time = datetime.now()
                    print(f'{datetime.now()} last 50 saved successfully')
                except Exception as e:
                    print(f'{datetime.now()} Error getting last 50 tracks: {e}')

            time.sleep(5)
    except KeyboardInterrupt:
        print('Tracker stopped manually.')

if __name__ == "__main__":
    main()