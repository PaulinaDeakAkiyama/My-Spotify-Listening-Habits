from datetime import datetime, timedelta
import time
from tracker import get_current_track, save_track_to_listening, save_track_to_reference


start_time = datetime.now()
def main():
    try:
        while datetime.now() < start_time + timedelta(hours=12):
            current_track = get_current_track()
            save_track_to_reference(current_track)
            save_track_to_listening(current_track)
            time.sleep(5)
    except Exception as e:
        print(e)
    except KeyboardInterrupt:
        print('stopped')

if __name__ == "__main__":
    main()