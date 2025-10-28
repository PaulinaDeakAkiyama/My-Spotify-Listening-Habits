from datetime import datetime, timedelta
import time
from tracker import get_current_track, save_track

start_time = datetime.now()
def main():
    try:
        while datetime.now() < start_time + timedelta(hours=12):
            current_track = get_current_track()
            save_track(current_track)
            time.sleep(5)
    except Exception as e:
        print(e)
    except KeyboardInterrupt:
        print('stopped')

if __name__ == "__main__":
    main()