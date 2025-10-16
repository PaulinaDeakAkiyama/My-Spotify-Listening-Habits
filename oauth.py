import spotipy
from spotipy.oauth2 import SpotifyOAuth


client_id = "9b81c0d4308646cea3a870d86315dd41"
client_secret = "c22c0085bc3b48efb03cd1c9c3f1ea5b"
redirect_url = "http://127.0.0.1:8888/redirect"
scope = "user-library-read user-read-recently-played user-read-currently-playing user-read-playback-state playlist-read-private playlist-modify-public playlist-modify-private"
state = '768tyfcghvhji'

def get_spotify_auth():
    return spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_url,
        scope=scope
))

sp = get_spotify_auth()
token_info = sp.auth_manager.get_access_token(as_dict=True)
print(token_info)