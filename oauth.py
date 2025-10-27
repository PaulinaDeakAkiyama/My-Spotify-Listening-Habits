import spotipy
from spotipy.oauth2 import SpotifyOAuth
from config.settings import Config


scope = "user-library-read user-read-recently-played user-read-currently-playing user-read-playback-state playlist-read-private playlist-modify-public playlist-modify-private"

def create_spotify_client():
    return spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=Config.CLIENT_ID,
        client_secret=Config.CLIENT_SECRET,
        redirect_uri=Config.REDIRECT_URI,
        scope=scope
    ))

sp = create_spotify_client()
