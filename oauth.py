import spotipy
from spotipy.oauth2 import SpotifyOAuth
from config.settings import Config
from requests import Session

scope = "user-library-read user-read-recently-played user-read-currently-playing user-read-playback-state playlist-read-private playlist-modify-public playlist-modify-private"
session = Session()

def create_spotify_client():
    return spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=Config.SPOTIFY_CLIENT_ID,
        client_secret=Config.SPOTIFY_CLIENT_SECRET,
        redirect_uri=Config.REDIRECT_URI,
        scope=scope
    ), requests_session=session,
       requests_timeout=10    )

sp = create_spotify_client()
token_info = sp.auth_manager.get_access_token()
access_token = token_info['access_token']
oauth_header = {"Authorization": f"Bearer {access_token}"}

if __name__ == '__main__':
    print(oauth_header)
    #url = 'https://api.spotify.com/v1/playlists/6YgRySvPiqRmmsDDyu98cb/tracks?offset=100&limit=100'
    #method = 'GET'
    #result = safe_request(method=method, url=url, headers=oauth_header)
    #print(result.keys())
    #print(result['items'][0].keys())
    #print(result['next'])

