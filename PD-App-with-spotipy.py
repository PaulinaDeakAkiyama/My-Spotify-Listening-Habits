# import spotipy
# from flask import Flask, redirect, request, session, url_for, render_template_string
# from spotipy.oauth2 import SpotifyOAuth
# import time
#
# app = Flask(__name__)
# app.config['SESSION_COOKIE_NAME'] = 'POW isCOOK'
# app.secret_key = '767tyfghvjiu8y7ty6rdfghui'
#
# TOKEN_INFO = ''
# TOKEN_URL = "https://accounts.spotify.com/api/token"
# client_id = "9b81c0d4308646cea3a870d86315dd41"
# client_secret = "c22c0085bc3b48efb03cd1c9c3f1ea5b"
# redirect_url = "http://127.0.0.1:8888/redirect"
# scope = "user-read-email user-read-private playlist-read-private playlist-modify-public playlist-modify-private"
# state = '768tyfcghvhji'
#
#
# @app.route('/')
# def login():
#     auth_url = create_spotify_oauth().get_authorize_url()
#     return redirect(auth_url)
#
#
# @app.route('/redirect')
# def redirect_page():
#     session.clear()
#     code = request.args.get('code')
#     token_info = create_spotify_oauth().get_access_token(code)
#     session[TOKEN_INFO] = token_info
#     return redirect(url_for('save_discover_weekly', external=True))
#
#
# @app.route('/saveDiscoverweekly')
# def save_discover_weekly():
#     try:
#         token_info = get_token()
#     except:
#         print('user not logged in')
#         return redirect('/')
#     return ('oauth succsessful!! :D:D:D')
#
#
# def get_token():
#     token_info = session.get(TOKEN_INFO, None)
#     if not token_info:
#         redirect(url_for('login', external=False))
#
#     now = int(time.time())
#     is_expired = token_info['expires_at'] - now < 60
#     if (is_expired):
#         spotify_oauth = create_spotify_oauth()
#         token_info = spotify_oauth.refresh_access_token(token_info('refresh_token'))
#
#     return token_info
#
#
# def create_spotify_oauth():
#     return SpotifyOAuth(
#         client_id=client_id,
#         client_secret=client_secret,
#         redirect_uri=url_for('redirect_page', _external=True),
#         scope=scope
#     )
#
#
# app.run(debug=True)