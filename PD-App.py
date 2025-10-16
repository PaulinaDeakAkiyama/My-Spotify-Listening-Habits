# import requests
# from requests.auth import HTTPBasicAuth
# import oauthlib
# from requests_oauthlib import OAuth2Session
# from urllib.parse import urlsplit, parse_qs, unquote
# import json
# from flask import Flask, redirect, request, session, url_for, render_template_string
# import urllib
#
# def get_token(client_id, client_secret):
#     """creates a client credentials token from spotify using client id and client secret"""
#     url = "https://accounts.spotify.com/api/token"
#
#     data = {
#         "grant_type": "client_credentials"
#     }
#
#     auth=HTTPBasicAuth(client_id, client_secret)
#
#     response = requests.post(url, data=data, auth=auth)
#
#     if response.status_code == 200:
#         token_info = response.json()
#         return token_info['access_token']
#     else:
#         print(f"Couldn't retrieve token \nError:{response.status_code}")
#
# token = get_token("9b81c0d4308646cea3a870d86315dd41","c22c0085bc3b48efb03cd1c9c3f1ea5b")
#
#
#
#
# TOKEN_URL = "https://accounts.spotify.com/api/token"
# client_id = "9b81c0d4308646cea3a870d86315dd41"
# client_secret = "c22c0085bc3b48efb03cd1c9c3f1ea5b"
# redirect_url = "http://127.0.0.1:8888/redirect"
# scope = "user-read-email user-read-private playlist-read-private playlist-modify-public playlist-modify-private"
# state = '768tyfcghvhji'
#
#
# app = Flask(__name__)
#
# @app.route("/")
# def login():
#     auth_url = "https://accounts.spotify.com/authorize"
#     params = {"client_id": client_id,
#               "response_type": "code",
#               "redirect_uri" : url_for('callback', _external= True),
#               "state": state,
#               "scope": scope
#         }
#     url = f"{auth_url}?{urllib.parse.urlencode(params)}"
#     return redirect(url)
#
# @app.route("/redirect")
# def callback():
#     code = request.args.get("code")
#     state = request.args.get("state")
#     print("Incoming /callback request - code:", repr(code), "state:", repr(state))
#     if state != state:
#         return "State mismatch!"
#     payload = {
#         "grant_type": "authorization_code",
#         "code": code,
#         "redirect_uri": redirect_url,
#         "client_id": client_id,
#         "client_secret": client_secret
#     }
#
#     headers = {"Content-Type": "application/x-www-form-urlencoded"}
#     response = requests.post(TOKEN_URL, data=payload, headers=headers)
#
#     if response.status_code != 200:
#         return f"Error getting token: {response.text}", 400
#
#     token_info = response.json()
#     access_token = token_info["access_token"]
#
#     return f"Superstar! Your access token is:<br><br>{access_token}"
#
#
# if __name__ == "__main__":
#     app.run(host="127.0.0.1", port=8888, debug=True)
#
#
#
#
#
#
# def get_user_country(arg):
#     url = 'https://api.spotify.com/v1/me'
#
#     headers = {'Authorization': f'Bearer {arg}'}
#
#     response = requests.get(url, headers=headers)
#
#     if response.status_code == 200:
#         user_info = response.json()
#         return user_info['country']
#     else:
#         print(f'error {response.status_code}{response.text}')

# get_user_country(token)
