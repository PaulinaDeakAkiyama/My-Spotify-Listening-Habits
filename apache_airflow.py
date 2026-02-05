import requests
from utils import safe_request

artist = "Chris Brown"
track = "Up To You"
"Come Alive:Foo Fighters"

url = f'https://api.deezer.com/search?q=artist:"{artist}" track:"{track}"'
response = safe_request('get', url)
if response:
    print(response)
    print(response.keys())
    if response.get('data') and response['data'][0].get('preview'):
        preview_url = response['data'][0]['preview']