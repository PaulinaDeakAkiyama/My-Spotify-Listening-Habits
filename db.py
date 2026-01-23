from importlib.metadata import metadata
from sqlalchemy import create_engine, MetaData, Table, text
from config.settings import Config


USER = Config.SQL_USER
PASSWORD = Config.SQL_PASSWORD
HOST = Config.SQL_HOST
DATABASE = "MySpotify"


engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DATABASE}")
metadata = MetaData()

playlists = Table('playlists', metadata, autoload_with=engine)
playlist_tracks = Table('playlist_tracks', metadata, autoload_with=engine)
track_features = Table('track_features', metadata, autoload_with=engine)
listening_two = Table('listening_two', metadata, autoload_with=engine)
logging = Table('logging', metadata, autoload_with=engine)
track_reference = Table('track_reference', metadata, autoload_with=engine)
albums = Table('albums', metadata, autoload_with=engine)
artists = Table('artists', metadata, autoload_with=engine)
listening_history = Table('listening_history', metadata, autoload_with=engine)
