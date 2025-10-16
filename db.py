from importlib.metadata import metadata
from sqlalchemy import create_engine, MetaData, Table, text


USER = "root"
PASSWORD = "MySQLGuwaaa123"
HOST = "localhost"
DATABASE = "MySpotify"


engine = create_engine(f"mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/{DATABASE}")
metadata = MetaData()

listening_history = Table('listening_history', metadata, autoload_with=engine)
listening_stream = Table('listening_stream', metadata, autoload_with=engine)
playlists = Table('playlists', metadata, autoload_with=engine)
my_tracks = Table('my_tracks', metadata, autoload_with=engine)
track_features = Table('track_features', metadata, autoload_with=engine)
# artists = Table('artists', metadata, autoload_with=engine)
# fact = Table('fact', metadata, autoload_with=engine)

with engine.connect() as conn:
    result = conn.execute(text("SELECT 1;"))
    print(result.fetchone())