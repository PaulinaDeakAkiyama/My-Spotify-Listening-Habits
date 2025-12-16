from db import engine, track_features
import pandas as pd
from sqlalchemy import create_engine

query = "SELECT acousticness, danceability, energy, instrumentalness, key_, loudness, mode_, speechiness, tempo, valence FROM track_features"
df = pd.read_sql(query, engine)
import seaborn as sns
import matplotlib.pyplot as plt
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

print(df.describe())
sns.pairplot(df)
plt.show()
