import spotipy
from spotipy.oauth2 import SpotifyOAuth
import datetime
import pandas as pd


def check_if_tracks_valid(df: pd.DataFrame) -> bool:
    # Chceck if I were listening to any songs yesterday:
    if df.empty:
        print("No songs have been downloaded. Dropping the task...")
        return False
    # Primary key check:
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary key check not successful")
    # Check if there's any empty data in our dataframe:
    if df.isnull().values.any():
        raise Exception("Some data is null in your dataframe")
    # Check if all timestamps are with correct date(yesterday):

    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df['timestamp'].tolist()
    for t in timestamps:
        if datetime.datetime.strftime(t, '%Y-%m-%d') != yesterday:
            raise Exception("There's at least one timestamp which date is not yesterday")
    return True


def get_recent_tracks():
    # The list of scopes can be found at:
    # https://developer.spotify.com/documentation/general/guides/authorization/scopes/
    scope = 'user-read-recently-played'
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    # Other variables for spotify function are OS variables in .env file
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))
    results = sp.current_user_recently_played(after=yesterday_unix_timestamp)
    return results


def parse_tracks_json(raw_tracks_json):
    recent_tracks_dict = []
    for i in raw_tracks_json['items']:
        track = i['track']['name']
        artist = i['track']['album']['artists'][0]['name']
        played_at = i['played_at']
        timestamp_ = i['played_at'][0:10]

        data = {
                'track': track,
                'artist': artist,
                'played_at': played_at,
                'timestamp': timestamp_
                }
        recent_tracks_dict.append(data)
    return recent_tracks_dict


if __name__ == "__main__":
    raw_tracks_json = get_recent_tracks()

    # Validate the keys of received dictionary:
    try:
        recent_tracks_dict = parse_tracks_json(raw_tracks_json)
    except KeyError:
        pass

    recent_tracks_df = pd.DataFrame(recent_tracks_dict)

    # Validate the dataframe:
    if check_if_tracks_valid:
        print("The data is valid, let's proceed")
