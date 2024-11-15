import json
import os
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime

# Set Spotify API credentials
load_dotenv()
client_id = os.environ.get('client_id')
client_secret = os.environ.get('client_secret')

# Authenticate with Spotify
client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# Spotify playlist link
playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbKj23U1GF4IR"

# Extract the playlist URI
playlist_URI = playlist_link.split("/")[-1]

# Fetch playlist tracks
spotify_data = sp.playlist_tracks(playlist_URI)

# Create a filename with the current timestamp
filename = "spotify_raw_" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".json"

# Save the data locally
with open(filename, 'w', encoding='utf-8') as f:
    json.dump(spotify_data, f, ensure_ascii=False, indent=4)

print(f"Data successfully saved to {filename}")
