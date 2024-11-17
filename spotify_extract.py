# spotify_extract.py

# This file is to extract data from MULTIPLE playlists.

import json
import os
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime
import time
import logging


# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load environment variables
load_dotenv()
client_id = os.environ.get('client_id')
client_secret = os.environ.get('client_secret')

# Authenticate with Spotify
client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)


def fetch_tracks_from_playlists(playlist_ids):
    """Fetch tracks, audio features, and related artists in a single process."""
    all_tracks = []
    seen_track_ids = set()
    seen_artist_ids = set()
    related_artists = {}
    audio_features = {}
    artist_details = {}  # Store artist information with genres and popularity

    for playlist_id in playlist_ids:
        logging.info(f"Fetching tracks from playlist {playlist_id}")
        results = sp.playlist_tracks(playlist_id)

        while results:
            for item in results['items']:
                track = item['track']
                if not track:
                    continue

                track_id = track['id']
                if track_id not in seen_track_ids:
                    # add track to result
                    all_tracks.append(track)
                    seen_track_ids.add(track_id)

                    # fetch audio features (batch fetch later if preferred)
                    if track_id not in audio_features:
                        audio_features[track_id] = sp.audio_features([track_id])[0]
                        time.sleep(0.1)  # Respect API rate limits

                    # fetch related artisits and details such as genres
                    for artist in track['artists']:
                        artist_id = artist['id']
                        if artist_id not in seen_artist_ids:
                            seen_artist_ids.add(artist_id)

                            # fetch artisit info for genres and popularity
                            artist_info = sp.artist(artist_id)
                            artist_details[artist_id] = {
                                "name": artist_info.get("name"),
                                "genres": artist_info.get("genres", []),
                                "popularity": artist_info.get("popularity", None),
                            }
                            time.sleep(0.1)

                            # fetch related artisits
                            related_artists[artist_id] = sp.artist_related_artists(artist_id)["artists"]
                            time.sleep(0.1)

            results = sp.next(results) if results['next'] else None

    # add audio features and related artists into tracks
    for track in all_tracks:
        track_id = track['id']
        track["audio_features"] = audio_features.get(track_id, {})
        track["related_artists"] = [
            related_artists.get(artist["id"], []) for artist in track["artists"]
        ]
        for artist in track["artists"]:
            artist_id = artist["id"]
            artist["genres"] = artist_details.get(artist_id, {}).get("genres", [])
            artist["popularity"] = artist_details.get(artist_id, {}).get("popularity", None)

    return all_tracks


def save_data_to_file(data, output_dir, filename, format="json"):
    """Save data to a specified file in JSON or other formats."""
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, filename)

    if format == "json":
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    else:
        raise ValueError(f"Unsupported format: {format}")

    logging.info(f"Data successfully saved to {file_path}")
    return file_path


def main():
    start_time = time.time()  # Start timing the main function

    # Sample playlists to fetch data
    playlist_ids = [
        "37i9dQZEVXbKj23U1GF4IR",  # Top 50 Canada
        # "37i9dQZF1DXcBWIGoYBM5M",  # Global Top 50
        # "37i9dQZF1DWXRqgorJj26U",  # Chill Hits
    ]

    # Fetch playlist tracks
    spotify_data = fetch_tracks_from_playlists(playlist_ids)

    # Create output filename
    filename = "spotify_raw_tracks_" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".json"
    output_dir = "./raw_data"

    # Save data to file
    save_data_to_file(spotify_data, output_dir, filename)

    end_time = time.time()  # End timing the main function
    logging.info(f"Total execution time: {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    main()
