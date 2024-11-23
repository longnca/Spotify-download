# spotify_extract.py

"""
This file aims to fetch datasets from Spotify using the Spotify API and Spotipy library.

Features:
- Dynamically searches playlists by year.
- Extracts tracks, audio features, and related artist names.
- Saves the data in JSON format.

Output:
- JSON files containing track metadata, audio features, and artist names.
"""

import json
import os
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime
import time
import logging

# choose the range of the years that you want to fetch data
YEAR_START = 1920
YEAR_END = 1921

# choose the limit of playlists per year that you want to fetch
PLAYLIST_LIMIT = 10

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load environment variables for Spotify API credentials
load_dotenv()
client_id = os.environ.get('client_id')
client_secret = os.environ.get('client_secret')

# Authenticate with Spotify API
client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)


def fetch_playlists_by_year(year, limit=10, query_template="{year}"):
    """
    Search for playlists containing a specific year in their title.

    :param year: the year to search for in playlist titles.
    :param limit: maximum number of playlists to return.
    :param query_template: template for formatting the query string.
    :return: list of playlist IDs.
    """
    logging.info(f"Searching for playlists with year: {year}")
    query = query_template.format(year=year)
    results = sp.search(q=query, type='playlist', limit=limit)

    # Log the response to understand why it might be None
    if not results or 'playlists' not in results or 'items' not in results['playlists']:
        logging.warning(f"No playlists found for year {year}. API response: {results}")
        return []  # Return an empty list if no playlists are found

    playlist_ids = [playlist['id'] for playlist in results['playlists']['items']]
    logging.info(f"Found {len(playlist_ids)} playlists for year {year}")
    return playlist_ids


def fetch_tracks_batch(playlist_id):
    """
    Fetch tracks from a playlist in batches.
    The key reason for batching is to respect Spoitfy's API rate limits.

    :param playlist_id: Spoitfy playlist ID.
    :return: list of tracks from the playlist.
    """
    logging.info(f"Fetching tracks from playlist {playlist_id}")
    all_tracks = []
    results = sp.playlist_tracks(playlist_id)

    while results:
        for item in results['items']:
            track = item['track']
            if track and track.get('id'):
                all_tracks.append(track)

        # Respect API rate limits by pausing if needed
        time.sleep(0.1)
        results = sp.next(results) if results['next'] else None

    return all_tracks


def fetch_audio_features_batch(track_ids):
    """
    Fetch audio features for a list of track IDs in batches.
    Spotify API allows up to 100 IDs per request so we can set the batch size up to 100.

    :param track_ids: list of track IDs that contain audio features.
    :return: dict of audio features with track IDs as keys.
    """
    logging.info(f"Fetching audio features for {len(track_ids)} tracks")

    # filter out any None values from track_ids
    track_ids = [track_id for track_id in track_ids if track_id]

    audio_features = {}
    batch_size = 100

    for i in range(0, len(track_ids), batch_size):
        batch_ids = track_ids[i:i + batch_size]
        features = sp.audio_features(batch_ids)
        time.sleep(0.2)  # pause to respect API rate limits of Spotify

        for feature in features:
            if feature:
                audio_features[feature['id']] = feature

    return audio_features


def fetch_tracks_from_playlists(playlist_ids):
    """
    Fetch tracks, audio features, and artist names from a list of playlist IDs.

    :param playlist_ids: list of Spoitfy playlist IDs.
    :return: list of tracks with audio features and artist names.
    """
    all_tracks = []
    seen_track_ids = set()
    audio_features = {}

    for playlist_id in playlist_ids:
        tracks = fetch_tracks_batch(playlist_id)
        all_tracks.extend(tracks)

    # deduplicate track IDs to only contain unique track IDs
    track_ids = list({track['id'] for track in all_tracks if track['id'] not in seen_track_ids})
    seen_track_ids.update(track_ids)

    # fetch audio features in batches
    audio_features = fetch_audio_features_batch(track_ids)

    # attach audio features and artist names to tracks
    for track in all_tracks:
        track_id = track['id']
        if track_id:
            track["audio_features"] = audio_features.get(track_id, {})
            track["artist_names"] = [artist['name'] for artist in track['artists']]

    return all_tracks


def fetch_tracks_from_playlists_by_year(year_range, limit_per_year=PLAYLIST_LIMIT):
    """
    Fetch tracks, audio features, and artist details dynamically based on a range of years.

    :param year_range: range of years to search for playlists.
    :param limit_per_year: number of playlists to fetch per year.
    :return: list of tracks with audio features and artist names.
    """
    all_tracks = []
    seen_playlist_ids = set()

    for year in year_range:
        # looking for the playlists "Top Hits of [the year]"
        # you can adjust if needed
        query_template = "Top Hits of {year}"
        playlist_ids = fetch_playlists_by_year(year, limit=limit_per_year, query_template=query_template)

        # note: if not using query_template, un-comment the next line and comment out the previouse lines
        # playlist_ids = fetch_playlists_by_year(year, limit=limit_per_year)

        # deduplicate playlist IDs
        unique_playlist_ids = [pid for pid in playlist_ids if pid not in seen_playlist_ids]
        seen_playlist_ids.update(unique_playlist_ids)

        if not unique_playlist_ids:
            logging.info(f"No playlists found for year {year}")
            continue  # Skip this year if no playlists are found

        # fetch tracks and artist details from playlists
        logging.info(f"Fetching tracks from {len(unique_playlist_ids)} playlists for year {year}")
        all_tracks.extend(fetch_tracks_from_playlists(unique_playlist_ids))

    return all_tracks


def save_data_to_file(data, output_dir, filename, format="json"):
    """
    Save data to a specified file in JSON or other formats.

    :param data: data to save.
    :param output_dir: directory to save the data to.
    :param filename: name of the output file.
    :param format: format of the output file. (default: json).
    :return: path to the saved file.
    """
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
    # start timing the main function
    start_time = time.time()

    # define year range
    year_range = range(YEAR_START, YEAR_END + 1)

    # fetch tracks dynamically by year
    spotify_data = fetch_tracks_from_playlists_by_year(year_range)

    # create a timestamped filename
    filename = ("spotify_dataset_by_year_"
                + str(YEAR_START)
                + "-"
                + str(YEAR_END)
                + "-"
                + datetime.now().strftime("%Y%m%d_%H%M%S")
                + ".json")
    output_dir = "./raw_data"

    # save data to file
    save_data_to_file(spotify_data, output_dir, filename)

    # end timing the main function
    end_time = time.time()
    logging.info(f"Total execution time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
