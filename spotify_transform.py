# spotify_transform.py

"""
This file aims to transform JSON data extracted from Spotify into a tabular CSV format.

Purpose:
- Flatten nested JSON data into a tabular format.
- Save the flattened data as a CSV file for easier processing with PySpark.

Output:
- A timestamped CSV file containing track metadata, audio features, and artist names.
"""

import json
import pandas as pd
import os
from datetime import datetime

# set input and output directories
input_dir = "./raw_data"
output_dir = "./transformed_data"
os.makedirs(output_dir, exist_ok=True)

# list all JSON files in the directory
json_files = [f for f in os.listdir(input_dir) if f.endswith('.json')]

# sort files by modification time (most recent first)
json_files = sorted(json_files, key=lambda x: os.path.getmtime(os.path.join(input_dir, x)), reverse=True)

# select the most recent file
input_file = os.path.join(input_dir, json_files[0])
print(f"Using input file: {input_file}")

# load the JSON data
with open(input_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

# extract relevant data and flatten into a tabular format
tracks = []
for track in data:
    audio_features = track.get("audio_features", {})
    tracks.append({
        "track_id": track["id"],
        "track_name": track["name"],
        "popularity": track["popularity"],
        "duration_ms": track["duration_ms"],
        "explicit": track["explicit"],
        "album_name": track["album"]["name"],
        "album_release_date": track["album"]["release_date"],
        "album_type": track["album"]["album_type"],
        "danceability": audio_features.get("danceability", None),
        "energy": audio_features.get("energy", None),
        "key": audio_features.get("key", None),
        "loudness": audio_features.get("loudness", None),
        "mode": audio_features.get("mode", None),
        "speechiness": audio_features.get("speechiness", None),
        "acousticness": audio_features.get("acousticness", None),
        "instrumentalness": audio_features.get("instrumentalness", None),
        "liveness": audio_features.get("liveness", None),
        "valence": audio_features.get("valence", None),
        "tempo": audio_features.get("tempo", None),
        "time_signature": audio_features.get("time_signature", None),
        "artist_names": ", ".join(track.get("artist_names", [])),  # Concatenate artist names into a single string
    })

# convert the extracted data into a DataFrame
tracks_df = pd.DataFrame(tracks)

# create a timestamped filename
filename = "spotify_dataset_" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".csv"
tracks_csv_path = os.path.join(output_dir, filename)

# save the DataFrame to a CSV file
tracks_df.to_csv(tracks_csv_path, index=False, encoding='utf-8')

# confirm that the file was saved successfully
print(f"Tracks saved to {tracks_csv_path}")

