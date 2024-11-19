# spotify_transform_artists_relationships.py

# This file transforms: track, artists, and their relationships.

import json
import pandas as pd
import os

# Load the JSON data

# Input and output directories
input_dir = "../raw_data"
output_dir = "../transformed_data"
os.makedirs(output_dir, exist_ok=True)

# List all JSON files in the directory
json_files = [f for f in os.listdir(input_dir) if f.endswith('.json')]

# Sort files by modification time (most recent first)
json_files = sorted(json_files, key=lambda x: os.path.getmtime(os.path.join(input_dir, x)), reverse=True)

# Select the most recent file
input_file = os.path.join(input_dir, json_files[0])
print(f"Using input file: {input_file}")

# Load the JSON data
with open(input_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

# Tracks CSV
tracks = []
for track in data:
    audio_features = track.get("audio_features", {})
    tracks.append({
        "track_id": track["id"],
        "name": track["name"],
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
    })

# Save Tracks to CSV
tracks_df = pd.DataFrame(tracks)
tracks_csv_path = os.path.join(output_dir, 'tracks.csv')
tracks_df.to_csv(tracks_csv_path, index=False, encoding='utf-8')
print(f"Tracks saved to {tracks_csv_path}")


# Artists CSV
artists = []
for track in data:
    for artist in track["artists"]:
        artists.append({
            "artist_id": artist["id"],
            "artist_name": artist["name"],
            "artist_popularity": artist.get("popularity", None),
            "artist_genres": ", ".join(artist.get("genres", []))
        })

# Remove duplicate artists
artists_df = pd.DataFrame(artists).drop_duplicates(subset="artist_id")
artists_csv_path = os.path.join(output_dir, 'artists.csv')
artists_df.to_csv(artists_csv_path, index=False, encoding='utf-8')
print(f"Artists saved to {artists_csv_path}")

# Relationships CSV
relationships = []
for track in data:
    track_id = track["id"]
    for related_artist_list in track.get("related_artists", []):
        for artist in related_artist_list:
            relationships.append({
                "track_id": track_id,
                "related_artist_id": artist["id"]
            })

# Save Relationships to CSV
relationships_df = pd.DataFrame(relationships)
relationships_csv_path = os.path.join(output_dir, 'relationships.csv')
relationships_df.to_csv(relationships_csv_path, index=False, encoding='utf-8')
print(f"Relationships saved to {relationships_csv_path}")
