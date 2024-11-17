# spotify_transform.py

import os
import json
from datetime import datetime
from io import StringIO
import pandas as pd


# Define transformation functions
def album(data):
    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        album_elements = {'album_id': album_id, 'name': album_name, 'release_date': album_release_date,
                          'total_tracks': album_total_tracks, 'URL': album_url}
        album_list.append(album_elements)
    return album_list


def artist(data):
    artist_list = []
    for row in data['items']:
        for key, value in row.items():
            if key == 'track':
                for artist in value['artists']:
                    artist_dict = {'artist_id': artist['id'], 'artist_name': artist['name'],
                                   'external_url': artist['href']}
                    artist_list.append(artist_dict)
    return artist_list


def song(data):
    song_list = []
    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added_at = row['added_at']  # note that this is not in the 'track'
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {'song_id': song_id, 'song_name': song_name, 'song_duration': song_duration,
                        'song_url': song_url,
                        'song_popularity': song_popularity, 'song_added_at': song_added_at, 'album_id': album_id,
                        'artist_id': artist_id}
        song_list.append(song_element)
    return song_list


# Main function to process local files
def main():
    # Input and output directories
    input_dir = "./raw_data"
    output_dir = "./transformed_data"

    # Ensure output directories exist
    os.makedirs(f"{output_dir}/song_data", exist_ok=True)
    os.makedirs(f"{output_dir}/album_data", exist_ok=True)
    os.makedirs(f"{output_dir}/artist_data", exist_ok=True)

    # List all JSON files in the input directory
    spotify_files = [f for f in os.listdir(input_dir) if f.endswith('.json')]

    for file in spotify_files:
        # Load the JSON file
        with open(os.path.join(input_dir, file), 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Transform data
        album_list = album(data)
        artist_list = artist(data)
        song_list = song(data)

        # Convert from dictionaries to DataFrames
        album_df = pd.DataFrame.from_dict(album_list).drop_duplicates(subset=['album_id'])
        artist_df = pd.DataFrame.from_dict(artist_list).drop_duplicates(subset=['artist_id'])
        song_df = pd.DataFrame.from_dict(song_list)

        # Convert to datetime data types
        album_df['release_date'] = pd.to_datetime(album_df['release_date'], errors='coerce')
        song_df['song_added_at'] = pd.to_datetime(song_df['song_added_at'], errors='coerce')

        # Save transformed data to CSV files
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        song_df.to_csv(f"{output_dir}/song_data/songs_transformed_{timestamp}.csv", index=False)
        album_df.to_csv(f"{output_dir}/album_data/albums_transformed_{timestamp}.csv", index=False)
        artist_df.to_csv(f"{output_dir}/artist_data/artists_transformed_{timestamp}.csv", index=False)

        print(f"Processed and transformed file: {file}")

    print("All files have been processed and transformed.")


# Execute the script
if __name__ == "__main__":
    main()
