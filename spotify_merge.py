# spotify_merge.py

"""
This script merges multiple CSV files from different periods into a single CSV file.

Input:
- Directory containing CSV files with names like 'spotify_dataset_by_year_XXXX-XXXX_YYYYMMDD_HHMMSS.csv'

Output:
- A single CSV file named 'spotify_merged_dataset_XXXX-XXXX_YYYYMMDD_HHMMSS.csv'
"""

import os
from datetime import datetime
import pandas as pd

# input directory where all CSV files are located
input_dir = "./transformed_data"

# output directory and file
output_dir = "./merged_data"
os.makedirs(output_dir, exist_ok=True)
filename = ("spotify_merged_dataset_" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".csv")
output_file = os.path.join(output_dir, filename)

# list all CSV files in the input directory
csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]

# check if there are CSV files to process
if not csv_files:
    print("No CSV files found in the directory.")
    exit()

# initialize an empty list to store DataFrames
dataframes = []

# load and append each CSV file into the list of DataFrames
for csv_file in csv_files:
    file_path = os.path.join(input_dir, csv_file)
    print(f"Reading file: {file_path}")
    df = pd.read_csv(file_path)
    dataframes.append(df)

# concatenate all DataFrames into one
merged_df = pd.concat(dataframes, ignore_index=True)

# save the merged DataFrame to a single CSV file
merged_df.to_csv(output_file, index=False, encoding='utf-8')
print(f"Merged dataset saved to: {output_file}")
