import os
import glob
import pandas as pd
import numpy as np
from geopy.distance import geodesic
from shapely import wkb
from logger import setup_logger

# Directory and file configuration
IN_DIR = 'data/chunks'
FILE_PATTERN = 'measurements_*.csv'
OUT_DIR = 'data'
OUT_FILE = os.path.join(OUT_DIR, 'measurements_daily.csv')
# meters (GPS accuracy is 10-20 meters but keep a margin of error in case the
# sensor was moved to another location)
DISTANCE_THRESHOLD = 1000  

logger = setup_logger()

def read_and_process_files():
    # Prepare output file
    if os.path.exists(OUT_FILE):
        os.remove(OUT_FILE)

    files = glob.glob(os.path.join(IN_DIR, FILE_PATTERN))
    for file in files:
        logger.info(f"Reading file: {file}")
        data = pd.read_csv(file)
        logger.info(f"Read {len(data)} records")
        processed_data = process_data(data)
        save_processed_data(processed_data)
        logger.success(f"Processed and saved data from {file}")

def process_data(df):
    # Decode WKB format and extract latitude and longitude
    df['Location'] = df['Location'].apply(lambda x: wkb.loads(bytes.fromhex(x)))
    df['Latitude'] = df['Location'].apply(lambda x: x.y)
    df['Longitude'] = df['Location'].apply(lambda x: x.x)

    # Handle missing 'Height' values by replacing them with a default placeholder -np.inf
    df['Height'].fillna(-np.inf, inplace=True)

    # Group data and process
    grouped = df.groupby(['Device ID', 'Measurement Day', 'Height'])
    processed_list = []

    for (device_id, measurement_day, height), group in grouped:
        processed_group = merge_close_measurements(group, device_id, measurement_day, height)
        processed_list.append(processed_group)

    # Check if processed_list is empty
    if processed_list:
        return pd.concat(processed_list, ignore_index=True)
    else:
        logger.warning("No data to process after grouping. Returning an empty DataFrame.")
        return pd.DataFrame()


def merge_close_measurements(group, device_id, measurement_day, height):
    # Use a temporary data structure to hold results
    result = []
    visited = set()

    for i, row1 in group.iterrows():
        if i in visited:
            continue
        close_rows = [row1]
        for j, row2 in group.iterrows():
            if j <= i or j in visited:
                continue
            dist = geodesic((row1['Latitude'], row1['Longitude']), (row2['Latitude'], row2['Longitude'])).meters
            if dist < DISTANCE_THRESHOLD:
                close_rows.append(row2)
                visited.add(j)

        # Calculate average values for close measurements
        avg_lat = sum(row['Latitude'] for row in close_rows) / len(close_rows)
        avg_lon = sum(row['Longitude'] for row in close_rows) / len(close_rows)
        avg_value = sum(row['Average Value'] for row in close_rows) / len(close_rows)
        result.append({
            'ID': device_id,
            'Unit': close_rows[0]['Unit'],
            'Latitude': avg_lat,
            'Longitude': avg_lon,
            'Height': height,
            'Measurement Day': measurement_day,
            'Average Value': avg_value
        })

        for row in close_rows:
            visited.add(row.name)

    return pd.DataFrame(result)

def save_processed_data(df):
    # Replace -np.inf with NaN in the 'Height' column before saving
    df['Height'].replace(-np.inf, pd.NA, inplace=True)

    # Sort data by date and save
    df.sort_values(by=['Measurement Day'], inplace=True)
    with open(OUT_FILE, 'a', newline='') as f:  # Ensure the file is opened without additional newline characters
        df.to_csv(f, header=f.tell()==0, index=False)
    logger.info(f"Saved {len(df)} records to output.")

if __name__ == '__main__':
    read_and_process_files()
