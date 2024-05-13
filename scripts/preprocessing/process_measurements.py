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
OUT_FILE_NAME = 'measurements_preprocessed.csv'

GPS_ACCURACY_THRESHOLD = 20  # meters
HEIGHT_MOVEMENT_THRESHOLD = 5  # meters for height changes

logger = setup_logger()

def read_chunks(log_every=10):
    files = glob.glob(os.path.join(IN_DIR, FILE_PATTERN))
    chunks = []
    total_files = len(files)
    
    for i, file in enumerate(files):
        chunk = pd.read_csv(file)
        chunks.append(chunk)
        
        if i % log_every == 0 and i != 0:
            logger.info(f"Processed {i} of {total_files} files.")
        
    all_data = pd.concat(chunks, ignore_index=True)
    logger.success(f"All chunks read. Total files processed: {total_files}. Total measurements: {len(all_data)}.")
    return all_data

def process_data(df):
    df['Location'] = df['Location'].apply(lambda x: wkb.loads(bytes.fromhex(x)))
    df['Latitude'] = df['Location'].apply(lambda x: x.y)
    df['Longitude'] = df['Location'].apply(lambda x: x.x)

    # Handle non-finite 'Device ID' values
    if df['Device ID'].isnull().any() or np.isinf(df['Device ID']).any():
        logger.warning("Incorrect 'Device ID' values found. Removing affected rows.")
        df = df.dropna(subset=['Device ID'])  # Adjust according to your data handling policy

    # Convert Device ID to integer and set as index
    df.loc[:, 'Device ID'] = df['Device ID'].astype(int)
    df.set_index('Device ID', inplace=True)

    aggregated_data = df.groupby(['Device ID', 'Measurement Day']).agg({
        'Unit': 'first',  # Assuming 'Unit' doesn't change daily per device
        'Latitude': 'median',
        'Longitude': 'median',
        'Height': 'median',
        'Average Value': 'mean'
    }).reset_index()

    # Temporary columns for movement calculations
    aggregated_data['Prev Latitude'] = aggregated_data.groupby('Device ID')['Latitude'].shift(1)
    aggregated_data['Prev Longitude'] = aggregated_data.groupby('Device ID')['Longitude'].shift(1)
    aggregated_data['Prev Height'] = aggregated_data.groupby('Device ID')['Height'].shift(1)

    # Calculate movement and height difference
    aggregated_data['Movement'] = aggregated_data.apply(
        lambda row: geodesic((row['Prev Latitude'], row['Prev Longitude']), (row['Latitude'], row['Longitude'])).meters if pd.notnull(row['Prev Latitude']) else 0,
        axis=1
    )
    aggregated_data['Height Difference'] = (aggregated_data['Height'] - aggregated_data['Prev Height']).abs()

    # Determine status with height difference handling
    aggregated_data['Status'] = aggregated_data.apply(
        lambda row: 'Moving' if row['Movement'] > GPS_ACCURACY_THRESHOLD or (row['Height Difference'] > HEIGHT_MOVEMENT_THRESHOLD and pd.notnull(row['Height Difference'])) else 'Stationary',
        axis=1
    )

    # Remove temporary calculation columns before final output
    final_data = aggregated_data.drop(columns=['Prev Latitude', 'Prev Longitude', 'Prev Height', 'Movement', 'Height Difference'])

    logger.success("Movement calculation and status assignment completed.")
    return smooth_status(final_data)

def smooth_status(df):
    for device_id, group in df.groupby('Device ID'):
        statuses = group['Status'].values
        for i in range(1, len(statuses) - 1):
            if statuses[i-1] == statuses[i+1] and statuses[i] != statuses[i-1]:
                statuses[i] = statuses[i-1]
        df.loc[group.index, 'Status'] = statuses
        logger.info(f"Status smoothed for Device ID {device_id}")

    logger.success("Status smoothing completed for all devices.")
    return df

def save_results(df):
    results_file_path = os.path.join(OUT_DIR, OUT_FILE_NAME)
    df.to_csv(results_file_path, index=False)
    logger.success(f"Results saved to {results_file_path}")

if __name__ == "__main__":
    logger.info("Starting data processing...")
    df = read_chunks()
    processed_df = process_data(df)
    save_results(processed_df)
    logger.success("Data processing completed successfully.")
