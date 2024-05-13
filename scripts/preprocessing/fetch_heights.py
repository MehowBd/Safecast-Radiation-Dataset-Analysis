import os
import pandas as pd
import requests
from logger import setup_logger
from dotenv import load_dotenv

load_dotenv()

API_URL = os.environ.get('API_URL')
API_KEY = os.getenv('API_KEY')

# Directory and file configuration
IN_DIR = 'data'
FILE_PATTERN = 'measurements.csv'
# Replace the existing data file
OUT_DIR = 'data'
OUT_FILE_NAME = 'measurements.csv'

LOG_EVERY = 100

logger = setup_logger()

def fetch_height(lat, lon, *, cache = {}):
    cache_key = f"{lat},{lon}"
    
    if cache_key not in cache:
        logger.info(f"Fetching height for location {lat}, {lon}.")
        response = requests.get(f'{API_URL}?locations={lat},{lon}&key={API_KEY}')
        
        if response.status_code == 200:
            results = response.json()['results']
            if not results:
                logger.warning(f"No results found for location {lat}, {lon}.")
                return None
            height = results[0]['elevation']
            cache[cache_key] = height
        else:
            logger.error(f"Failed to fetch height for location {lat}, {lon}.")
            return None
    else:
        logger.info(f"Using cached height for location {lat}, {lon}.")
    
    return cache[cache_key]

def fetch_missing_heights(df):
    missing_heights = df['Height'].isnull()
    if missing_heights.any():
        for i, row in df[missing_heights].iterrows():
            height = fetch_height(row['Latitude'], row['Longitude'])
            if height is not None:
                df.at[i, 'Height'] = height
            if i % LOG_EVERY == 0 and i != 0:
                logger.info(f"Processed {i} of {len(df[missing_heights])} missing heights.")
    else:
        logger.success("No missing heights found.")
    
    return df

if __name__ == '__main__':
    # Read the data
    df = pd.read_csv(os.path.join(IN_DIR, FILE_PATTERN))
    logger.info(f"Read {len(df)} measurements from {FILE_PATTERN}.")

    # Fetch heights
    df = fetch_missing_heights(df)
    
    # Save the updated df
    df.to_csv(os.path.join(OUT_DIR, OUT_FILE_NAME), index=False)
    logger.success(f"Updated measurements saved to {OUT_FILE_NAME}.")
