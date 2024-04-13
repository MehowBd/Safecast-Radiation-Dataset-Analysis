import os
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from utils.logger import setup_logger  # Import the logger setup

# Constants
FILE_PREFIX = 'measurements'
DATA_DIR = 'temp'
RETRY_LIMIT = 5
DECREASE_FACTOR = 2
MIN_INCREMENT = 1
INITIAL_INCREMENT = 180

# Setup logger
logger = setup_logger()

# Ensure the data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

# Load environment variables
load_dotenv()

# Database connection parameters
DB_PARAMS = {
    'database': os.getenv('DATABASE'),
    'user': os.getenv('USER'),
    'password': os.getenv('PASSWORD'),
    'host': os.getenv('HOST'),
    'port': os.getenv('PORT')
}

def connect_database():
    """Safely establish a connection to the database."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        logger.info("Successfully connected to the database.")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Failed to connect to the database: {e}")
        exit(1)

def execute_query(conn, condition, file_suffix):
    """Execute a query and save the results to a CSV file if non-empty, using the connection provided."""
    with conn.cursor() as cursor:
        query = f"""
        SELECT
            m.device_id,
            m.unit,
            m.location,
            m.height,
            m.captured_at::date AS measurement_day,
            AVG(m.value) AS average_value
        FROM
            public.measurements AS m
        WHERE
            {condition} AND (m.device_id IS NOT NULL OR m.height IS NOT NULL)
        GROUP BY
            m.device_id, m.unit, m.location, m.height, measurement_day
        ORDER BY
            m.device_id;
        """
        try:
            cursor.execute(query)
            logger.debug(f"Query executed: {query}")
            results = cursor.fetchall()
            if results:
                df = pd.DataFrame(results)
                if not df.empty:
                    filename = os.path.join(DATA_DIR, f"{FILE_PREFIX}_{file_suffix}.csv")
                    df.to_csv(filename, index=False)
                    logger.info(f"Results saved to {filename}.")
                else:
                    logger.info("No data to save. Data frame was empty.")
            else:
                logger.info("Query returned no data.")
            return True
        except psycopg2.Error as e:
            logger.error(f"Failed to execute query: {e}")
            return False

def execute_queries(start_date, increment):
    conn = connect_database()  # Initial connection
    try:
        date_list = determine_dates(start_date, increment)
        for current_date in date_list:
            retries = RETRY_LIMIT
            condition, file_suffix = determine_query_params(current_date, increment)
            logger.debug(f"Processing {current_date.strftime('%Y-%m-%d')} with condition {condition}.")

            while retries > 0:
                success = execute_query(conn, condition, file_suffix)
                if success:
                    break
                retries -= 1
                if retries > 0:
                    logger.warning(f"Retrying... {retries} retries left.")
                else:
                    logger.error("Failed to execute query after multiple retries.")
                    break  # Break out of the loop if retries are exhausted
                try:
                    conn.rollback()  # Attempt to clean up the connection
                except psycopg2.InterfaceError:
                    logger.warning("Connection already closed. Reconnecting...")
                    conn = connect_database()  # Reconnect if the connection was closed
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

def determine_dates(start_date, increment):
    """Generate date list based on the increment type."""
    if increment == 'before':
        return [start_date]
    elif isinstance(increment, int):
        return [start_date + timedelta(days=i) for i in range(0, (datetime.now() - start_date).days, increment)]
    else:
        logger.error("Invalid increment type. Exiting.")
        exit(1)

def determine_query_params(current_date, increment):
    """Helper to determine query conditions and file suffix based on date and increment."""
    if increment == 'before':
        condition = f"m.captured_at < '{current_date.strftime('%Y-%m-%d')}'"
        file_suffix = f"before_{current_date.strftime('%Y-%m-%d')}"
    else:
        next_date = current_date + timedelta(days=increment)
        condition = f"m.captured_at BETWEEN '{current_date.strftime('%Y-%m-%d')}' AND '{next_date.strftime('%Y-%m-%d')}'"
        file_suffix = f"{current_date.strftime('%Y-%m-%d')}_to_{next_date.strftime('%Y-%m-%d')}"
    return condition, file_suffix

if __name__ == '__main__':
    execute_queries(datetime(2015, 6, 4), INITIAL_INCREMENT)
