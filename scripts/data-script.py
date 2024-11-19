import time
import requests
import pandas as pd
from google.cloud import storage
from xml.etree import ElementTree

# Configure logging
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the URL for the BoardGameGeek API and other constants
BGG_API_URL = "https://www.boardgamegeek.com/xmlapi2/thing"
GAME_IDS = ["1", "2", "3"]  # Example game IDs, replace with your actual game IDs
BATCH_SIZE = 100  # Upload to GCS in batches of 100 records
GCS_BUCKET_NAME = 'bgf_game_data_bronze_layer'
GCS_CLIENT = storage.Client.from_service_account_json('/Users/aarda/Downloads/bgf-project-442110-0572b0978980.json')

# Function to fetch game data from the BoardGameGeek API
def get_game_data(game_id):
    try:
        response = requests.get(BGG_API_URL, params={"id": game_id})
        response.raise_for_status()  # Raise an error for bad responses (4xx, 5xx)
        return response.text
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data for game ID {game_id}: {e}")
        return None

# Function to parse XML game data
def parse_game_data(xml_data):
    try:
        root = ElementTree.fromstring(xml_data)
        game_data = []

        for item in root.findall("item"):
            game_info = {
                'id': item.get("id"),
                'name': item.find("name").text,
                'yearpublished': item.find("yearpublished").text,
                'minplayers': item.find("minplayers").text,
                'maxplayers': item.find("maxplayers").text,
                'playingtime': item.find("playingtime").text,
                'description': item.find("description").text
            }
            game_data.append(game_info)

        return pd.DataFrame(game_data)

    except Exception as e:
        logger.error(f"Error parsing XML data: {e}")
        return pd.DataFrame()

# Function to fetch all games and aggregate them
def fetch_all_games(game_ids):
    all_games_data = []
    for i, game_id in enumerate(game_ids, start=1):
        logger.info(f"Fetching data for game {i}/{len(game_ids)} (ID: {game_id})")
        xml_data = get_game_data(game_id)

        if xml_data:
            game_df = parse_game_data(xml_data)
            if not game_df.empty:
                all_games_data.append(game_df)
        
        # Sleep to prevent hitting API rate limits
        time.sleep(2)

    # Concatenate all fetched game data into a single DataFrame
    if all_games_data:
        return pd.concat(all_games_data, ignore_index=True)
    return pd.DataFrame()  # Return an empty DataFrame if no data was fetched

# Function to upload data to GCS in batches
def upload_to_gcs(df, bucket_name):
    logger.info("Starting GCS upload...")
    bucket = GCS_CLIENT.bucket(bucket_name)
    
    # Process and upload the data in batches
    for i in range(0, len(df), BATCH_SIZE):
        batch_df = df.iloc[i:i + BATCH_SIZE]
        batch_csv = batch_df.to_csv(index=False)
        blob = bucket.blob(f"bgf_games_batch_{i//BATCH_SIZE + 1}.csv")
        blob.upload_from_string(batch_csv, content_type="text/csv")
        logger.info(f"Uploaded batch {i//BATCH_SIZE + 1} to GCS")
    
    logger.info("GCS upload complete.")

# Main execution function
def main():
    # Fetch all game data
    logger.info("Fetching all game data...")
    all_games_df = fetch_all_games(GAME_IDS)

    # If we have data, upload it to GCS
    if not all_games_df.empty:
        upload_to_gcs(all_games_df, GCS_BUCKET_NAME)
    else:
        logger.warning("No game data to upload.")

if __name__ == "__main__":
    main()