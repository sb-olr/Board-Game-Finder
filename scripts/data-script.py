import requests
import xml.etree.ElementTree as ET
import pandas as pd
import time
from google.cloud import storage
import os

# Set Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/ademircies/.gcp_keys/bgf-project-442110-0572b0978980.json"

def get_game_data(game_ids):
    """Fetch game data from BoardGameGeek API."""
    url = "https://boardgamegeek.com/xmlapi2/thing"
    params = {"id": ",".join(map(str, game_ids)), "stats": 1}
    
    print(f"Fetching IDs {game_ids[0]}-{game_ids[-1]}")
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        return ET.fromstring(response.content)
    elif response.status_code == 429:
        print("Rate limit hit. Retrying...")
        time.sleep(4)
        return get_game_data(game_ids)
    else:
        print(f"Error {response.status_code}: {response.text}")
        return None

def parse_game_data(xml_root):
    """Parse XML data into a structured format."""
    games_data = []
    for item in xml_root.findall('.//item'):
        game_data = {
            'id': int(item.get('id')),
            'name': item.find(".//name[@type='primary']").get('value') if item.find(".//name[@type='primary']") is not None else None,
            'year': int(item.find('yearpublished').get('value')) if item.find('yearpublished') is not None else None,
            'min_players': int(item.find('minplayers').get('value')) if item.find('minplayers') is not None else None,
            'max_players': int(item.find('maxplayers').get('value')) if item.find('maxplayers') is not None else None,
            'playing_time': int(item.find('playingtime').get('value')) if item.find('playingtime') is not None else None,
            'min_playtime': int(item.find('minplaytime').get('value')) if item.find('minplaytime') is not None else None,
            'max_playtime': int(item.find('maxplaytime').get('value')) if item.find('maxplaytime') is not None else None,
            'min_age': int(item.find('minage').get('value')) if item.find('minage') is not None else None,
            'weight': float(item.find('.//averageweight').get('value')) if item.find('.//averageweight') is not None else None,
            'num_ratings': int(item.find('.//usersrated').get('value')) if item.find('.//usersrated') is not None else None,
            'avg_rating': float(item.find('.//average').get('value')) if item.find('.//average') is not None else None,
            'bayes_avg_rating': float(item.find('.//bayesaverage').get('value')) if item.find('.//bayesaverage') is not None else None,
            'num_owners': int(item.find('.//owned').get('value')) if item.find('.//owned') is not None else None,
            'categories': ', '.join([link.get('value') for link in item.findall(".//link[@type='boardgamecategory']")]),
            'mechanics': ', '.join([link.get('value') for link in item.findall(".//link[@type='boardgamemechanic']")]),
            'themes': ', '.join([link.get('value') for link in item.findall(".//link[@type='boardgamefamily']")]),
            'Description': item.find('description').text if item.find('description') is not None else None,
            'GameWeight': float(item.find('.//averageweight').get('value')) if item.find('.//averageweight') is not None else None,
            'StdDev': float(item.find('.//standarddeviation').get('value')) if item.find('.//standarddeviation') is not None else None,
            'ComAgeRec': int(item.find('.//minage').get('value')) if item.find('.//minage') is not None else None,
            'LanguageEase': item.find('.//languageease').text if item.find('.//languageease') is not None else None,
            'BestPlayers': ', '.join([link.get('value') for link in item.findall(".//link[@type='bestplayers']")]),
            'GoodPlayers': ', '.join([link.get('value') for link in item.findall(".//link[@type='goodplayers']")]),
            'NumWant': int(item.find('.//wanteds').get('value')) if item.find('.//wanteds') is not None else 0,
            'NumWish': int(item.find('.//wishlisted').get('value')) if item.find('.//wishlisted') is not None else 0,
            'NumWeightVotes': int(item.find('.//usersrated').get('value')) if item.find('.//usersrated') is not None else 0,
            'MfgPlaytime': int(item.find('playingtime').get('value')) if item.find('playingtime') is not None else 0,
            'ComMinPlaytime': int(item.find('.//minplaytime').get('value')) if item.find('.//minplaytime') is not None else 0,
            'ComMaxPlaytime': int(item.find('.//maxplaytime').get('value')) if item.find('.//maxplaytime') is not None else 0,
            'MfgAgeRec': int(item.find('.//minage').get('value')) if item.find('.//minage') is not None else 0,
            'NumUserRatings': int(item.find('.//usersrated').get('value')) if item.find('.//usersrated') is not None else 0,
            'NumComments': int(item.find('.//numcomments').get('value')) if item.find('.//numcomments') is not None else 0,
            'NumAlternates': int(item.find('.//numalternates').get('value')) if item.find('.//numalternates') is not None else 0,
            'NumExpansions': int(item.find('.//numexpansions').get('value')) if item.find('.//numexpansions') is not None else 0,
            'NumImplementations': int(item.find('.//numimplementations').get('value')) if item.find('.//numimplementations') is not None else 0,
            'IsReimplementation': bool(int(item.find(".//isreimplementation").text)) if item.find(".//isreimplementation") is not None else False,
            'Family': ', '.join([link.get("value") for link in item.findall(".//link[@type='boardgamefamily']")]),
            'Kickstarted': bool(int(item.find(".//kickstarted").text)) if item.find(".//kickstarted") is not None else False,
            'ImagePath': item.find('./image').text if item.find('./image') is not None else '',
            'rank:boardgame': None
        }
        
        # Handle the rank separately due to its more complex structure
        rank_element = item.find(".//rank[@name='boardgame']")
        if rank_element is not None and rank_element.get("value") != "Not Ranked":
            try:
                game_data['rank:boardgame'] = int(rank_element.get("value"))
            except ValueError:
                game_data['rank:boardgame'] = None
        
        games_data.append(game_data)
    return games_data

import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def upload_to_gcs(dataframe, destination_blob_name, bucket_name='bgf_game_data_bronze_layer'):
    """Upload DataFrame to Google Cloud Storage as a CSV file."""
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        
        csv_buffer = dataframe.to_csv(index=False)
        blob = bucket.blob(destination_blob_name)
        
        blob.upload_from_string(csv_buffer, content_type='text/csv')
        logging.info(f"Successfully uploaded: {destination_blob_name} to {bucket_name}")
    except Exception as e:
        logging.error(f"Error uploading {destination_blob_name}: {str(e)}")
        raise

def fetch_and_upload_game_data(total_games=200000, batch_size=20, bucket_name='bgf_game_data_bronze_layer'):
    """Fetch game data and upload to Google Cloud Storage."""
    all_games_data = []
    batch_number = 1
    
    for current_id in range(1, total_games + 1, batch_size):
        end_id = min(current_id + batch_size - 1, total_games)
        game_ids = list(range(current_id, end_id + 1))
        
        xml_data = get_game_data(game_ids)
        if xml_data is not None:
            games_data = parse_game_data(xml_data)
            all_games_data.extend(games_data)
            logging.info(f"Fetched {len(games_data)} games. Total games: {len(all_games_data)}")

        # Upload in batches of 1000
        while len(all_games_data) >= 1000:
            df = pd.DataFrame(all_games_data[:1000])
            file_name = f'bgf_game_data_{batch_number}.csv'
            try:
                upload_to_gcs(df, file_name, bucket_name)
                logging.info(f"Uploaded batch {batch_number} (IDs {current_id-999}-{current_id})")
            except Exception as e:
                logging.error(f"Failed to upload batch {batch_number}: {str(e)}")
            
            all_games_data = all_games_data[1000:]  # Keep remaining data
            batch_number += 1

    # Final upload for any remaining data
    if all_games_data:
        df = pd.DataFrame(all_games_data)
        file_name = f'bgf_game_data_{batch_number}.csv'
        try:
            upload_to_gcs(df, file_name, bucket_name)
            logging.info(f"Uploaded final batch {batch_number}")
        except Exception as e:
            logging.error(f"Failed to upload final batch: {str(e)}")

    logging.info("Data fetching and uploading complete.")

# Example usage
start_time = time.time()
fetch_and_upload_game_data(total_games=200000, batch_size=20)
end_time = time.time()

logging.info(f"Total time taken: {end_time - start_time:.2f} seconds")
