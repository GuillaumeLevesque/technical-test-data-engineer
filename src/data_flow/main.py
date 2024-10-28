import requests
import pandas as pd
import logging
import os
import schedule
import time

log_dir = 'log'
log_file = 'etl.log'
log_path = os.path.join(log_dir, log_file)
os.makedirs(log_dir, exist_ok=True)

if not os.path.exists(log_path):
    open(log_path, 'w').close()

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def extract_items(items_name):
    api_url = 'http://127.0.0.1:8000'
    all_items = []
    page = 1
    size = 100

    while True:
        response = requests.get(f'{api_url}/{items_name}?page={page}&size={size}')

        if response.status_code != 200:
            logging.error(f"Failed to retrieve data: {response.status_code}")
            break

        data = response.json()
        items = data.get("items", [])

        if not items:
            break

        all_items.extend(items)

        total_pages = data.get("pages", 1)

        if page >= total_pages:
            break

        page += 1

    return all_items

def transform_data(raw_data, filename: str):
    df = pd.DataFrame(raw_data)

    # list to tuple
    for column in df.columns:
        if df[column].map(type).eq(list).any():
            df[column] = df[column].map(lambda x: tuple(x) if isinstance(x, list) else x)

    # save initial data
    load_data_to_csv(df, 'raw', filename)

    # empty
    df = df.dropna()

    # duplicates
    df = df.drop_duplicates()

    # convert dates
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])

    # more cleaning and validation
    # ..

    print('\n', df.head())
    return df

def load_data_to_csv(df, folder: str, file_name: str):
    output_dir = folder
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, file_name)

    try:
        existing_df = pd.read_csv(filepath)
        df = pd.concat([existing_df, df]).drop_duplicates().reset_index(drop=True)
    except FileNotFoundError:
        pass

    df.to_csv(filepath, index=False)
    print(f"\n{file_name} length: {len(df)}")

def etl():
    logging.info("ETL job started")

    # Extract
    daily_tracks = extract_items('tracks')
    daily_users = extract_items('users')
    daily_listen_history = extract_items('listen_history')
    print('\nextracted')
    print(len(daily_tracks), len(daily_users), len(daily_listen_history))

    # Transform
    daily_tracks_df = transform_data(daily_tracks, 'raw_tracks.csv')
    daily_users_df = transform_data(daily_users, 'raw_users.csv')
    daily_listen_history_df = transform_data(daily_listen_history, 'raw_listen_history.csv')
    print('\ntransformed')
    print(len(daily_tracks_df), len(daily_users_df), len(daily_listen_history_df))

    # Load
    load_data_to_csv(daily_tracks_df, 'clean', 'tracks.csv')
    load_data_to_csv(daily_users_df, 'clean', 'users.csv')
    load_data_to_csv(daily_listen_history_df, 'clean', 'listen_history.csv')

    logging.info("ETL job completed")

if __name__ == "__main__":
    schedule.every(1).days.do(etl)
    # schedule.every(15).seconds.do(etl)
    while True:
        schedule.run_pending()
        time.sleep(1)