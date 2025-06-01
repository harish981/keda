import os
import pandas as pd
import redis
from typing import Tuple

def read_csv_from_redis() -> Tuple[str, pd.DataFrame]:
    redis_host = os.getenv("REDIS_HOST")
    redis_list = os.getenv("REDIS_LIST")

    if not redis_host or not redis_list:
        print(f"Missing environment variables. REDIS_HOST: {redis_host}, REDIS_LIST: {redis_list}")
        exit()

    redis_client = redis.Redis(host=redis_host, port=6379, db=0)
    last_csv_name = redis_client.rpop(redis_list)
    if not last_csv_name:
        print("No CSV files to process, exiting.")
        exit()

    last_csv_name = last_csv_name.decode("utf-8")
    full_filename = os.path.join(os.path.dirname(__file__), '..', 'data/raw', last_csv_name)
    df = pd.read_csv(full_filename)
    return last_csv_name, df

def aggregate_csv(filename_raw: str, df: pd.DataFrame) -> None:
    aggregated_data = df.groupby('item_id')['quantity_sold'].sum().reset_index()
    processed_dir = os.path.join(os.path.dirname(__file__), '..', 'data/processed')
    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)
    output_filename = os.path.join(processed_dir, f"{filename_raw.split('.')[0]}_aggregated_sales.csv")
    aggregated_data.to_csv(output_filename, index=False)
    print(f"Aggregated sales data written to '{output_filename}'.")

def main():
    filename, df = read_csv_from_redis()
    aggregate_csv(filename, df)

if __name__ == '__main__':
    main()

