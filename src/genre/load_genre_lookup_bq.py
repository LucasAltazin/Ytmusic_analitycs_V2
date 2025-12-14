from google.cloud import bigquery
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
INPUT_FILE = PROJECT_ROOT / "data" / "raw" / "genre_lookup" / "genre_lookup.csv"
SERVICE_ACCOUNT = PROJECT_ROOT / "secrets" / "ytmusic-analytics-478417-692a6c5d2282.json"

TABLE_ID = "ytmusic-analytics-478417.ytmusic_raw.genre_lookup"


def load_genre_lookup():
    print(f"➡️ Loading lookup file: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)

    client = bigquery.Client.from_service_account_json(str(SERVICE_ACCOUNT))

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("spotify_raw_genre", "STRING"),
            bigquery.SchemaField("main_genre", "STRING"),
            bigquery.SchemaField("sub_genre", "STRING"),
        ],
    )

    print("⬆️ Uploading to BigQuery...")
    job = client.load_table_from_dataframe(
        df, TABLE_ID, job_config=job_config, location="EU"
    )
    job.result()

    print(f"✅ Loaded {len(df)} genres into {TABLE_ID}")


if __name__ == "__main__":
    load_genre_lookup()
