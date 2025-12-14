from google.cloud import bigquery
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]

INPUT_FILE = PROJECT_ROOT / "data" / "interim" / "spotify_enriched_library.csv"
SERVICE_ACCOUNT = PROJECT_ROOT / "secrets" / "ytmusic-analytics-478417-692a6c5d2282.json"
TABLE_ID = "ytmusic-analytics-478417.ytmusic_raw.raw_spotify_library"


def load_spotify_enrichment():
    print(f"➡️ Loading enriched Spotify file: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    # FIXES
    df["release_year"] = df["release_year"].astype(str)
    df["extraction_date"] = pd.to_datetime(df["extraction_date"]).dt.date

    # BigQuery client
    client = bigquery.Client.from_service_account_json(str(SERVICE_ACCOUNT))

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=False,
        schema=[
            bigquery.SchemaField("source_track_id", "STRING"),
            bigquery.SchemaField("title_original", "STRING"),
            bigquery.SchemaField("artist_original", "STRING"),
            bigquery.SchemaField("album_original", "STRING"),
            bigquery.SchemaField("source", "STRING"),

            bigquery.SchemaField("spotify_track_id", "STRING"),
            bigquery.SchemaField("spotify_artist_id", "STRING"),
            bigquery.SchemaField("spotify_album_id", "STRING"),
            bigquery.SchemaField("release_year", "STRING"),

            bigquery.SchemaField("duration_ms", "INTEGER"),
            bigquery.SchemaField("duration_seconds", "FLOAT"),
            bigquery.SchemaField("popularity", "INTEGER"),
            bigquery.SchemaField("explicit", "BOOLEAN"),
            bigquery.SchemaField("genres", "STRING"),

            bigquery.SchemaField("extraction_date", "DATE"),
        ],
    )

    print("⬆️ Uploading to BigQuery…")

    job = client.load_table_from_dataframe(
        df,
        TABLE_ID,
        job_config=job_config,
        location="EU"
    )
    job.result()

    print(f"✅ Loaded {len(df)} rows into {TABLE_ID}")


if __name__ == "__main__":
    load_spotify_enrichment()
