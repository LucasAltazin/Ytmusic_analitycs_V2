from google.cloud import bigquery
from pathlib import Path
import pandas as pd

# ============================================================
# PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]

INPUT_FILE = (
    PROJECT_ROOT
    / "data"
    / "interim"
    / "history"
    / "watch_history_youtube_music.csv"
)

SERVICE_ACCOUNT = (
    PROJECT_ROOT
    / "secrets"
    / "ytmusic-analytics-478417-692a6c5d2282.json"
)

TABLE_ID = "ytmusic-analytics-478417.ytmusic_raw.raw_watch_history_youtube_music"

# ============================================================
# LOAD TO BIGQUERY
# ============================================================

def load_to_bigquery():
    print(f"➡️ Loading watch history file: {INPUT_FILE}")

    # --------------------------------------------------------
    # Load CSV
    # --------------------------------------------------------
    df = pd.read_csv(INPUT_FILE)

    # --------------------------------------------------------
    # BigQuery client
    # --------------------------------------------------------
    client = bigquery.Client.from_service_account_json(
        str(SERVICE_ACCOUNT)
    )

    # --------------------------------------------------------
    # Load configuration
    # --------------------------------------------------------
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,               # OK for raw layer
        skip_leading_rows=1,
    )

    # --------------------------------------------------------
    # Load job (EU location enforced)
    # --------------------------------------------------------
    job = client.load_table_from_dataframe(
        df,
        TABLE_ID,
        job_config=job_config,
        location="EU"
    )

    job.result()  # Wait for completion

    print(f"✅ Loaded {len(df)} rows into {TABLE_ID}")

# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    load_to_bigquery()
