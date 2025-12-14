from google.cloud import bigquery
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
INPUT_FILE = PROJECT_ROOT / "data" / "interim" / "library_clean.csv"

SERVICE_ACCOUNT = PROJECT_ROOT / "secrets" / "ytmusic-analytics-478417-692a6c5d2282.json"
TABLE_ID = "ytmusic-analytics-478417.ytmusic_raw.raw_library_test"


def load_to_bigquery():
    print(f"➡️ Loading cleaned file: {INPUT_FILE}")

    # Load CSV
    df = pd.read_csv(INPUT_FILE)

    # BigQuery client
    client = bigquery.Client.from_service_account_json(str(SERVICE_ACCOUNT))

    # Load configuration
    job_config = bigquery.LoadJobConfig(
        autodetect=False,
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
    )

    # Load to BigQuery with location enforced to EU
    job = client.load_table_from_dataframe(
        df,
        TABLE_ID,
        job_config=job_config,
        location="EU"
    )
    job.result()  # Wait for job to complete

    print(f"✅ Loaded {len(df)} rows into {TABLE_ID}")


if __name__ == "__main__":
    load_to_bigquery()
