import pandas as pd
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[3]

INPUT_FILE = PROJECT_ROOT / "data" / "interim" / "library_clean.csv"
LOG_DIR = PROJECT_ROOT / "data" / "processed" / "dq"
LOG_FILE = LOG_DIR / f"dq_log_{datetime.utcnow().date().isoformat()}.csv"


def run_dq_checks():
    print(f"➡️ Running data quality checks on {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    dq_results = []

    # 1. Count total rows
    dq_results.append({
        "check": "row_count",
        "value": len(df)
    })

    # 2. Missing artist
    missing_artist = df["artist"].isna().sum()
    dq_results.append({
        "check": "missing_artist",
        "value": missing_artist
    })

    # 3. Missing album
    missing_album = df["album"].isna().sum()
    dq_results.append({
        "check": "missing_album",
        "value": missing_album
    })

    # 4. Duplicate (track_id + source)
    duplicates = df.duplicated(subset=["track_id", "source"]).sum()
    dq_results.append({
        "check": "duplicate_track_source",
        "value": duplicates
    })

    # Save log
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(dq_results).to_csv(LOG_FILE, index=False)

    print(f"✅ Data Quality checks saved → {LOG_FILE}")
    print("📊 Summary:")
    for row in dq_results:
        print(f" - {row['check']}: {row['value']}")


if __name__ == "__main__":
    run_dq_checks()
