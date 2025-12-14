import pandas as pd
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[3]

INPUT_FILE = PROJECT_ROOT / "data" / "interim" / "history" / "spotify_enriched_history.csv"
LOG_DIR = PROJECT_ROOT / "data" / "processed" / "dq"
LOG_FILE = LOG_DIR / f"dq_spotify_enriched_history{datetime.utcnow().date().isoformat()}.csv"


def run_dq_checks():
    print(f"➡️ Running data quality checks on {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    dq_results = []

    # 1. Row count
    dq_results.append({
        "check": "row_count",
        "value": len(df)
    })

    # 2. Missing source_track_id (YT reference)
    missing_source_track_id = df["source_track_id"].isna().sum()
    dq_results.append({
        "check": "missing_source_track_id",
        "value": missing_source_track_id
    })

    # 3. Missing spotify_track_id (CRITICAL)
    missing_spotify_track_id = df["spotify_track_id"].isna().sum()
    dq_results.append({
        "check": "missing_spotify_track_id",
        "value": missing_spotify_track_id
    })

    # 4. Missing duration_seconds (Spotify enrichment)
    missing_duration = df["duration_seconds"].isna().sum()
    dq_results.append({
        "check": "missing_duration_seconds",
        "value": missing_duration
    })

    # 5. Missing genres (Spotify)
    missing_genres = df["genres"].isna().sum()
    dq_results.append({
        "check": "missing_genres",
        "value": missing_genres
    })

    # 6. Duplicate spotify_track_id
    duplicate_spotify_ids = df.duplicated(
        subset=["spotify_track_id"]
    ).sum()
    dq_results.append({
        "check": "duplicate_spotify_track_id",
        "value": duplicate_spotify_ids
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
