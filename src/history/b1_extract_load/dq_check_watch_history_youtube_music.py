import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

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

DQ_LOG_DIR = PROJECT_ROOT / "data" / "processed" / "dq"
DQ_LOG_FILE = DQ_LOG_DIR / f"dq_watch_history_{datetime.utcnow().date().isoformat()}.csv"

ANALYSIS_DIR = PROJECT_ROOT / "data" / "processed" / "analysis"
REPLAYED_TRACKS_FILE = ANALYSIS_DIR / "tracks_played_multiple_times.csv"

# ============================================================
# MAIN
# ============================================================

def run_checks_and_analysis():
    print(f"➡️ Running DQ & usage analysis on {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    dq_results = []

    # ========================================================
    # BASIC DQ CHECKS
    # ========================================================

    dq_results.append({
        "check": "row_count",
        "value": len(df)
    })

    dq_results.append({
        "check": "missing_track_id",
        "value": df["track_id"].isna().sum()
    })

    dq_results.append({
        "check": "missing_title",
        "value": df["title"].isna().sum()
    })

    dq_results.append({
        "check": "missing_artist",
        "value": df["artist"].isna().sum()
    })

    dq_results.append({
        "check": "missing_played_at",
        "value": df["played_at"].isna().sum()
    })

    # played_at parsing
    played_at_parsed = pd.to_datetime(
        df["played_at"],
        errors="coerce",
        utc=True
    )

    dq_results.append({
        "check": "invalid_played_at_format",
        "value": played_at_parsed.isna().sum()
    })

    now_utc = datetime.now(timezone.utc)
    dq_results.append({
        "check": "future_plays",
        "value": (played_at_parsed > now_utc).sum()
    })

    # ========================================================
    # USAGE ANALYTICS — REPLAYS (THIS IS WHAT YOU WANT)
    # ========================================================

    plays_per_track = (
        df
        .groupby(["track_id", "source"], dropna=False)
        .size()
        .reset_index(name="play_count")
    )

    tracks_played_multiple_times = plays_per_track.query("play_count > 1")

    dq_results.append({
        "check": "tracks_played_multiple_times",
        "value": tracks_played_multiple_times["track_id"].nunique()
    })

    dq_results.append({
        "check": "total_replays",
        "value": tracks_played_multiple_times["play_count"].sum()
    })

    # ========================================================
    # SAVE ANALYSIS OUTPUT
    # ========================================================

    ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)

    tracks_played_multiple_times = tracks_played_multiple_times.sort_values(
        "play_count",
        ascending=False
    )

    tracks_played_multiple_times.to_csv(
        REPLAYED_TRACKS_FILE,
        index=False
    )

    # ========================================================
    # SAVE DQ LOG
    # ========================================================

    DQ_LOG_DIR.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(dq_results).to_csv(DQ_LOG_FILE, index=False)

    # ========================================================
    # CONSOLE OUTPUT
    # ========================================================

    print(f"✅ DQ log saved → {DQ_LOG_FILE}")
    print(f"📊 Replay analysis saved → {REPLAYED_TRACKS_FILE}")
    print("📊 Summary:")
    for row in dq_results:
        print(f" - {row['check']}: {row['value']}")

# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    run_checks_and_analysis()
