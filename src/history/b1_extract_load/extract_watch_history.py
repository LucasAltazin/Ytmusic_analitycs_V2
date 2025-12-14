import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, parse_qs

# ============================================================
# PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]

WATCH_HISTORY_FILE = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "takeout"
    / "youtube_music"
    / "history"
    / "watch-history.json"
)

OUTPUT_FILE = (
    PROJECT_ROOT
    / "data"
    / "interim"
    / "history"
    / "watch_history_youtube_music.csv"
)

# ============================================================
# HELPERS
# ============================================================

def extract_video_id(url: str) -> str | None:
    """Extract YouTube video ID from a YouTube Music URL."""
    if not url:
        return None
    parsed = urlparse(url)
    return parse_qs(parsed.query).get("v", [None])[0]


def clean_title(title: str | None) -> str | None:
    """
    Remove the 'Watched ' prefix added by Google Takeout
    in watch-history.json for YouTube Music entries.

    Example:
        'Watched Girl!' -> 'Girl!'
    """
    if not isinstance(title, str):
        return title

    if title.startswith("Watched "):
        return title.replace("Watched ", "", 1)

    return title

import re

def clean_artist(artist: str | None) -> str | None:
    """
    Clean artist string coming from YouTube Music / Google Takeout.

    Removes the ' - Topic' suffix (with optional spaces).
    """
    if not isinstance(artist, str):
        return artist

    artist = artist.strip()
    return re.sub(r"\s*-\s*Topic\s*$", "", artist)




def extract_artist(event: dict) -> str | None:
    """
    Extract artist name from the 'subtitles' field
    of a YouTube Music watch history event.
    """
    subtitles = event.get("subtitles")
    if isinstance(subtitles, list) and len(subtitles) > 0:
        return subtitles[0].get("name")
    return None


# ============================================================
# EXTRACTION
# ============================================================

def extract_watch_history_youtube_music():
    print("➡️ Loading watch-history.json...")

    with open(WATCH_HISTORY_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    rows = []

    for event in data:
        # Keep only YouTube Music events
        if event.get("header") != "YouTube Music":
            continue

        # NOTE: Google Takeout prefixes titles with 'Watched ' in watch history
        raw_title = event.get("title")
        title = clean_title(raw_title)

        url = event.get("titleUrl")
        video_id = extract_video_id(url)

        raw_artist = extract_artist(event)
        artist = clean_artist(raw_artist)


        rows.append({
            "track_id": video_id,
            "title": title,
            "artist": artist,
            "album": None,               # Not available in watch history
            "duration_seconds": None,
            "liked": None,
            "ytm_url": url,
            "source": "watch_history",
            "played_at": event.get("time"),
            "extraction_date": datetime.utcnow().date().isoformat(),
        })

    df = pd.DataFrame(rows)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)

    print(f"✅ Saved watch history → {OUTPUT_FILE}")
    print(f"📊 Total YouTube Music plays: {len(df)}")


# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    extract_watch_history_youtube_music()
