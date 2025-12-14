# src/config/paths.py
from pathlib import Path
from datetime import datetime

# Racine du projet = ytmusic-analytics/
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Dossiers principaux
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
PROCESSED_DIR = DATA_DIR / "processed"
SECRETS_DIR = PROJECT_ROOT / "secrets"

# --- Takeout (Product B) ---
RAW_TAKEOUT_DIR = RAW_DIR / "takeout" / "youtube_music"
RAW_TAKEOUT_HISTORY_DIR = RAW_TAKEOUT_DIR / "history"
RAW_TAKEOUT_LIBRARY_DIR = RAW_TAKEOUT_DIR / "music_library"
RAW_TAKEOUT_PLAYLISTS_DIR = RAW_TAKEOUT_DIR / "playlists"

WATCH_HISTORY_JSON = RAW_TAKEOUT_HISTORY_DIR / "watch-history.json"
SEARCH_HISTORY_JSON = RAW_TAKEOUT_HISTORY_DIR / "search-history.json"

PROCESSED_HISTORY_DIR = PROCESSED_DIR / "history"


def today_suffix():
    """Suffix date pour nommer les fichiers, ex: 2025-11-27."""
    return datetime.utcnow().strftime("%Y-%m-%d")

