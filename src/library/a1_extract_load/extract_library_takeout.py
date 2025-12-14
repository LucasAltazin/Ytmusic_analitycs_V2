import pandas as pd
from pathlib import Path
from datetime import datetime
import json

PROJECT_ROOT = Path(__file__).resolve().parents[3]

LIBRARY_FILE = PROJECT_ROOT / "data" / "raw" / "takeout" / "youtube_music" / "music (library and uploads)" / "music library songs.csv"
PLAYLISTS_DIR = PROJECT_ROOT / "data" / "raw" / "takeout" / "youtube_music" / "playlists"


ALLOWLIST_FILE = PROJECT_ROOT / "src" / "config" / "playlists_allowlist.json"
OUTPUT_FILE = PROJECT_ROOT / "data" / "interim" / "library" / "library_clean.csv"


def load_main_library():
    df = pd.read_csv(LIBRARY_FILE)

    df["artist"] = df["Artist Name 1"]
    df["ytm_url"] = df["Video ID"].apply(lambda x: f"https://music.youtube.com/watch?v={x}")

    df_lib = pd.DataFrame({
        "track_id": df["Video ID"],
        "title": df["Song Title"],
        "artist": df["artist"],
        "album": df["Album Title"],
        "duration_seconds": None,
        "liked": None,
        "ytm_url": df["ytm_url"],
        "source": "library",
        "extraction_date": datetime.utcnow().date().isoformat(),
    })

    return df_lib, df  # df = raw library used for lookups


def extract_playlists(df_raw_library, allowed):
    rows = []

    for playlist_file in PLAYLISTS_DIR.glob("*.csv"):
        playlist_name = playlist_file.stem.replace("-videos", "").strip()

        if playlist_name not in allowed:
            print(f"   ⛔ Ignored playlist: {playlist_name}")
            continue

        print(f"   ✔ Loading playlist: {playlist_name}")

        df_pl = pd.read_csv(playlist_file)

        # Only Video ID -> join with library to recover metadata
        merged = df_pl.merge(
            df_raw_library,
            on="Video ID",
            how="left"
        )

        merged["ytm_url"] = merged["Video ID"].apply(lambda x: f"https://music.youtube.com/watch?v={x}")

        merged_clean = pd.DataFrame({
            "track_id": merged["Video ID"],
            "title": merged["Song Title"],
            "artist": merged["Artist Name 1"],
            "album": merged["Album Title"],
            "duration_seconds": None,
            "liked": None,
            "ytm_url": merged["ytm_url"],
            "source": f"playlist:{playlist_name}",
            "extraction_date": datetime.utcnow().date().isoformat(),
        })

        rows.append(merged_clean)

    if rows:
        return pd.concat(rows, ignore_index=True)
    else:
        return pd.DataFrame([])


def extract_library_and_playlists():
    print("➡️ Loading main library...")
    df_library_clean, df_raw_lib = load_main_library()


    with open(ALLOWLIST_FILE, "r", encoding="utf-8") as f:
        allowed = json.load(f)["allowed_playlists"]

    print("➡️ Extracting whitelisted playlists...")
    df_playlists = extract_playlists(df_raw_lib, allowed)

    df_all = pd.concat([df_library_clean, df_playlists], ignore_index=True)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df_all.to_csv(OUTPUT_FILE, index=False)

    print(f"✅ Saved merged clean library → {OUTPUT_FILE}")
    print(f"📊 Total rows extracted: {len(df_all)}")


if __name__ == "__main__":
    extract_library_and_playlists()
