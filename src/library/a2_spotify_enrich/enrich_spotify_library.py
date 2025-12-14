import pandas as pd
from pathlib import Path
from datetime import datetime
from spotify_client import SpotifyClient

PROJECT_ROOT = Path(__file__).resolve().parents[3]

INPUT_FILE = PROJECT_ROOT / "data" / "interim" / "library_clean.csv"
OUTPUT_FILE = PROJECT_ROOT / "data" / "interim" / "spotify_enriched_library.csv"


def enrich_library_with_spotify():
    print(f"➡️ Loading library: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)

    client = SpotifyClient()
    results = []

    print(f"🎧 Starting Spotify enrichment for {len(df)} tracks...")

    for idx, row in df.iterrows():
        track_name = row["title"]
        artist_name = row["artist"]

        if pd.isna(track_name) or pd.isna(artist_name):
            continue

        # ---------- SEARCH TRACK ---------- #
        item = client.search_track(track_name, artist_name)

        if not item:
            print(f"⚠ No match found: {track_name} — {artist_name}")
            continue

        # Track metadata
        spotify_track_id = item["id"]
        duration_ms = item["duration_ms"]
        popularity = item["popularity"]
        explicit = item["explicit"]

        album = item["album"]
        album_id = album["id"]
        release_year = album.get("release_date", "")[:4]

        # Artist metadata
        artist_id = item["artists"][0]["id"]
        genres = client.get_artist_genres(artist_id)
        genres_joined = ", ".join(genres)

        # ---------- BUILD FINAL ROW ---------- #
        enriched_row = {
            "source_track_id": row["track_id"],
            "title_original": row["title"],
            "artist_original": row["artist"],
            "album_original": row["album"],
            "source": row["source"],

            "spotify_track_id": spotify_track_id,
            "spotify_artist_id": artist_id,
            "spotify_album_id": album_id,
            "release_year": release_year,
            "duration_ms": duration_ms,
            "duration_seconds": round(duration_ms / 1000, 2) if duration_ms else None,
            "popularity": popularity,
            "explicit": explicit,
            "genres": genres_joined,

            "extraction_date": datetime.utcnow().date().isoformat(),
        }

        results.append(enriched_row)

        # Log every 30 rows
        if idx % 30 == 0:
            print(f"   → {idx}/{len(df)} tracks processed")

    # ---------- SAVE OUTPUT ---------- #
    df_out = pd.DataFrame(results)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(OUTPUT_FILE, index=False)

    print(f"✅ Spotify enrichment complete → {OUTPUT_FILE}")
    print(f"📊 Total enriched tracks: {len(df_out)}")


if __name__ == "__main__":
    enrich_library_with_spotify()
