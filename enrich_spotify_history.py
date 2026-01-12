import pandas as pd
from pathlib import Path
from datetime import datetime
from spotify_client import SpotifyClient

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

OUTPUT_FILE = (
    PROJECT_ROOT
    / "data"
    / "interim"
    / "history"
    / "spotify_enriched_history.csv"
)


def enrich_library_with_spotify():
    print(f"➡️ Loading library: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)


    client = SpotifyClient()

    # ============================================================
    # 🆕 SAFE SPEED BOOST — IN-MEMORY CACHES
    # ============================================================

    track_cache = {}   # (track_name, artist_name) -> spotify item
    genre_cache = {}   # spotify_artist_id -> genres

    results = []

    print(f"🎧 Starting Spotify enrichment for {len(df)} tracks...")

    # ============================================================
    # LOOP
    # ============================================================

    for idx, row in enumerate(df.itertuples(index=False), start=1):
        track_name = row.title
        artist_name = row.artist

        if pd.isna(track_name) or pd.isna(artist_name):
            continue

        # ---------- SEARCH TRACK (CACHED) ----------
        cache_key = (track_name, artist_name)

        if cache_key in track_cache:
            item = track_cache[cache_key]
        else:
            item = client.search_track(track_name, artist_name)
            track_cache[cache_key] = item

        if not item:
            print(f"⚠ No match found: {track_name} — {artist_name}")
            continue

        # ---------- TRACK METADATA ----------
        spotify_track_id = item.get("id")
        duration_ms = item.get("duration_ms")
        popularity = item.get("popularity")
        explicit = item.get("explicit")

        album = item.get("album") or {}
        spotify_album_id = album.get("id")
        release_year = (album.get("release_date") or "")[:4]

        # ---------- ARTIST METADATA (CACHED) ----------
        spotify_artist_id = item["artists"][0]["id"]

        if spotify_artist_id in genre_cache:
            genres = genre_cache[spotify_artist_id]
        else:
            genres = client.get_artist_genres(spotify_artist_id)
            genre_cache[spotify_artist_id] = genres

        # ---------- BUILD FINAL ROW ----------
        enriched_row = {
            "source_track_id": row.track_id,
            "title_original": row.title,
            "artist_original": row.artist,
            "album_original": row.album,
            "source": row.source,

            "spotify_track_id": spotify_track_id,
            "spotify_artist_id": spotify_artist_id,
            "spotify_album_id": spotify_album_id,
            "release_year": release_year,
            "duration_ms": duration_ms,
            "duration_seconds": round(duration_ms / 1000, 2) if duration_ms else None,
            "popularity": popularity,
            "explicit": explicit,
            "genres": ", ".join(genres) if genres else None,

            "extraction_date": datetime.utcnow().date().isoformat(),
        }

        results.append(enriched_row)

        # Log every 30 rows
        if idx % 30 == 0:
            print(f"   → {idx}/{len(df)} tracks processed")

    # ============================================================
    # SAVE OUTPUT
    # ============================================================

    df_out = pd.DataFrame(results)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(OUTPUT_FILE, index=False)

    print(f"✅ Spotify enrichment complete → {OUTPUT_FILE}")
    print(f"📊 Total enriched tracks: {len(df_out)}")
    print(f"🚀 Track cache size: {len(track_cache)}")
    print(f"🎼 Genre cache size: {len(genre_cache)}")


if __name__ == "__main__":
    enrich_library_with_spotify()
