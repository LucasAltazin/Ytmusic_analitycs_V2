import requests
import time
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SECRETS_FILE = PROJECT_ROOT / "secrets" / "spotify_credentials.json"


class SpotifyClient:
    """
    Wrapper Spotify API (Client Credentials Flow)
    - Handles token generation
    - Handles rate limits (429)
    - Provides track search + artist lookup
    - Provides caching for artist metadata
    """

    def __init__(self):
        self._load_credentials()
        self.token = self._generate_token()
        self.artist_cache = {}  # prevent 800 calls for same artist

    def _load_credentials(self):
        with open(SECRETS_FILE, "r") as f:
            creds = json.load(f)
            self.client_id = creds["client_id"]
            self.client_secret = creds["client_secret"]

    def _generate_token(self):
        """Get access token using client_credentials flow."""
        url = "https://accounts.spotify.com/api/token"
        resp = requests.post(url, {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret
        })

        if resp.status_code != 200:
            raise Exception(f"Failed to authenticate Spotify API: {resp.text}")

        return resp.json()["access_token"]

    def _request(self, url, params=None):
        """Generic GET with rate-limit handling."""
        headers = {"Authorization": f"Bearer {self.token}"}

        r = requests.get(url, headers=headers, params=params)

        # Token expired → retry
        if r.status_code == 401:
            self.token = self._generate_token()
            return self._request(url, params)

        # Rate limit → wait and retry
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 2))
            print(f"⏳ Rate limited — waiting {wait}s...")
            time.sleep(wait)
            return self._request(url, params)

        if r.status_code != 200:
            print(f"⚠ Spotify API Error: {r.status_code} - {r.text}")
            return None

        return r.json()

    # ---------------- TRACK SEARCH ---------------- #
    def search_track(self, track_name, artist_name):
        """Search for a track with track + artist."""
        query = f"track:{track_name} artist:{artist_name}"
        url = "https://api.spotify.com/v1/search"
        params = {
            "q": query,
            "type": "track",
            "limit": 5
        }

        data = self._request(url, params)
        if data is None or not data["tracks"]["items"]:
            # Fallback: search only by track title
            fallback = self._fallback_search(track_name)
            return fallback

        return data["tracks"]["items"][0]  # best match

    def _fallback_search(self, track_name):
        """Second chance search: track only."""
        print(f"🔎 Fallback search: {track_name}")
        url = "https://api.spotify.com/v1/search"
        params = {
            "q": f"track:{track_name}",
            "type": "track",
            "limit": 5
        }

        data = self._request(url, params)
        if data is None or not data["tracks"]["items"]:
            return None

        return data["tracks"]["items"][0]

    # ---------------- ARTIST METADATA (GENRES) ---------------- #
    def get_artist_genres(self, artist_id):
        """Fetch artist genres (cached to avoid redundant API calls)."""
        if artist_id in self.artist_cache:
            return self.artist_cache[artist_id]

        url = f"https://api.spotify.com/v1/artists/{artist_id}"
        data = self._request(url)

        if data is None:
            self.artist_cache[artist_id] = []
            return []

        genres = data.get("genres", [])
        self.artist_cache[artist_id] = genres
        return genres
