
# spotify_scraper.py

import requests
import base64
import os

# Set these via environment variables or secrets in production
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

def get_access_token() -> str:
    auth_str = f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}"
    b64_auth_str = base64.b64encode(auth_str.encode()).decode()

    headers = {
        "Authorization": f"Basic {b64_auth_str}",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = {
        "grant_type": "client_credentials"
    }

    response = requests.post("https://accounts.spotify.com/api/token", headers=headers, data=data)
    response.raise_for_status()
    return response.json()["access_token"]

def get_spotify_label(song_title: str, artist_name: str) -> dict:
    token = get_access_token()

    headers = {
        "Authorization": f"Bearer {token}"
    }

    # Search for the track
    query = f"{song_title} {artist_name}"
    search_url = f"https://api.spotify.com/v1/search"
    params = {"q": query, "type": "track", "limit": 1}

    search_res = requests.get(search_url, headers=headers, params=params)
    search_res.raise_for_status()
    items = search_res.json().get("tracks", {}).get("items", [])

    if not items:
        return {"error": "Track not found"}

    track = items[0]
    album_id = track["album"]["id"]

    # Get album info
    album_url = f"https://api.spotify.com/v1/albums/{album_id}"
    album_res = requests.get(album_url, headers=headers)
    album_res.raise_for_status()
    album_data = album_res.json()

    return {
        "track": track["name"],
        "artist": ", ".join([a["name"] for a in track["artists"]]),
        "album": album_data["name"],
        "label": album_data.get("label", "Unknown")
    }
