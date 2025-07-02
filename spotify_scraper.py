# spotify_scraper.py

import requests
import base64
import os
import pandas as pd
import time

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

def get_spotify_label(song_title: str, artist_name: str, token: str) -> dict:
    headers = {
        "Authorization": f"Bearer {token}"
    }

    query = f"{song_title} {artist_name}"
    search_url = "https://api.spotify.com/v1/search"
    params = {"q": query, "type": "track", "limit": 1}

    try:
        search_res = requests.get(search_url, headers=headers, params=params)
        search_res.raise_for_status()
        items = search_res.json().get("tracks", {}).get("items", [])
        if not items:
            return {"Spotify Track": None, "Spotify Artist": None, "Album": None, "Label": "Unknown"}

        track = items[0]
        album_id = track["album"]["id"]

        album_url = f"https://api.spotify.com/v1/albums/{album_id}"
        album_res = requests.get(album_url, headers=headers)
        album_res.raise_for_status()
        album_data = album_res.json()

        return {
            "Spotify Track": track["name"],
            "Spotify Artist": ", ".join([a["name"] for a in track["artists"]]),
            "Album": album_data["name"],
            "Label": album_data.get("label", "Unknown")
        }

    except Exception:
        return {"Spotify Track": None, "Spotify Artist": None, "Album": None, "Label": "Unknown"}


def enrich_with_spotify(df: pd.DataFrame) -> pd.DataFrame:
    """
    Accepts a DataFrame with 'Song Title' and 'Artist' columns and adds Spotify enrichment.
    """
    token = get_access_token()

    results = []
    for _, row in df.iterrows():
        title = row.get("Song Title", "")
        artist = row.get("Artist", "")
        result = get_spotify_label(title, artist, token)
        results.append(result)
        time.sleep(0.2)  # avoid hitting rate limits

    enrichment_df = pd.DataFrame(results)
    return pd.concat([df.reset_index(drop=True), enrichment_df.reset_index(drop=True)], axis=1)
