# spotify_scraper.py

import requests
import base64
import os
import pandas as pd
import time
import streamlit as st

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

    query = f"{song_title} {artist_name}".strip()
    search_url = "https://api.spotify.com/v1/search"
    params = {"q": query, "type": "track", "limit": 1}

    try:
        search_res = requests.get(search_url, headers=headers, params=params)
        search_res.raise_for_status()
        items = search_res.json().get("tracks", {}).get("items", [])

        if not items:
            return {
                "Spotify Track": None,
                "Spotify Artist": None,
                "Album": None,
                "Label": "Unknown"
            }

        track = items[0]
        album = track.get("album", {})
        album_id = album.get("id")

        label = "Unknown"
        album_name = album.get("name")

        if album_id:
            album_url = f"https://api.spotify.com/v1/albums/{album_id}"
            album_res = requests.get(album_url, headers=headers)
            if album_res.ok:
                album_data = album_res.json()
                label = album_data.get("label", "Unknown")

        return {
            "Spotify Track": track["name"],
            "Spotify Artist": ", ".join([a["name"] for a in track["artists"]]),
            "Album": album_name,
            "Label": label
        }

    except Exception as e:
        st.warning(f"⚠️ Spotify lookup failed for: {query} — {str(e)}")
        return {
            "Spotify Track": None,
            "Spotify Artist": None,
            "Album": None,
            "Label": "Unknown"
        }


def enrich_with_spotify(df: pd.DataFrame) -> pd.DataFrame:
    """
    Accepts a DataFrame with 'Song Title' and 'Artist' columns and adds Spotify enrichment.
    """
    token = get_access_token()

    results = []
    for i, row in df.iterrows():
        title = row.get("Song Title", "")
        artist = row.get("Artist", "")
        st.write(f"🔍 Searching Spotify for: {title} – {artist}")
        result = get_spotify_label(title, artist, token)
        results.append(result)
        time.sleep(0.2)  # to stay under rate limits

    enrichment_df = pd.DataFrame(results)
    return pd.concat([df.reset_index(drop=True), enrichment_df.reset_index(drop=True)], axis=1)
