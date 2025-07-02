import requests
import streamlit as st


SPOTIFY_API_URL = "https://spotify-label-api.fly.dev/spotify_label"



def enrich_with_spotify_metadata(title: str, artist: str) -> dict:
    """Query your deployed Spotify label scraper."""
    # Get secrets from environment variables (injected by Streamlit)
    SPOTIFY_CLIENT_ID = st.secrets["SPOTIFY_CLIENT_ID"]
    SPOTIFY_CLIENT_SECRET = st.secrets["SPOTIFY_CLIENT_SECRET"]
    
    params = {"song": title, "artist": artist}

    try:
        res = requests.get(SPOTIFY_API_URL, params=params, timeout=15)
        res.raise_for_status()
        data = res.json()

        return {
            "Spotify Title": data.get("track"),
            "Spotify Artist": data.get("artist"),
            "Album": data.get("album"),
            "Spotify Label": data.get("label"),
        }
    except Exception as e:
        print("Spotify label lookup failed:", e)
        return {
            "Spotify Title": title,
            "Spotify Artist": artist,
            "Album": None,
            "Spotify Label": None,
        }
