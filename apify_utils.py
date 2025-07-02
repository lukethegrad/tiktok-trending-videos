import os
import requests
import time
import pandas as pd
import streamlit as st
from typing import List

# 🔐 Apify credentials
APIFY_API_KEY = os.getenv("APIFY_API_KEY")
SCRAPER_ACTOR = "lexis-solutions/tiktok-trending-videos-scraper"
ENRICHMENT_ACTOR = "delicious_zebu~tiktok-video-comment-scraper"

def run_trending_scraper():
    """
    Triggers the Apify actor to fetch trending TikTok videos and returns results as a DataFrame.
    """
    run_url = f"https://api.apify.com/v2/acts/{SCRAPER_ACTOR}/runs?token={APIFY_API_KEY}"
    headers = {"Content-Type": "application/json"}
    payload = {}

    st.write("🎬 Starting Apify video scraper...")
    st.write("🔗 POSTing to:", run_url)
    response = requests.post(run_url, headers=headers, json=payload)
    st.write(f"📡 Apify POST status: {response.status_code}")

    if response.status_code != 201:
        st.error("❌ Failed to start Apify actor.")
        st.error(response.text)
        return None

    run_id = response.json()["data"]["id"]
    st.write(f"✅ Apify run started: {run_id}")

    # ⏱️ Poll for completion
    status_url = f"https://api.apify.com/v2/actor-runs/{run_id}"
    for _ in range(30):
        time.sleep(5)
        status_res = requests.get(status_url)
        status_data = status_res.json()["data"]
        status = status_data["status"]
        st.write(f"⏳ Apify status: {status}")
        if status == "SUCCEEDED":
            break
    else:
        st.error("❌ Apify run did not finish successfully.")
        return None

    dataset_id = status_data.get("defaultDatasetId")
    st.write(f"📁 Using dataset ID: {dataset_id}")
    if not dataset_id:
        st.error("❌ No dataset ID found.")
        return None

    dataset_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?clean=true"
    data_res = requests.get(dataset_url)
    st.write(f"📥 Dataset fetch status: {data_res.status_code}")

    if data_res.status_code != 200:
        st.error(f"❌ Failed to fetch dataset: {data_res.status_code}")
        return None

    records = data_res.json()
    st.write(f"🎥 Number of videos fetched: {len(records)}")

    if not records:
        st.warning("⚠️ Apify returned an empty dataset.")
        return None

    df = pd.DataFrame(records)
    return df


def run_video_comment_scraper(video_urls: List[str]) -> pd.DataFrame:
    """
    Uses the Apify actor to enrich TikTok video URLs with comment and sound metadata.
    Returns a DataFrame with music and video metadata.
    """
    if not video_urls:
        st.warning("⚠️ No video URLs provided to enrich.")
        return pd.DataFrame()

    run_url = f"https://api.apify.com/v2/acts/{ENRICHMENT_ACTOR}/runs?token={APIFY_API_KEY}"
    headers = {"Content-Type": "application/json"}
    payload = {"video_urls": video_urls}

    st.write("🎼 Starting Apify enrichment (video -> sound metadata)...")
    st.write("🔗 POSTing to:", run_url)
    response = requests.post(run_url, headers=headers, json=payload)
    st.write(f"📡 Apify POST status: {response.status_code}")

    if response.status_code != 201:
        st.error("❌ Failed to start enrichment actor.")
        st.error(response.text)
        return pd.DataFrame()

    run_id = response.json()["data"]["id"]
    st.write(f"✅ Enrichment run started: {run_id}")

    # ⏱️ Poll for completion
    status_url = f"https://api.apify.com/v2/actor-runs/{run_id}"
    for _ in range(30):
        time.sleep(5)
        status_res = requests.get(status_url)
        status_data = status_res.json()["data"]
        status = status_data["status"]
        st.write(f"⏳ Apify enrichment status: {status}")
        if status == "SUCCEEDED":
            break
    else:
        st.error("❌ Enrichment run did not finish successfully.")
        return pd.DataFrame()

    dataset_id = status_data.get("defaultDatasetId")
    st.write(f"📁 Using enrichment dataset ID: {dataset_id}")
    if not dataset_id:
        st.error("❌ No dataset ID found for enrichment.")
        return pd.DataFrame()

    dataset_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?clean=true"
    data_res = requests.get(dataset_url)
    st.write(f"📥 Enrichment dataset fetch status: {data_res.status_code}")

    if data_res.status_code != 200:
        st.error(f"❌ Failed to fetch enrichment dataset: {data_res.status_code}")
        return pd.DataFrame()

    records = data_res.json()
    st.write(f"🎧 Enriched videos with metadata: {len(records)}")

    if not records:
        st.warning("⚠️ Apify enrichment returned an empty dataset.")
        return pd.DataFrame()

    return pd.DataFrame(records)
