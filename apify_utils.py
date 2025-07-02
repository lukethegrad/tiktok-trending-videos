import os
import pandas as pd
import streamlit as st
from typing import List
from apify_client import ApifyClient

# 🔐 Apify credentials
APIFY_API_KEY = os.getenv("APIFY_API_KEY")
SCRAPER_ACTOR = "lexis-solutions/tiktok-trending-videos-scraper"
ENRICHMENT_ACTOR = "delicious_zebu/tiktok-video-comment-scraper"  # SDK format uses `/`, not `~`

client = ApifyClient(APIFY_API_KEY)

def run_trending_scraper() -> pd.DataFrame:
    """
    Uses ApifyClient to fetch trending TikTok videos via SDK.
    """
    try:
        st.write("🎬 Starting Apify video scraper...")
        run = client.actor(SCRAPER_ACTOR).call()
        dataset_id = run["defaultDatasetId"]
        st.write(f"📁 Dataset ID: {dataset_id}")

        records = list(client.dataset(dataset_id).iterate_items())
        st.write(f"🎥 Number of videos fetched: {len(records)}")

        if not records:
            st.warning("⚠️ Apify returned an empty dataset.")
            return pd.DataFrame()

        return pd.DataFrame(records)

    except Exception as e:
        st.error("❌ Failed to run video scraper actor.")
        st.error(str(e))
        return pd.DataFrame()


def run_video_comment_scraper(video_urls: List[str]) -> pd.DataFrame:
    """
    Uses ApifyClient to enrich TikTok video URLs with comment and sound metadata.
    Returns a DataFrame with music and video metadata.
    """
    if not video_urls:
        st.warning("⚠️ No video URLs provided to enrich.")
        return pd.DataFrame()

    try:
        st.write("🎼 Starting Apify enrichment (video → sound metadata)...")
        run = client.actor(ENRICHMENT_ACTOR).call(run_input={"video_urls": video_urls})
        dataset_id = run["defaultDatasetId"]
        st.write(f"📁 Enrichment dataset ID: {dataset_id}")

        records = list(client.dataset(dataset_id).iterate_items())
        st.write(f"🎧 Enriched records received: {len(records)}")

        if not records:
            st.warning("⚠️ Enrichment returned an empty dataset.")
            return pd.DataFrame()

        return pd.DataFrame(records)

    except Exception as e:
        st.error("❌ Failed to run enrichment actor.")
        st.error(str(e))
        return pd.DataFrame()
