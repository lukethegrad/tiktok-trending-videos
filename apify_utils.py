import os
import json
import requests
import pandas as pd
import streamlit as st
from typing import List
from apify_client import ApifyClient

# 🔐 Apify credentials
APIFY_API_KEY = os.getenv("APIFY_API_KEY")
SCRAPER_ACTOR = "lexis-solutions/tiktok-trending-videos-scraper"
ENRICHMENT_ACTOR = "clockworks~tiktok-video-scraper"  # HTTP API format uses `~`

client = ApifyClient(APIFY_API_KEY)

def run_trending_scraper(country_code="United Kingdom", sort_by="hot", period_type="last 7 days", max_items=10) -> pd.DataFrame:
    """
    Triggers the Apify actor to fetch trending TikTok videos using user-defined parameters.
    """
    try:
        country_map = {
            "United Kingdom": "GB",
            "United States": "US",
            "France": "FR",
            "Germany": "DE"
        }
        period_map = {
            "last 7 days": "7",
            "last 30 days": "30"
        }

        country_code_resolved = country_map.get(country_code, country_code)
        period_resolved = period_map.get(period_type, period_type)

        input_payload = {
            "countryCode": country_code_resolved,
            "sort": sort_by,
            "period": period_resolved,
            "maxItems": max_items
        }

        st.write("🎬 Starting Apify trending video scrape with parameters:")
        st.json(input_payload)

        run = client.actor(SCRAPER_ACTOR).call(run_input=input_payload)
        dataset_items = list(client.dataset(run["defaultDatasetId"]).iterate_items())

        if not dataset_items:
            st.warning("⚠️ Apify returned an empty dataset.")
            return pd.DataFrame()

        df = pd.DataFrame(dataset_items)
        st.write(f"🎥 Number of videos fetched: {len(df)}")
        return df

    except Exception as e:
        st.error("❌ Failed to run video scraper actor.")
        st.error(str(e))
        return pd.DataFrame()


def run_video_comment_scraper(video_urls: List[str]) -> pd.DataFrame:
    """
    Uses the HTTP API to enrich TikTok video URLs with sound metadata.
    """
    if not video_urls:
        st.warning("⚠️ No video URLs provided to enrich.")
        return pd.DataFrame()

    valid_urls = [url for url in video_urls if url.startswith("https://www.tiktok.com/@")]
    if not valid_urls:
        st.error("❌ No valid TikTok @username/video links found. Aborting enrichment.")
        return pd.DataFrame()

    try:
        st.write("🎼 Starting Apify enrichment (HTTP API)...")

        run_input = {
            "mode": "bulk",
            "videoUrls": valid_urls,
            "postURLs": [],
            "shouldDownloadVideos": False,
            "shouldDownloadCovers": False,
            "scrapeRelatedVideos": False,
            "shouldDownloadSubtitles": False,
            "shouldDownloadSlideshowImages": False
        }

        # Debug view
        st.json(run_input)
        st.code(json.dumps(run_input, indent=2))
        st.write("Number of video URLs passed:", len(valid_urls))

        # Trigger HTTP run
        response = requests.post(
            f"https://api.apify.com/v2/acts/{ENRICHMENT_ACTOR}/runs?token={APIFY_API_KEY}&wait=1",
            json=run_input,
            timeout=300
        )

        if response.status_code != 201:
            st.error("❌ HTTP request to Apify failed.")
            st.error(response.text)
            return pd.DataFrame()

        run_data = response.json()
        dataset_id = run_data.get("data", {}).get("defaultDatasetId")
        st.write(f"📁 Enrichment dataset ID: {dataset_id}")

        if not dataset_id:
            st.warning("⚠️ No dataset ID returned.")
            return pd.DataFrame()

        dataset_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?format=json"
        records_response = requests.get(dataset_url, timeout=60)

        if records_response.status_code != 200:
            st.error("❌ Failed to fetch dataset records.")
            st.error(records_response.text)
            return pd.DataFrame()

        records = records_response.json()
        st.write(f"🎧 Enriched records received: {len(records)}")

        return pd.DataFrame(records)

    except Exception as e:
        st.error("❌ Failed to run enrichment actor via HTTP API.")
        st.error(str(e))
        return pd.DataFrame()
