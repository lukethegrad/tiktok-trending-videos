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
ENRICHMENT_ACTOR = "clockworks~tiktok-video-scraper"  # HTTP API format uses ~

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
    Uses Apify HTTP API to run the clockworks/tiktok-video-scraper actor with bulk TikTok video URLs.
    """
    if not video_urls:
        st.warning("⚠️ No video URLs provided to enrich.")
        return pd.DataFrame()

    # ✅ Filter only valid TikTok URLs
    valid_urls = [url for url in video_urls if url.startswith("https://www.tiktok.com/@")]
    if not valid_urls:
        st.error("❌ No valid TikTok @username/video links found. Aborting enrichment.")
        return pd.DataFrame()

    try:
        st.write("🎼 Starting Apify enrichment (clockworks actor via HTTP API)...")

        # Prepare payload
        start_urls = [{"url": url} for url in valid_urls]
        run_input = {
            "mode": "bulk",
            "startUrls": start_urls,
            "shouldDownloadVideos": False,
            "shouldDownloadCovers": False,
            "scrapeRelatedVideos": False,
            "shouldDownloadSubtitles": False,
            "shouldDownloadSlideshowImages": False,
            "resultsPerPage": len(start_urls)
        }

        st.json(run_input)
        st.code(json.dumps(run_input, indent=2))
        st.write("Number of video URLs passed:", len(start_urls))

        # Step 1: Trigger Apify run
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {APIFY_API_KEY}"
        }

        trigger_url = f"https://api.apify.com/v2/acts/{ENRICHMENT_ACTOR}/runs?wait=1"
        response = requests.post(trigger_url, headers=headers, json=run_input)
        response.raise_for_status()
        run_data = response.json()
        dataset_id = run_data["data"]["defaultDatasetId"]
        st.write(f"📁 Enrichment dataset ID: {dataset_id}")

        # Step 2: Download dataset results
        dataset_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?format=json"
        items_response = requests.get(dataset_url, headers=headers)
        items_response.raise_for_status()
        records = items_response.json()
        st.write(f"🎧 Enriched records received: {len(records)}")

        if not records:
            st.warning("⚠️ Enrichment returned an empty dataset.")
            return pd.DataFrame()

        return pd.DataFrame(records)

    except Exception as e:
        st.error("❌ Failed to run enrichment actor.")
        st.error(str(e))
        return pd.DataFrame()
