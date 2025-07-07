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
    Uses clockworks/tiktok-video-scraper via HTTP to enrich TikTok video URLs with sound metadata.
    Waits dynamically until dataset is populated or timeout is reached.
    """
    import requests
    import json
    import time

    if not video_urls:
        st.warning("⚠️ No video URLs provided to enrich.")
        return pd.DataFrame()

    valid_urls = [url for url in video_urls if url.startswith("https://www.tiktok.com/@")]
    if not valid_urls:
        st.error("❌ No valid TikTok @username/video links found. Aborting enrichment.")
        return pd.DataFrame()

    try:
        st.write("🎼 Starting Apify enrichment (clockworks actor)...")

        post_urls = valid_urls  # Required key format

        run_input = {
            "postURLs": post_urls,
            "mode": "bulk",
            "shouldDownloadVideos": False,
            "shouldDownloadCovers": False,
            "scrapeRelatedVideos": False,
            "shouldDownloadSubtitles": False,
            "shouldDownloadSlideshowImages": False,
            "resultsPerPage": len(post_urls)
        }

        st.json(run_input)
        st.code(json.dumps(run_input, indent=2))
        st.write("Number of video URLs passed:", len(post_urls))

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {APIFY_API_KEY}"
        }

        # Run Apify actor (async wait for completion)
        response = requests.post(
            f"https://api.apify.com/v2/acts/{ENRICHMENT_ACTOR}/runs?wait=1",
            json=run_input,
            headers=headers
        )
        response.raise_for_status()
        run_data = response.json()
        dataset_id = run_data["data"]["defaultDatasetId"]
        st.write(f"📁 Enrichment dataset ID: {dataset_id}")

                # ⏳ Poll dataset until it's populated with valid music data or timeout hits
        dataset_items_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?format=json"
        timeout = 1000  # seconds (5 min max)
        poll_interval = 10
        elapsed = 0
        first_poll_delay = 60

        time.sleep(first_poll_delay)  # Give Apify time to populate dataset
        st.write("🔄 Polling Apify for dataset readiness...")

        while elapsed < timeout:
            items_response = requests.get(dataset_items_url, headers=headers)
            if items_response.status_code == 200:
                records = items_response.json()
                if any("musicMeta" in r and r["musicMeta"] for r in records):
                    st.success(f"🎧 Enriched records received: {len(records)}")
                    return pd.DataFrame(records)

            time.sleep(poll_interval)
            elapsed += poll_interval
            st.write(f"⏳ Waited {elapsed}s... still waiting for dataset with valid music metadata...")


        st.write("🔄 Polling Apify for dataset readiness...")

        while elapsed < timeout:
            items_response = requests.get(dataset_items_url, headers=headers)
            if items_response.status_code == 200:
                records = items_response.json()
                if records:
                    st.success(f"🎧 Enriched records received: {len(records)}")
                    return pd.DataFrame(records)

            time.sleep(poll_interval)
            elapsed += poll_interval
            st.write(f"⏳ Waited {elapsed}s... still waiting for dataset...")

        st.warning("⚠️ Enrichment timed out after 5 minutes without receiving any data.")
        return pd.DataFrame()

    except Exception as e:
        st.error("❌ Failed to run enrichment actor.")
        st.error(str(e))
        return pd.DataFrame()

