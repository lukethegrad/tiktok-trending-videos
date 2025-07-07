import os
import pandas as pd
import streamlit as st
from typing import List
from apify_client import ApifyClient

# 🔐 Apify credentials
APIFY_API_KEY = os.getenv("APIFY_API_KEY")
SCRAPER_ACTOR = "lexis-solutions/tiktok-trending-videos-scraper"
ENRICHMENT_ACTOR = "clockworks/tiktok-video-scraper"  # SDK format uses `/`

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
    Uses clockworks/tiktok-video-scraper to enrich TikTok video URLs with sound metadata.
    """
    if not video_urls:
        st.warning("⚠️ No video URLs provided to enrich.")
        return pd.DataFrame()

    # ⚠️ Sanity check — Apify fails silently if any URLs are malformed
    valid_urls = [url for url in video_urls if url.startswith("https://www.tiktok.com/@")]
    if not valid_urls:
        st.error("❌ No valid TikTok @username/video links found. Aborting enrichment.")
        return pd.DataFrame()

    try:
        st.write("🎼 Starting Apify enrichment (clockworks actor)...")

        run_input = {
            "mode": "bulk",
            "videoUrls": valid_urls,
            "shouldDownloadVideos": False,
            "shouldDownloadCovers": False,
            "scrapeRelatedVideos": False,
            "shouldDownloadSubtitles": False,
            "shouldDownloadSlideshowImages": False,
            "resultsPerPage": len(valid_urls)  # ✅ tells actor how many to handle
        }

        st.json(run_input)  # Debug input payload

        import json
        st.code(json.dumps(run_input, indent=2))  # 💡 Shows exact payload
        st.write("Number of video URLs passed:", len(run_input["videoUrls"]))


        run = client.actor(ENRICHMENT_ACTOR).call(run_input=run_input)
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
