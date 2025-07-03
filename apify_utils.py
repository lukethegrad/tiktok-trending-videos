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

def run_trending_scraper(country_code="United Kingdom", sort_by="hot", period_type="last 7 days", max_items=10):
    """
    Triggers the Apify actor to fetch trending TikTok videos using user-defined parameters.
    """
    from apify_client import ApifyClient  # Make sure this is installed and added to requirements.txt

    client = ApifyClient(os.getenv("APIFY_API_KEY"))

    # Map human-friendly country to Apify expected code if needed
    # You can expand this dictionary if needed
    country_map = {
        "United Kingdom": "GB",
        "United States": "US",
        "France": "FR",
        "Germany": "DE"
    }
    country_code_resolved = country_map.get(country_code, country_code)

    input_payload = {
        "countryCode": country_code_resolved,
        "sort": sort_by,
        "period": period_type,
        "maxItems": max_items
    }

    st.write("🎬 Starting Apify trending video scrape with parameters:")
    st.json(input_payload)

    run = client.actor("lexis-solutions/tiktok-trending-videos-scraper").call(run_input=input_payload)
    dataset_items = list(client.dataset(run["defaultDatasetId"]).iterate_items())

    if not dataset_items:
        st.warning("⚠️ Apify returned an empty dataset.")
        return None

    df = pd.DataFrame(dataset_items)
    st.write(f"🎥 Number of videos fetched: {len(df)}")
    return df


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
