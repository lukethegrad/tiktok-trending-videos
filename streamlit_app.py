import streamlit as st
import pandas as pd
import os
import time

from apify_utils import run_trending_scraper
from data_utils import process_raw_data
from metadata_utils import enrich_with_spotify_metadata
from label_filter import filter_unsigned_tracks

st.set_page_config(page_title="TikTok Trending Discovery", layout="wide")

st.title("🎵 TikTok Trending Discovery Tool")
st.markdown("This tool pulls the top 100 trending TikTok sounds via Apify.")

# Helper to enrich each song with Spotify metadata
def enrich_with_metadata(df):
    enriched_rows = []
    for _, row in df.iterrows():
        meta = enrich_with_spotify_metadata(row["Title"], row["Artist"])
        if not meta:
            meta = {
                "Spotify Title": None,
                "Spotify Artist": None,
                "Album": None,
                "Spotify Label": "Lookup Failed"
            }
        enriched_rows.append({**row, **meta})
        time.sleep(0.5)
    return pd.DataFrame(enriched_rows)

# Main button logic
if st.button("Fetch Trending Songs"):
    with st.spinner("Fetching data from Apify..."):
        df = run_trending_scraper()

        # ✅ Token check AFTER fetching, shown securely
        token_loaded = "APIFY_API_TOKEN" in st.secrets
        st.info(f"🔐 Apify Token Loaded: {'✅ YES' if token_loaded else '❌ NO'}")

        if df is not None and not df.empty:
            clean_df = process_raw_data(df)
            st.success(f"✅ Fetched {len(clean_df)} clean songs.")

            with st.spinner("Enriching with Spotify metadata..."):
                enriched_df = enrich_with_metadata(clean_df)

            st.subheader("🎧 All Enriched Songs")
            st.dataframe(enriched_df)

            with st.spinner("Filtering signed tracks..."):
                unsigned_df = filter_unsigned_tracks(enriched_df)
                st.success(f"🆓 {len(unsigned_df)} unsigned or unknown-label songs found.")

                st.subheader("🆓 Unsigned or Unknown-Label Songs")
                st.dataframe(unsigned_df)

            # Optional downloads
            # csv = unsigned_df.to_csv(index=False).encode("utf-8")
            # st.download_button("Download CSV", csv, "unsigned_songs.csv", "text/csv")

        else:
            st.error("⚠️ No data was returned from Apify.")
