import streamlit as st
import pandas as pd
import time

from apify_utils import run_trending_scraper, run_video_comment_scraper
from data_utils import (
    process_raw_data,
    process_enriched_video_data,
    merge_video_and_song_data,
    filter_music_only
)
from spotify_scraper import enrich_with_spotify
from label_filter import filter_unsigned_tracks

st.set_page_config(page_title="TikTok Trending Discovery", layout="wide")

st.title("🎵 TikTok Trending Discovery Tool")
st.markdown("This tool pulls the top trending TikTok **videos**, extracts the **songs used**, enriches them via **Spotify**, and filters for **unsigned tracks**.")

# Sidebar: Scraper Settings
st.sidebar.header("📊 Scraper Settings")

country = st.sidebar.selectbox("🌍 Country", ["United Kingdom", "United States", "France", "Germany"])
sort_by = st.sidebar.selectbox("🔥 Sort By", ["hot", "likes", "comments", "shares"])
period = st.sidebar.selectbox("🕒 Period Type", ["last 7 days", "last 30 days"])
max_items = st.sidebar.slider("🔢 Max Items", min_value=5, max_value=100, value=10, step=5)

# Step 1 – Scrape trending TikTok videos
if st.button("1⃣ Fetch Trending Videos"):
    with st.spinner("Fetching trending TikTok videos..."):
        raw_df = run_trending_scraper(
            country_code=country,
            sort_by=sort_by,
            period_type=period,
            max_items=max_items
        )

    if raw_df is None or raw_df.empty:
        st.error("❌ No data returned from Apify.")
    else:
        video_df = process_raw_data(raw_df)
        st.session_state["video_df"] = video_df
        st.success(f"✅ Loaded {len(video_df)} trending videos.")

        # Table – All trending videos
        st.subheader("🎥 Trending Videos")
        st.dataframe(video_df)

# Step 2 – Enrich with video sound metadata and filter for music
if "video_df" in st.session_state and st.button("2⃣ Enrich Sound Metadata"):
    video_urls = st.session_state["video_df"]["video_url"].tolist()
    with st.spinner("Enriching with sound metadata via Apify..."):
        enriched_df = run_video_comment_scraper(video_urls)

    if enriched_df is None or enriched_df.empty:
        st.error("❌ Enrichment failed or returned no data.")
    else:
        clean_enriched_df = process_enriched_video_data(enriched_df)
        st.session_state["enriched_df"] = clean_enriched_df

        st.success(f"✅ Enriched {len(clean_enriched_df)} videos.")

        # Show all enriched videos (flattened columns)
        columns_to_show = [
            "Author", "Text", "Diggs", "Shares", "Plays", "Comments",
            "Duration (seconds)", "Music", "Music author"
        ]
        st.subheader("🎬 All Enriched Videos")
        st.dataframe(clean_enriched_df[columns_to_show])

        # Filter music-only subset
        music_df = filter_music_only(clean_enriched_df)
        st.session_state["music_df"] = music_df

        st.success(f"✅ Filtered {len(music_df)} music-based videos.")
        st.subheader("🎶 Videos with Music")
        st.dataframe(music_df[columns_to_show])

# Step 3 – Enrich with Spotify metadata (only music videos)
if "music_df" in st.session_state and st.button("3⃣ Enrich with Spotify"):
    with st.spinner("Querying Spotify..."):
        spotify_input_df = st.session_state["music_df"].rename(
            columns={
                "Music": "Song Title",
                "Music author": "Artist"
            }
        )
        spotify_df = enrich_with_spotify(spotify_input_df)

        # Rename back for display purposes
        display_df = spotify_df.rename(
            columns={
                "Song Title": "Music",
                "Artist": "Music author"
            }
        )

        st.session_state["spotify_df"] = display_df
        st.success("✅ Spotify enrichment complete.")

        display_cols = [
            "Music", "Music author", "Label", 
            "diggCount", "shareCount", "playCount", "commentCount"
        ]
        try:
            spotify_display_df = display_df[display_cols]
            st.subheader("🎧 Enriched Songs with Labels")
            st.dataframe(spotify_display_df)
        except KeyError as e:
            st.warning(f"⚠️ Could not find expected display columns. {e}")
            st.dataframe(display_df)


# Step 4 – Filter unsigned songs
if "spotify_df" in st.session_state and st.button("4⃣ Show Unsigned Songs"):
    with st.spinner("Filtering for unsigned or unknown-label songs..."):
        unsigned_df = filter_unsigned_tracks(st.session_state["spotify_df"])
        st.session_state["unsigned_df"] = unsigned_df

        st.success(f"🇳🇴 Found {len(unsigned_df)} unsigned or unknown-label songs.")
        unsigned_display_df = unsigned_df[display_cols]  # reuse the same column filter
        st.subheader("🆓 Unsigned or Unknown-Label Songs")
        st.dataframe(unsigned_display_df)
        
        csv = unsigned_display_df.to_csv(index=False).encode("utf-8")


        csv = unsigned_df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download Unsigned Songs CSV", csv, "unsigned_tiktok_songs.csv", "text/csv")
