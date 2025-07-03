import pandas as pd
import streamlit as st
import re

def process_raw_data(df: pd.DataFrame) -> pd.DataFrame:
    st.write("Raw DataFrame columns:", list(df.columns))

    required_cols = ["item_url", "music_title", "music_author", "music_url"]
    if not all(col in df.columns for col in required_cols):
        st.error("❌ Required video columns not found in the dataset.")
        return pd.DataFrame()

    df = df[required_cols].copy()
    df.rename(columns={
        "item_url": "video_url",
        "music_title": "Song Title",
        "music_author": "Artist",
        "music_url": "TikTok Sound URL"
    }, inplace=True)

    # Drop rows with missing critical info
    df.dropna(subset=["Song Title", "Artist", "TikTok Sound URL", "video_url"], inplace=True)

    # Drop duplicates
    df.drop_duplicates(subset=["Song Title", "Artist", "TikTok Sound URL", "video_url"], inplace=True)

    return df.reset_index(drop=True)

def process_enriched_video_data(df: pd.DataFrame) -> pd.DataFrame:
    st.write("Enriched DataFrame columns:", list(df.columns))

    required_cols = ["video_url", "music_title", "music_author", "music_url"]
    if not all(col in df.columns for col in required_cols):
        st.error("❌ Required music metadata columns not found.")
        return pd.DataFrame()

    df = df[required_cols].copy()
    df.rename(columns={
        "music_title": "Song Title",
        "music_author": "Artist",
        "music_url": "TikTok Sound URL"
    }, inplace=True)

    # Extract sound_id from TikTok Sound URL
    def extract_sound_id(url):
        if isinstance(url, str):
            match = re.search(r"-([0-9]+)$", url)
            return match.group(1) if match else None
        return None

    df["Sound ID"] = df["TikTok Sound URL"].apply(extract_sound_id)

    # Clean and deduplicate
    df.dropna(subset=["Song Title", "Artist", "Sound ID"], inplace=True)
    df.drop_duplicates(subset=["Song Title", "Artist", "Sound ID"], inplace=True)

    return df.reset_index(drop=True)

def filter_music_only(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter out non-music sounds (e.g. 'original sound' or empty strings).
    """
    st.write("🔍 Filtering non-music sounds...")
    if "Song Title" not in df.columns:
        st.warning("⚠️ 'Song Title' column missing — cannot filter music.")
        return df

    # Filter out typical UGC or sound effects
    music_df = df[~df["Song Title"].str.lower().str.startswith("original sound")]
    music_df = music_df[music_df["Song Title"].str.strip() != ""]

    st.write(f"🎼 Music-based videos remaining: {len(music_df)}")
    return music_df.reset_index(drop=True)

def merge_video_and_song_data(video_df: pd.DataFrame, enriched_df: pd.DataFrame) -> pd.DataFrame:
    st.write("Merging video data with enriched sound metadata...")
    if "video_url" not in video_df.columns or "video_url" not in enriched_df.columns:
        st.error("❌ 'video_url' must be present in both DataFrames.")
        return pd.DataFrame()

    merged_df = pd.merge(video_df, enriched_df, on="video_url", how="inner")
    st.write(f"✅ Merged {len(merged_df)} records.")
    return merged_df
