import pandas as pd
import streamlit as st
import re

# From Lexis Solutions trending scraper
def process_raw_data(df: pd.DataFrame) -> pd.DataFrame:
    st.write("Raw DataFrame columns:", list(df.columns))

    required_cols = ["item_url", "title", "id", "cover", "country_code", "duration"]
    if not all(col in df.columns for col in required_cols):
        st.error("❌ Required video columns not found in the dataset.")
        return pd.DataFrame()

    df = df[required_cols].copy()
    df.rename(columns={
        "item_url": "video_url",               # consistent with Clockworks
        "title": "caption",
        "id": "video_id",
        "cover": "thumbnail_url",
        "country_code": "region",
        "duration": "duration_seconds"
    }, inplace=True)

    df.dropna(subset=["video_url", "caption", "video_id"], inplace=True)
    df.drop_duplicates(subset=["video_url", "caption", "video_id"], inplace=True)

    return df.reset_index(drop=True)


# From Clockworks video metadata scraper
def process_enriched_video_data(df: pd.DataFrame) -> pd.DataFrame:
    st.write("Enriched DataFrame columns:", list(df.columns))

    # Ensure required nested fields exist
    if "musicMeta" not in df.columns or "webVideoUrl" not in df.columns:
        st.error("❌ Required fields 'musicMeta' or 'webVideoUrl' not found.")
        return pd.DataFrame()

    # Extract nested fields from musicMeta
    df["Song Title"] = df["musicMeta"].apply(lambda x: x.get("musicName") if isinstance(x, dict) else None)
    df["Artist"] = df["musicMeta"].apply(lambda x: x.get("musicAuthor") if isinstance(x, dict) else None)
    df["TikTok Sound URL"] = None  # Optional: we don't get this from Clockworks

    # Rename for consistency
    df.rename(columns={"webVideoUrl": "video_url"}, inplace=True)

    # Drop incomplete and duplicate rows
    df.dropna(subset=["Song Title", "Artist", "video_url"], inplace=True)
    df.drop_duplicates(subset=["Song Title", "Artist", "video_url"], inplace=True)

    return df.reset_index(drop=True)



# Filter music only (exclude "original sound" etc.)
def filter_music_only(df: pd.DataFrame) -> pd.DataFrame:
    st.write("🔍 Filtering non-music sounds...")
    if "Song Title" not in df.columns:
        st.warning("⚠️ 'Song Title' column missing — cannot filter music.")
        return df

    music_df = df[~df["Song Title"].str.lower().str.startswith("original sound")]
    music_df = music_df[music_df["Song Title"].str.strip() != ""]

    st.write(f"🎼 Music-based videos remaining: {len(music_df)}")
    return music_df.reset_index(drop=True)


# Merge enriched metadata with raw video data
def merge_video_and_song_data(video_df: pd.DataFrame, enriched_df: pd.DataFrame) -> pd.DataFrame:
    st.write("Merging video data with enriched sound metadata...")
    if "video_url" not in video_df.columns or "video_url" not in enriched_df.columns:
        st.error("❌ 'video_url' must be present in both DataFrames.")
        return pd.DataFrame()

    merged_df = pd.merge(video_df, enriched_df, on="video_url", how="inner")
    st.write(f"✅ Merged {len(merged_df)} records.")
    return merged_df
