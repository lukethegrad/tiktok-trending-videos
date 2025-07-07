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
        "item_url": "video_url",               # ✅ Use actual URL from Lexis data
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

    if "musicMeta" not in df.columns or "webVideoUrl" not in df.columns:
        st.error("❌ Required fields 'musicMeta' or 'webVideoUrl' not found.")
        return pd.DataFrame()

    # Extract flattened columns
    df["Author"] = df["authorMeta"].apply(lambda x: x.get("name") if isinstance(x, dict) else None)
    df["Text"] = df["text"]
    df["Diggs"] = df["diggCount"]
    df["Shares"] = df["shareCount"]
    df["Plays"] = df["playCount"]
    df["Comments"] = df["commentCount"]
    df["Duration (seconds)"] = df["videoMeta"].apply(lambda x: x.get("duration") if isinstance(x, dict) else None)
    df["Music"] = df["musicMeta"].apply(lambda x: x.get("musicName") if isinstance(x, dict) else None)
    df["Music author"] = df["musicMeta"].apply(lambda x: x.get("musicAuthor") if isinstance(x, dict) else None)
    df["Music original?"] = df["musicMeta"].apply(lambda x: x.get("musicOriginal") if isinstance(x, dict) else None)
    df["Create Time"] = df["createTimeISO"]
    df["Video URL"] = df["webVideoUrl"]

    # Extract and normalize video_id
    import re
    df["video_id"] = df["Video URL"].apply(lambda url: re.search(r'/video/(\d+)', url).group(1) if isinstance(url, str) else None)
    df["video_url"] = df["video_id"].apply(lambda vid: f"https://www.tiktok.com/video/{vid}")

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
