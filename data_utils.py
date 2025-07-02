import pandas as pd
import streamlit as st

def process_raw_data(df: pd.DataFrame) -> pd.DataFrame:
    # Print the original columns for debug
    st.write("Raw DataFrame columns:", list(df.columns))

    # Check required fields for TikTok video data
    required_cols = ["title", "item_url", "duration", "cover", "region", "item_id"]
    if not all(col in df.columns for col in required_cols):
        st.error("❌ Required video columns not found in the dataset.")
        return pd.DataFrame()

    # Select and rename relevant fields
    df = df[required_cols].copy()
    df.rename(columns={
        "title": "caption",
        "item_url": "video_url",
        "duration": "duration_seconds",
        "cover": "thumbnail_url",
        "region": "region",
        "item_id": "video_id"
    }, inplace=True)

    # Drop rows with missing critical info
    df.dropna(subset=["caption", "video_url", "video_id"], inplace=True)

    # Drop duplicates based on content and ID
    df.drop_duplicates(subset=["caption", "video_url", "video_id"], inplace=True)

    return df.reset_index(drop=True)
