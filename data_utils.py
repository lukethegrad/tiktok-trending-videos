import pandas as pd
import streamlit as st

def process_raw_data(df: pd.DataFrame) -> pd.DataFrame:
    # Print the original columns for debug
    st.write("Raw DataFrame columns:", list(df.columns))

    # Rename and extract needed fields
    if not all(col in df.columns for col in ["title", "author", "song_id"]):
        st.error("Required columns not found in the dataset.")
        return pd.DataFrame()

    df = df[["title", "author", "song_id"]].copy()
    df.rename(columns={
        "title": "Title",
        "author": "Artist",
        "song_id": "Sound ID"
    }, inplace=True)

    # Construct TikTok URL
    df["TikTok Sound URL"] = df["Sound ID"].apply(
        lambda sid: f"https://www.tiktok.com/music/original-sound-{sid}" if pd.notna(sid) else None
    )

    df.dropna(subset=["Title", "Sound ID"], inplace=True)
    df.drop_duplicates(subset=["Title", "Artist", "Sound ID"], inplace=True)

    return df.reset_index(drop=True)
