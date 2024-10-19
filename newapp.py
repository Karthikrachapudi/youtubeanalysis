import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px

# Database connection details
db_host = "localhost"
db_user = "root"
db_password = "54321"  # Update with your password
db_name = "youtube_data"
db_port = 3306  # Default MySQL port

# Create database connection
engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

st.title("YouTube Data Analysis")

# Query 1: Show all channels from the database
st.header("All YouTube Channels")
query = "SELECT * FROM channels"
channels_df = pd.read_sql(query, engine)
st.dataframe(channels_df)

# Query 2: Total videos per channel
st.header("Total Videos Per Channel")
query_videos = """
    SELECT channel_id, COUNT(video_id) as total_videos
    FROM videos
    GROUP BY channel_id
"""
videos_df = pd.read_sql(query_videos, engine)
st.dataframe(videos_df)

# Visualization: Top 5 Channels by Video Count
st.header("Top 5 Channels by Video Count")
top_channels_df = videos_df.sort_values(by="total_videos", ascending=False).head(5)
fig = px.bar(top_channels_df, x="channel_id", y="total_videos", title="Top 5 Channels by Video Count")
st.plotly_chart(fig)

# Query 3: Top Videos by Views
st.header("Top 10 Videos by Views")
query_views = """
    SELECT title, view_count
    FROM videos
    ORDER BY view_count DESC
    LIMIT 10
"""
top_videos_df = pd.read_sql(query_views, engine)
st.dataframe(top_videos_df)

# Visualization: Top 10 Videos by Views
st.header("Top 10 Most Viewed Videos")
fig_views = px.bar(top_videos_df, x="title", y="view_count", title="Top 10 Most Viewed Videos", labels={"view_count": "Views"})
st.plotly_chart(fig_views)
