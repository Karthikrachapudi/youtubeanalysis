import streamlit as st
from sqlalchemy import create_engine
import pandas as pd
import pymysql
from googleapiclient.discovery import build  # YouTube API client
from datetime import datetime
import json

# Database connection setup
db_host = "localhost"
db_user = "root"
db_password = "54321"
db_name = "youtube_data"
db_port = 3306

# Establish connection
engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

# YouTube API setup
api_key = 'AIzaSyBLwQwdZ0PCteULZh31bkU4QYE3D3KR0RQ'
youtube = build('youtube', 'v3', developerKey=api_key)

# Function to format YouTube date strings to 'YYYY-MM-DD' format
def format_date(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d')
    except ValueError:
        return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d')

# Function to collect data using YouTube API
def collect_youtube_data(channel_id):
    request = youtube.channels().list(part="snippet,statistics,contentDetails", id=channel_id)
    response = request.execute()

    if response["items"]:
        channel_info = response["items"][0]
        # Extract channel details
        channel_name = channel_info["snippet"]["title"]
        subscriber_count = channel_info["statistics"]["subscriberCount"]
        video_count = channel_info["statistics"]["videoCount"]
        view_count = channel_info["statistics"]["viewCount"]
        channel_description = channel_info["snippet"]["description"]
        country = channel_info["snippet"].get("country", "Not Available")
        created_at = format_date(channel_info["snippet"]["publishedAt"])
        published_at = format_date(channel_info["snippet"]["publishedAt"])
        thumbnails = channel_info["snippet"]["thumbnails"]["default"]["url"]
        uploads_playlist_id = channel_info["contentDetails"]["relatedPlaylists"]["uploads"]

        # Collect video data from the uploads playlist
        videos = []
        next_page_token = None
        while True:
            playlist_request = youtube.playlistItems().list(
                playlistId=uploads_playlist_id,
                part="snippet,contentDetails",
                maxResults=50,
                pageToken=next_page_token
            )
            playlist_response = playlist_request.execute()

            for item in playlist_response["items"]:
                video_id = item["contentDetails"]["videoId"]
                title = item["snippet"]["title"]
                description = item["snippet"]["description"]
                published_at = format_date(item["contentDetails"]["videoPublishedAt"])
                
                video_details = youtube.videos().list(
                    part="statistics,contentDetails,snippet",
                    id=video_id
                ).execute()
                
                # Fetching additional video details
                video_info = video_details["items"][0]
                view_count = video_info["statistics"].get("viewCount", 0)
                like_count = video_info["statistics"].get("likeCount", 0)
                dislike_count = video_info["statistics"].get("dislikeCount", 0)
                comment_count = video_info["statistics"].get("commentCount", 0)
                duration = video_info["contentDetails"]["duration"]
                tags = video_info["snippet"].get("tags", [])
                category = video_info["snippet"].get("categoryId", "")
                live_broadcast_content = video_info["snippet"].get("liveBroadcastContent", "")
                default_language = video_info["snippet"].get("defaultAudioLanguage", "")
                region_restrictions = video_info["contentDetails"].get("regionRestriction", {})

                # Convert tags and region_restrictions to JSON strings
                tags_json = json.dumps(tags)
                region_restrictions_json = json.dumps(region_restrictions)

                videos.append({
                    'video_id': video_id,
                    'channel_id': channel_id,
                    'title': title,
                    'description': description,
                    'published_at': published_at,
                    'view_count': view_count,
                    'like_count': like_count,
                    'dislike_count': dislike_count,
                    'comment_count': comment_count,
                    'duration': duration,
                    'tags': tags_json,
                    'category': category,
                    'thumbnails': video_info["snippet"]["thumbnails"]["default"]["url"],
                    'live_broadcast_content': live_broadcast_content,
                    'default_language': default_language,
                    'region_restrictions': region_restrictions_json
                })

            next_page_token = playlist_response.get("nextPageToken")
            if not next_page_token:
                break

        # Create dataframes for both channels and videos
        channel_data = {
            'channel_id': [channel_id],
            'channel_name': [channel_name],
            'subscriber_count': [subscriber_count],
            'video_count': [video_count],
            'view_count': [view_count],
            'channel_description': [channel_description],
            'country': [country],
            'created_at': [created_at],
            'published_at': [published_at],
            'thumbnails': [thumbnails]
        }

        videos_df = pd.DataFrame(videos)
        channel_df = pd.DataFrame(channel_data)

        return channel_df, videos_df
    else:
        st.error("No channel found for this ID.")
        return None, None

# Initialize session state for selected menu and channel data
if 'menu' not in st.session_state:
    st.session_state['menu'] = 'Home'

if 'collected_data' not in st.session_state:
    st.session_state['collected_data'] = None
    st.session_state['videos_data'] = None

# Main menu layout using buttons
st.sidebar.markdown("### Main Menu")
if st.sidebar.button("Home"):
    st.session_state['menu'] = 'Home'

if st.sidebar.button("Data Zone"):
    st.session_state['menu'] = 'Data Zone'

if st.sidebar.button("Analysis Zone"):
    st.session_state['menu'] = 'Analysis Zone'

if st.sidebar.button("Query Zone"):
    st.session_state['menu'] = 'Query Zone'

# Home Menu
if st.session_state['menu'] == 'Home':
    st.markdown("## Welcome to the YouTube Data Harvesting App")
    st.markdown("""
    This app allows you to collect YouTube data and migrate it to your MySQL database. Use the navigation buttons to switch between the data collection and migration sections.
    """)

# Data Zone
if st.session_state['menu'] == 'Data Zone':
    st.markdown("## COLLECT & MIGRATE")

    tabs = st.tabs(["Data Collection Zone", "Data Migration Zone"])

    with tabs[0]:
        st.markdown("### Data Collection Zone")
        channel_id = st.text_input("Enter the channel_id", "")
        if st.button("Retrieve and store data"):
            if channel_id:
                channel_df, videos_df = collect_youtube_data(channel_id)
                if channel_df is not None:
                    st.session_state['collected_data'] = channel_df
                    st.session_state['videos_data'] = videos_df
                    st.success(f"Data successfully collected for channel: {channel_id}")
            else:
                st.error("Please enter a valid channel ID.")

    with tabs[1]:
        st.markdown("### Data Migration Zone")
        if st.button("Migrate to MySQL"):
            if st.session_state['collected_data'] is None or st.session_state['videos_data'] is None:
                st.error("No data to migrate. Please collect data first.")
            else:
                channel_df = st.session_state['collected_data']
                channel_df.to_sql('channels', con=engine, if_exists='append', index=False)
                videos_df = st.session_state['videos_data']
                videos_df.to_sql('videos', con=engine, if_exists='append', index=False)
                st.success("Data successfully migrated to MySQL database.")

# Analysis Zone
if st.session_state['menu'] == 'Analysis Zone':
    st.markdown("## Channel Data Analysis Zone")

    query = "SELECT channel_name FROM channels"
    channels_df = pd.read_sql(query, engine)

    if st.checkbox("Check available channel data for analysis"):
        if not channels_df.empty:
            st.write("### Available channel data")
            channels_df.insert(0, 'S.No', range(1, 1 + len(channels_df)))
            st.markdown(channels_df.to_html(index=False), unsafe_allow_html=True)
        else:
            st.write("No channel data available for analysis.")

# Query Zone
if st.session_state['menu'] == 'Query Zone':
    st.markdown("## Query Zone")

    # Dropdown for predefined queries
    query_options = {
        "What are the names of all the videos and their corresponding channels?": 
            "SELECT v.title AS video_name, c.channel_name FROM videos v JOIN channels c ON v.channel_id = c.channel_id",
        "Which channels have the most number of videos, and how many videos do they have?":
            "SELECT channel_name, video_count FROM channels ORDER BY video_count DESC",
        "What are the top 10 most viewed videos and their respective channels?":
            "SELECT v.title AS video_name, c.channel_name, v.view_count FROM videos v JOIN channels c ON v.channel_id = c.channel_id ORDER BY v.view_count DESC LIMIT 10",
        "How many comments were made on each video, and what are their corresponding video names?":
            "SELECT v.title AS video_name, v.comment_count FROM videos v",
        "Which videos have the highest number of likes, and what are their corresponding channel names?":
            "SELECT v.title AS video_name, c.channel_name, v.like_count FROM videos v JOIN channels c ON v.channel_id = c.channel_id ORDER BY v.like_count DESC",
        "What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
            "SELECT v.title AS video_name, (v.like_count + v.dislike_count) AS total_likes_dislikes FROM videos v",
        "What is the total number of views for each channel, and what are their corresponding channel names?":
            "SELECT channel_name, view_count FROM channels",
        "What are the names of all the channels that have published videos in the year 2022?":
            "SELECT DISTINCT c.channel_name FROM channels c JOIN videos v ON c.channel_id = v.channel_id WHERE YEAR(v.published_at) = 2022",
        "What is the average duration of all videos in each channel, and what are their corresponding channel names?":
            "SELECT c.channel_name, AVG(v.duration) AS avg_duration FROM videos v JOIN channels c ON v.channel_id = c.channel_id GROUP BY c.channel_name",
        "Which videos have the highest number of comments, and what are their corresponding channel names?":
            "SELECT v.title AS video_name, c.channel_name, v.comment_count FROM videos v JOIN channels c ON v.channel_id = c.channel_id ORDER BY v.comment_count DESC"
    }

    # Dropdown to select a query
    query_selection = st.selectbox("Choose a query:", list(query_options.keys()))

    if st.button("Run Query"):
        query = query_options[query_selection]
        try:
            # Execute the selected query and display the result
            query_result = pd.read_sql(query, engine)
            if query_result.empty:
                st.warning("No data returned from the query.")
            else:
                st.write(f"### Results for: {query_selection}")
                st.dataframe(query_result)
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")