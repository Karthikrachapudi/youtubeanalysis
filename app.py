# Import necessary libraries
import streamlit as st
from googleapiclient.discovery import build
from sqlalchemy import create_engine, text
import pandas as pd
import pymysql
import re

# Streamlit UI setup
st.title("YouTube Data Harvesting and Warehousing")

# Enter YouTube Channel ID in the app
channel_id = st.text_input("Enter YouTube Channel ID")

# API Key
API_KEY = 'AIzaSyBLwQwdZ0PCteULZh31bkU4QYE3D3KR0RQ'  # Your provided API Key

# SQL database connection setup
db_host = "localhost"
db_user = "root"
db_password = "54321"  # Your provided MySQL password
db_name = "youtube_data"
db_port = 3306  # Default MySQL port

# SQLAlchemy engine setup
engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

# Function to get channel data from YouTube API
def get_channel_data(api_key, channel_id):
    youtube = build('youtube', 'v3', developerKey=api_key)
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    if 'items' in response and response['items']:
        uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        channel_data = {
            'channel_name': response['items'][0]['snippet']['title'],
            'channel_id': channel_id,
            'subscriber_count': int(response['items'][0]['statistics']['subscriberCount']),
            'video_count': int(response['items'][0]['statistics']['videoCount']),
            'uploads_playlist_id': uploads_playlist_id
        }
        st.write(f"Channel data fetched successfully for: {channel_id}")
        return channel_data
    else:
        st.write(f"No channel data found for: {channel_id}")
        return None

# Function to get video IDs from the uploads playlist
def get_video_ids(api_key, playlist_id):
    youtube = build('youtube', 'v3', developerKey=api_key)
    video_ids = []
    next_page_token = None
    while True:
        request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break
    return video_ids

# Function to get details for each video
def get_video_details(api_key, video_ids, channel_id):
    youtube = build('youtube', 'v3', developerKey=api_key)
    video_stats = []
    for i in range(0, len(video_ids), 50):
        response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i+50])
        ).execute()
        for video in response['items']:
            video_info = {
                'video_id': video['id'],
                'channel_id': channel_id,  # Explicitly add channel_id here
                'title': video['snippet']['title'],
                'description': video['snippet'].get('description', ''),
                'likes': int(video['statistics'].get('likeCount', 0)),
                'comments': int(video['statistics'].get('commentCount', 0)),
                'view_count': int(video['statistics'].get('viewCount', 0)),
                'duration': video['contentDetails'].get('duration', 'PT0M0S')
            }
            video_stats.append(video_info)
    return pd.DataFrame(video_stats)

# Function to convert ISO 8601 duration format to HH:MM:SS
def convert_duration(duration):
    regex = re.compile(r'PT(\d+H)?(\d+M)?(\d+S)?')
    matches = regex.match(duration)
    hours, minutes, seconds = 0, 0, 0
    if matches:
        hours = int(matches.group(1)[:-1]) if matches.group(1) else 0
        minutes = int(matches.group(2)[:-1]) if matches.group(2) else 0
        seconds = int(matches.group(3)[:-1]) if matches.group(3) else 0
    return f"{hours:02}:{minutes:02}:{seconds:02}"

# Function to check if channel_id already exists
def channel_exists(channel_id):
    query = text("SELECT EXISTS(SELECT 1 FROM channels WHERE channel_id = :channel_id)")
    with engine.connect() as connection:
        result = connection.execute(query, {'channel_id': channel_id}).fetchone()
    return result[0] == 1

# Button action: Fetch and store data
if st.button("Fetch and Store Data"):
    channel_data = get_channel_data(API_KEY, channel_id)
    if channel_data:
        # Check if channel already exists
        if not channel_exists(channel_id):
            # Insert channel data into database
            channel_df = pd.DataFrame([channel_data])
            with engine.connect() as connection:
                channel_df.to_sql('channels', connection, if_exists='append', index=False)
            st.write("Channel data stored successfully.")
            
            # Fetch video data
            video_ids = get_video_ids(API_KEY, channel_data['uploads_playlist_id'])
            video_details = get_video_details(API_KEY, video_ids, channel_id)
            video_details['duration'] = video_details['duration'].apply(convert_duration)
            
            # Store video data and explicitly check for 'channel_id'
            st.write("Video data with channel_id:", video_details[['video_id', 'channel_id']])  # Show channel_id before inserting
            with engine.connect() as connection:
                video_details.to_sql('videos', connection, if_exists='append', index=False)
            st.write("Video data stored successfully.")
        else:
            st.write("Channel data already exists in the database.")
    else:
        st.write("No data found for this channel.")

# Query to view data
if st.checkbox("Show Stored Video Data"):
    query = "SELECT * FROM videos"
    with engine.connect() as connection:
        results = pd.read_sql(query, connection)
        st.write(results)
