import streamlit as st
from googleapiclient.discovery import build
from pymongo import MongoClient
import mysql.connector
import pandas as pd

# Set your API key here
api_key = 'AIzaSyATYzpzcSvmy9X-61PdYa6CBunJV0ZVkbI'

# Create YouTube API client
youtube = build('youtube', 'v3', developerKey=api_key)


# Function to get channel details
def get_channel_details(channel_id):
    try:
        request = youtube.channels().list(
            part='snippet,contentDetails,statistics',
            id=channel_id
        )
        response = request.execute()
        data = dict(
            channel_name=response['items'][0]['snippet']['title'],
            subscriber_count=response['items'][0]['statistics']['subscriberCount'],
            views=response['items'][0]['statistics']['viewCount'],
            total_videos=response['items'][0]['statistics']['videoCount'],
            playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
            country=response['items'][0]['snippet'].get('country', 'Not specified')
        )
        return data
    except Exception as e:
        st.error(f"Error fetching channel details: {e}")
        return None

# Function to get video IDs from a playlist
def get_video_ids_from_playlist(playlist_id):
    try:
        video_ids = []
        next_page_token = None

        while True:
            request = youtube.playlistItems().list(
                part='contentDetails',
                maxResults=50,  # Adjust as needed, maximum is 50
                playlistId=playlist_id,
                pageToken=next_page_token
            )

            response = request.execute()

            for item in response['items']:
                video_ids.append(item['contentDetails']['videoId'])

            next_page_token = response.get('nextPageToken')

            if not next_page_token:
                break

        return video_ids
    except Exception as e:
        st.error(f"Error fetching video IDs from the playlist: {e}")
        return None

# Function to get video details
def get_video_details(video_id):
    try:
        request = youtube.videos().list(
            part='snippet,statistics',
            id=video_id
        )
        response = request.execute()
        video_data = dict(
            video_id=video_id,
            title=response['items'][0]['snippet']['title'],
            author=response['items'][0]['snippet']['channelTitle'],
            published=response['items'][0]['snippet']['publishedAt'],
            views=response['items'][0]['statistics']['viewCount'],
            likes=response['items'][0]['statistics']['likeCount'],
            comments=response['items'][0]['statistics']['commentCount']
        )
        return video_data
    except Exception as e:
        st.error(f"Error fetching video details: {e}")
        return None

# Function to get video comments
def get_video_comments(video_id):
    try:
        comments = []
        next_page_token = None

        while True:
            request = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=100,  # Adjust as needed, maximum is 100
                textFormat='plainText',
                pageToken=next_page_token
            )

            response = request.execute()

            for item in response['items']:
                comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
                comments.append(comment)

            next_page_token = response.get('nextPageToken')

            if not next_page_token:
                break

        return comments
    except Exception as e:
        st.error(f"Error fetching comments: {e}")
        return None
    
page = st.sidebar.selectbox("Select a page:", ["Channel Data", "Export to Mongo DB", "Transform to MySQL"])

if page=="Channel Data":
    st.title("YouTube Channel Data and Comments")

# Input for YouTube channel ID
    channel_id = st.text_input("Enter YouTube Channel ID:")

    if st.button("Get Channel and Comments"):
        if channel_id:
            # Get channel details
            channel_data = get_channel_details(channel_id)

            if channel_data:
                st.write("Channel Details:")
                st.write(f"Channel Name: {channel_data['channel_name']}")
                st.write(f"subscriber_count: {channel_data['subscriber_count']}")
                st.write(f"Views: {channel_data['views']}")
                st.write(f"Total Videos: {channel_data['total_videos']}")
                st.write(f"Playlist ID: {channel_data['playlist_id']}")
                st.write(f"Country: {channel_data['country']}")

                # Get video IDs from the playlist
                playlist_id = channel_data['playlist_id']
                video_ids = get_video_ids_from_playlist(playlist_id)

                if video_ids:
                    # Get video details for the first video
                    first_video_id = video_ids[0]
                    video_data = get_video_details(first_video_id)

                    if video_data:
                        st.write("Video Details:")
                        st.write(f"Video ID: {video_data['video_id']}")
                        st.write(f"Title: {video_data['title']}")
                        st.write(f"Author: {video_data['author']}")
                        st.write(f"Published: {video_data['published']}")
                        st.write(f"Views: {video_data['views']}")
                        st.write(f"Likes: {video_data['likes']}")
                        st.write(f"Comments: {video_data['comments']}")

                        # Get comments for the first video
                        comments = get_video_comments(first_video_id)

                        if comments:
                            st.write("Comments for the video:")
                            for comment in comments:
                                st.write(comment)
                        else:
                            st.warning("No comments found for the video.")
                    else:
                        st.warning("Failed to retrieve video details.")
                else:
                    st.warning("No video IDs found in the playlist.")
            else:
                st.warning("Failed to retrieve channel details.")
                           
            client = MongoClient("mongodb+srv://aravinthr:sologod@cluster0.p5abf2r.mongodb.net/?retryWrites=true&w=majority")
            db = client.get_database('Channel_stats')
            records = db.Channel_stats

            # Insert data into MongoDB
            insert_result = records.insert_one(channel_data)

            if insert_result.inserted_id:
                st.success("Data inserted to Mongo DB successfully. Document ID: {}".format(insert_result.inserted_id))
            else:
                st.error("Data insertion failed.")

            # Count the documents in the MongoDB collection
            num_documents = records.count_documents({})
            st.write("Number of documents in 'Channel_stats' collection:", num_documents)


            mysql_client = mysql.connector.connect(
            host="localhost",
            user="root",
            password="123456",
            database="project")

    # Create a cursor
            cursor = mysql_client.cursor()

            # Create a table to store the data
            create_table_query = """
            CREATE TABLE channel_stats1 (
                channel_name VARCHAR(255),
                subscriber_count INT,
                views BIGINT,
                total_videos INT,
                playlist_id VARCHAR(255),
                country VARCHAR(255)
            )
            """
            cursor.execute(create_table_query)

            # Iterate through the data from MongoDB and insert it into the MySQL table
            data_from_mongo = list(records.find())

            for document in data_from_mongo:
                insert_query = """
                INSERT INTO channel_stats1 (channel_name, subscriber_count, views, total_videos, playlist_id, country)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                values = (
                    document['channel_name'],
                    document['subscriber_count'],
                    document['views'],
                    document['total_videos'],
                    document['playlist_id'],
                    document['country']
                )
                cursor.execute(insert_query, values)

            # Commit the changes to the MySQL database
            mysql_client.commit()

            # Close the MySQL connection and cursor
            cursor.close()
            mysql_client.close()
# MongoDB operations

            
            client = MongoClient("mongodb+srv://aravinthr:sologod@cluster0.p5abf2r.mongodb.net/?retryWrites=true&w=majority")
            db = client.get_database('Video_stats')
            records = db.Video_stats

            # Insert data into MongoDB
            insert_result = records.insert_one(video_data)

            if insert_result.inserted_id:
                st.success("Video data inserted to MongoDB successfully. Document ID: {}".format(insert_result.inserted_id))
            else:
                st.error("Video data insertion into MongoDB failed.")
            
            # Count the documents in the MongoDB collection
            num_documents = records.count_documents({})
            st.write("Number of documents in 'Video_stats' collection:", num_documents)

            mysql_client = mysql.connector.connect(
            host="localhost",
            user="root",
            password="123456",
            database="project"
        )

        # Create a cursor
            cursor = mysql_client.cursor()

            # Create a table to store video details if not exists
            create_table_query = """
            CREATE TABLE video_stats1 (
                video_id VARCHAR(255),
                title VARCHAR(255),
                author VARCHAR(255),
                views BIGINT,
                likes INT,
                comments INT
            )
            """
            cursor.execute(create_table_query)

            # Insert video details into MySQL
            insert_query = """
            INSERT INTO video_stats1 (video_id, title, author, views, likes, comments)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            values = (
                video_data['video_id'],
                video_data['title'],
                video_data['author'],
                video_data['views'],
                video_data['likes'],
                video_data['comments']
            )
            cursor.execute(insert_query, values)

            # Commit the changes to the MySQL database
            mysql_client.commit()

            st.success("Video data inserted to MySQL successfully.")
        



