import json
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from googleapiclient.discovery import build

# Configuration
API_KEY = "AIzaSyC1KEwf4B5ZFboZWM8ZhRndPkZ1TjC-tNc"
CHANNELS = ["MrBeast", "Fireship", "Veritasium"]

spark = SparkSession.builder \
    .appName("youtube-ingestion") \
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS youtube_tracker")

youtube = build("youtube", "v3", developerKey=API_KEY)

def get_channel_id(channel_name):
    response = youtube.search().list(
        q=channel_name,
        type="channel",
        part="id,snippet",
        maxResults=1
    ).execute()
    if not response["items"]:
        return None
    return response["items"][0]["id"]["channelId"]

def get_channel_stats(channel_id):
    response = youtube.channels().list(
        part="statistics,snippet",
        id=channel_id
    ).execute()
    channel = response["items"][0]
    stats = channel["statistics"]
    return {
        "channel_id": channel_id,
        "channel_name": channel["snippet"]["title"],
        "total_views": int(stats.get("viewCount", 0)),
        "total_subscribers": int(stats.get("subscriberCount", 0)),
        "total_videos": int(stats.get("videoCount", 0)),
        "fetched_at": datetime.utcnow().isoformat()
    }

def get_videos(channel_id):
    response = youtube.search().list(
        channelId=channel_id,
        part="id",
        type="video",
        maxResults=10
    ).execute()
    video_ids = [item["id"]["videoId"] for item in response["items"]]

    response = youtube.videos().list(
        part="statistics,snippet",
        id=",".join(video_ids)
    ).execute()

    videos = []
    for item in response["items"]:
        s = item["statistics"]
        view_count = int(s.get("viewCount", 0))
        like_count = int(s.get("likeCount", 0))
        engagement = round((like_count / view_count) * 100, 2) if view_count > 0 else 0
        videos.append({
            "channel_id": channel_id,
            "video_id": item["id"],
            "title": item["snippet"]["title"],
            "view_count": view_count,
            "like_count": like_count,
            "comment_count": int(s.get("commentCount", 0)),
            "engagement_rate_percent": engagement,
            "fetched_at": datetime.utcnow().isoformat()
        })
    return videos

all_channels = []
all_videos = []

for channel_name in CHANNELS:
    print(f"Processing: {channel_name}")
    channel_id = get_channel_id(channel_name)
    if not channel_id:
        print(f"Channel not found: {channel_name}")
        continue

    channel_stats = get_channel_stats(channel_id)
    videos = get_videos(channel_id)

    all_channels.append(channel_stats)
    all_videos.extend(videos)
    print(f"Done: {channel_name}")

if all_channels:
    channel_df = spark.createDataFrame(all_channels)
    channel_df.write.mode("append").saveAsTable("youtube_tracker.channel_history")
    print(f"Channels written: {len(all_channels)}")

if all_videos:
    videos_df = spark.createDataFrame(all_videos)
    videos_df.write.mode("append").saveAsTable("youtube_tracker.video_history")
    print(f"Videos written: {len(all_videos)}")

print("Ingestion complete")
spark.stop()