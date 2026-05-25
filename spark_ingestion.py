import json
import os
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder \
    .appName("youtube-ingestion") \
    .getOrCreate()

print("Spark started successfully")

cache_data = os.environ.get("CACHE_DATA", "")

if not cache_data:
    print("No cache data provided")
    spark.stop()
    exit(0)

cache = json.loads(cache_data)
print(f"Loaded {len(cache)} entries from cache")

spark.sql("CREATE DATABASE IF NOT EXISTS youtube_tracker")

all_channels = []
all_videos = []
all_comments = []

for entry in cache:
    all_channels.append(entry["channel"])
    all_videos.extend(entry["videos"])
    all_comments.extend(entry["comments"])

if all_channels:
    channel_df = spark.createDataFrame(all_channels)
    channel_df.write.mode("append").saveAsTable("youtube_tracker.channel_history")
    print(f"Channels written: {len(all_channels)}")

if all_videos:
    videos_df = spark.createDataFrame(all_videos)
    videos_df.write.mode("append").saveAsTable("youtube_tracker.video_history")
    print(f"Videos written: {len(all_videos)}")

if all_comments:
    comments_df = spark.createDataFrame(all_comments)
    comments_df.write.mode("append").saveAsTable("youtube_tracker.comment_history")
    print(f"Comments written: {len(all_comments)}")

print("Ingestion complete")
spark.stop()