import json
import urllib.request
import tempfile
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("YouTubeIngestion").getOrCreate()

cache_url = "https://raw.githubusercontent.com/TripathiShyaamal/youtube-lakehouse-spark/main/search_cache.json"
with urllib.request.urlopen(cache_url) as response:
    cache_data = json.loads(response.read().decode())

# Print structure for debugging
print("cache_data type:", type(cache_data))
if isinstance(cache_data, list):
    print("First item keys:", cache_data[0].keys() if cache_data else "empty")
else:
    print("Top-level keys:", list(cache_data.keys())[:3])

all_channels = []
all_videos = []

# Handle both list and dict formats
items = cache_data.items() if isinstance(cache_data, dict) else [(item.get("channel_name", str(i)), item) for i, item in enumerate(cache_data)]

for channel_name, data in items:
    all_channels.append({
        "channel_name": str(channel_name),
        "subscribers": int(data.get("subscribers") or 0),
        "views": int(data.get("views") or 0),
        "video_count": int(data.get("video_count") or 0),
        "fetched_at": str(data.get("fetched_at", ""))
    })
    for v in data.get("videos", []):
        all_videos.append({
            "channel_name": str(channel_name),
            "video_id": str(v.get("id", "")),
            "title": str(v.get("title", "")),
            "views": int(v.get("views") or 0),
            "likes": int(v.get("likes") or 0),
            "comments": int(v.get("comments") or 0)
        })

print("Channels found:", len(all_channels))
print("Videos found:", len(all_videos))

def write_and_read(spark, records):
    if not records:
        return None
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    for r in records:
        tmp.write(json.dumps(r) + "\n")
    tmp.close()
    df = spark.read.json("file://" + tmp.name)
    os.unlink(tmp.name)
    return df

spark.sql("CREATE DATABASE IF NOT EXISTS youtube_tracker")

channel_df = write_and_read(spark, all_channels)
if channel_df:
    channel_df.writeTo("youtube_tracker.channel_history").createOrReplace()
    print("channel_history written:", channel_df.count())

video_df = write_and_read(spark, all_videos)
if video_df:
    video_df.writeTo("youtube_tracker.video_history").createOrReplace()
    print("video_history written:", video_df.count())

print("Done.")
spark.stop()
