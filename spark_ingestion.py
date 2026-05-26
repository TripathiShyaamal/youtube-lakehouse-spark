import json
import urllib.request
import tempfile
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("YouTubeIngestion").getOrCreate()

cache_url = "https://raw.githubusercontent.com/TripathiShyaamal/youtube-lakehouse-spark/main/search_cache.json"
with urllib.request.urlopen(cache_url) as response:
    cache_data = json.loads(response.read().decode())

all_channels = []
all_videos = []
all_comments = []

for item in cache_data:
    ch = item.get("channel", {})
    all_channels.append({
        "channel_id": str(ch.get("channel_id", "")),
        "channel_name": str(ch.get("channel_name", "")),
        "total_views": int(ch.get("total_views") or 0),
        "total_subscribers": int(ch.get("total_subscribers") or 0),
        "total_videos": int(ch.get("total_videos") or 0),
        "fetched_at": str(ch.get("fetched_at", ""))
    })
    for v in item.get("videos", []):
        all_videos.append({
            "channel_id": str(v.get("channel_id", "")),
            "video_id": str(v.get("video_id", "")),
            "title": str(v.get("title", "")),
            "view_count": int(v.get("view_count") or 0),
            "like_count": int(v.get("like_count") or 0),
            "comment_count": int(v.get("comment_count") or 0)
        })

print("Channels:", len(all_channels), "Videos:", len(all_videos))

def write_and_read(spark, records):
    if not records:
        return None
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    for r in records:
        tmp.write(json.dumps(r) + "\n")
    tmp.close()
    return spark.read.json("file://" + tmp.name)

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
