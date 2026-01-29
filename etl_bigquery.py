import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
from googleapiclient.discovery import build
import os
import json
from datetime import datetime

# --- CONFIGURATION ---
# List of Channel IDs to Track (Add more here!)
CHANNEL_IDS = [
    "UCX6OQ3DkcsbYNE6H8uQQuVA", # MrBeast
    "UCq-Fj5jknLsUf-MWSy4_brA", # T-Series
    "UCbCmjCuTUZos6Inko4u57UQ", # Cocomelon
    "UCpEhnqL0y41EpW2TvWAHD7Q", # SET India
    "UC-lHJZR3Gqxm24_Vd_AJ5Yw", # PewDiePie
    "UCJ5v_MCY6GNUBTO8-D3XoAg", # WWE
]

TABLE_ID = "youtube_analytics.top_channels_stats" 
PROJECT_ID = json.loads(os.environ["GCP_SA_KEY"])["project_id"]

# --- STEP 1: AUTHENTICATION ---
print("🔑 Authenticating...")
service_account_info = json.loads(os.environ["GCP_SA_KEY"])
bq_credentials = service_account.Credentials.from_service_account_info(service_account_info)
youtube_api_key = os.environ["YOUTUBE_API_KEY"]
youtube = build('youtube', 'v3', developerKey=youtube_api_key)

# --- STEP 2: EXTRACT (Batch Processing) ---
all_data = [] # We will store all results in this list

print(f"📡 Fetching data for {len(CHANNEL_IDS)} channels...")

# We loop through each ID in our list
for channel_id in CHANNEL_IDS:
    try:
        request = youtube.channels().list(
            part="snippet,statistics",
            id=channel_id
        )
        response = request.execute()
        
        if "items" in response:
            item = response["items"][0]
            stats = {
                "date": datetime.now().strftime("%Y-%m-%d"),
                "channel_name": item["snippet"]["title"],
                "subscribers": int(item["statistics"]["subscriberCount"]),
                "total_views": int(item["statistics"]["viewCount"]),
                "video_count": int(item["statistics"]["videoCount"])
            }
            all_data.append(stats)
            print(f"   ✅ Fetched: {stats['channel_name']}")
    except Exception as e:
        print(f"   ❌ Error fetching {channel_id}: {e}")

# --- STEP 3: LOAD (Push Batch to BigQuery) ---
if all_data:
    df = pd.DataFrame(all_data)
    
    try:
        print(f"🚀 Uploading {len(df)} rows to BigQuery...")
        pandas_gbq.to_gbq(
            df,
            TABLE_ID,
            project_id=PROJECT_ID,
            if_exists="append",
            credentials=bq_credentials
        )
        print("✅ Success! Batch upload complete.")
    except Exception as e:
        print(f"❌ BigQuery Error: {e}")
        exit(1)
else:
    print("⚠️ No data collected.")
