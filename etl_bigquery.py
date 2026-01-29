import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
from googleapiclient.discovery import build
import os
import json
from datetime import datetime

# --- CONFIGURATION ---
# T-Series Channel ID
CHANNEL_ID = "UCq-Fj5jknLsUf-MWSy4_brA" 
TABLE_ID = "youtube_analytics.tseries_stats" 
# This automatically gets your Project ID from the JSON key
PROJECT_ID = json.loads(os.environ["GCP_SA_KEY"])["project_id"]

# --- STEP 1: AUTHENTICATION ---
print("🔑 Authenticating...")
service_account_info = json.loads(os.environ["GCP_SA_KEY"])
bq_credentials = service_account.Credentials.from_service_account_info(service_account_info)
youtube_api_key = os.environ["YOUTUBE_API_KEY"]

# --- STEP 2: EXTRACT ---
def get_channel_stats(api_key, channel_id):
    youtube = build('youtube', 'v3', developerKey=api_key)
    request = youtube.channels().list(part="snippet,statistics", id=channel_id)
    response = request.execute()

    if "items" in response:
        item = response["items"][0]
        return {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "channel_name": item["snippet"]["title"],
            "subscribers": int(item["statistics"]["subscriberCount"]),
            "total_views": int(item["statistics"]["viewCount"]),
            "video_count": int(item["statistics"]["videoCount"])
        }
    else:
        return None

print(f"📡 Fetching stats for T-Series...")
data = get_channel_stats(youtube_api_key, CHANNEL_ID)

if data:
    print(f"✅ Data Found: {data['channel_name']} | Subs: {data['subscribers']}")
else:
    print("❌ Error: Could not find channel.")
    exit(1)

# --- STEP 3: LOAD ---
df = pd.DataFrame([data])

try:
    print(f"🚀 Uploading to BigQuery: {TABLE_ID}...")
    pandas_gbq.to_gbq(
        df,
        TABLE_ID,
        project_id=PROJECT_ID,
        if_exists="append",
        credentials=bq_credentials
    )
    print("✅ Success! Data pushed to cloud.")
except Exception as e:
    print(f"❌ BigQuery Error: {e}")
    exit(1)
