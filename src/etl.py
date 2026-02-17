import yaml
import os
import json
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime
import streamlit as st
from image_utils import get_dominant_color

# --- CONFIG & PATHS ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "config", "channels.yaml")
KEY_PATH = os.path.join(BASE_DIR, "service_key.json")
DATASET_ID = "youtube_analytics"
TABLE_ID = f"{DATASET_ID}.fact_video_metrics"

# --- AUTHENTICATION ---
def get_api_key():
    try:
        if hasattr(st, "secrets") and "YOUTUBE_API_KEY" in st.secrets:
            return st.secrets["YOUTUBE_API_KEY"]
    except FileNotFoundError:
        pass
    
    api_key = os.environ.get("YOUTUBE_API_KEY")
    if not api_key:
        raise ValueError("❌ YOUTUBE_API_KEY not found.")
    return api_key

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

def get_authenticated_service():
    return build('youtube', 'v3', developerKey=get_api_key())

def get_bq_credentials():
    if "GCP_SA_KEY" in os.environ:
        service_account_info = json.loads(os.environ["GCP_SA_KEY"])
        return service_account.Credentials.from_service_account_info(service_account_info), service_account_info["project_id"]
    
    try:
        if hasattr(st, "secrets") and "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            return service_account.Credentials.from_service_account_info(info), info["project_id"]
    except FileNotFoundError:
        pass

    if os.path.exists(KEY_PATH):
        creds = service_account.Credentials.from_service_account_file(KEY_PATH)
        with open(KEY_PATH) as f:
            project_id = json.load(f)["project_id"]
        return creds, project_id
        
    raise FileNotFoundError("❌ No GCP Credentials found.")

# --- YOUTUBE LOGIC (ROBUST) ---

def get_uploads_id(youtube, channel_id):
    """Try to get the Uploads Playlist ID. Returns None if it fails."""
    try:
        res = youtube.channels().list(id=channel_id, part='contentDetails').execute()
        if "items" in res and len(res["items"]) > 0:
            return res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    except Exception:
        return None
    return None

def get_videos_from_playlist(youtube, playlist_id, limit=5):
    """Method A: Fetch via Playlist (Cheapest & Best)"""
    try:
        res = youtube.playlistItems().list(
            playlistId=playlist_id, part='snippet', maxResults=limit
        ).execute()
        
        videos = []
        for item in res.get('items', []):
            snippet = item['snippet']
            thumbnails = snippet.get('thumbnails', {})
            thumb_url = thumbnails.get('high', thumbnails.get('standard', thumbnails.get('default', {}))).get('url')
            
            videos.append({
                'video_id': snippet['resourceId']['videoId'],
                'title': snippet['title'],
                'published_at': snippet['publishedAt'],
                'channel_title': snippet['channelTitle'],
                'thumbnail_url': thumb_url
            })
        return videos
    except Exception:
        return []

def get_videos_from_activities(youtube, channel_id, limit=5):
    """Method B: Fetch via Activities (Fallback for Channels with hidden playlists)"""
    try:
        # print(f"   ℹ️ Switching to Activities fallback for {channel_id}...")
        res = youtube.activities().list(
            channelId=channel_id,
            part='snippet,contentDetails',
            maxResults=limit + 5  # Fetch extra to skip non-upload activities
        ).execute()

        videos = []
        for item in res.get('items', []):
            # Only keep actual video uploads
            if 'upload' in item['contentDetails']:
                snippet = item['snippet']
                thumbnails = snippet.get('thumbnails', {})
                thumb_url = thumbnails.get('high', thumbnails.get('standard', thumbnails.get('default', {}))).get('url')
                
                videos.append({
                    'video_id': item['contentDetails']['upload']['videoId'],
                    'title': snippet['title'],
                    'published_at': snippet['publishedAt'],
                    'channel_title': snippet['channelTitle'],
                    'thumbnail_url': thumb_url
                })
                if len(videos) >= limit: break
        return videos
    except Exception as e:
        print(f"   ❌ Activities method failed for {channel_id}: {e}")
        return []

def get_video_stats(youtube, video_ids):
    if not video_ids: return {}
    try:
        res = youtube.videos().list(id=','.join(video_ids), part='statistics').execute()
        stats_map = {}
        for item in res.get('items', []):
            stats = item['statistics']
            stats_map[item['id']] = {
                'views': int(stats.get('viewCount', 0)),
                'likes': int(stats.get('likeCount', 0)),
                'comments': int(stats.get('commentCount', 0))
            }
        return stats_map
    except Exception:
        return {}

# --- MAIN ETL PIPELINE ---

def run_etl():
    print("🚀 Starting YouTube Velocity ETL...")
    
    config = load_config()
    youtube = get_authenticated_service()
    creds, project_id = get_bq_credentials()
    
    all_metrics = []
    
    for sector, channels in config['sectors'].items():
        print(f"\n📂 Processing Sector: {sector.upper()} ({len(channels)} channels)")
        
        for channel in channels:
            cid = channel['id'].strip() # Strip whitespace safety
            videos = []
            
            # 1. Try Method A: Uploads Playlist
            uploads_id = get_uploads_id(youtube, cid)
            if uploads_id:
                videos = get_videos_from_playlist(youtube, uploads_id)
            
            # 2. If Method A failed (empty or 404), Try Method B: Activities
            if not videos:
                videos = get_videos_from_activities(youtube, cid, limit=config['settings']['max_videos_to_fetch'])
            
            if not videos:
                print(f"   ⚠️ Skipping {cid} (Could not find videos via Playlist OR Activities)")
                continue

            # 3. Get Stats & Color
            vid_ids = [v['video_id'] for v in videos]
            stats = get_video_stats(youtube, vid_ids)
            
            for vid in videos:
                vid_id = vid['video_id']
                if vid_id in stats:
                    # Optional: Print to show progress
                    # print(f"   🎨 Analyzing: {vid['title'][:20]}...")
                    dom_color = get_dominant_color(vid['thumbnail_url'])
                    
                    row = {
                        'snapshot_at': datetime.utcnow(),
                        'sector': sector,
                        'channel_id': cid,
                        'channel_name': vid['channel_title'],
                        'video_id': vid_id,
                        'video_title': vid['title'],
                        'published_at': vid['published_at'],
                        'view_count': stats[vid_id]['views'],
                        'like_count': stats[vid_id]['likes'],
                        'comment_count': stats[vid_id]['comments'],
                        'thumbnail_url': vid['thumbnail_url'],
                        'dominant_color': dom_color
                    }
                    all_metrics.append(row)
            
            print(f"   ✅ Fetched {len(videos)} videos for {videos[0]['channel_title'] if videos else cid}")

    if all_metrics:
        df = pd.DataFrame(all_metrics)
        df['snapshot_at'] = pd.to_datetime(df['snapshot_at'])
        df['published_at'] = pd.to_datetime(df['published_at'])
        
        print(f"\n📦 Uploading {len(df)} rows to BigQuery...")
        try:
            pandas_gbq.to_gbq(
                df, TABLE_ID, project_id=project_id, if_exists='append', credentials=creds
            )
            print("✅ ETL Success! Data is live.")
        except Exception as e:
            print(f"❌ BigQuery Upload Failed: {e}")
    else:
        print("⚠️ No data collected.")

if __name__ == "__main__":
    run_etl()