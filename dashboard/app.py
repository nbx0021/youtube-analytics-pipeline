import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import logging
import pytz
import numpy as np
from datetime import datetime, timedelta

# =========================
# 🧾 CONFIG & LOGGING
# =========================
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.ERROR)

st.set_page_config(
    page_title="YouTube Velocity Intelligence",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =========================
# 🎨 1. ADVANCED CSS
# =========================
st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');
        html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
        
        div[data-testid="stMetric"] {
            background: #ffffff;
            border: 1px solid #e0e0e0;
            padding: 10px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }
        
        /* Grid Card Styling */
        .video-card {
            background-color: #ffffff;
            padding: 15px;
            border-radius: 10px;
            margin-bottom: 15px;
            border: 1px solid #e0e0e0;
            box-shadow: 0 2px 5px rgba(0,0,0,0.05);
        }
        
        /* Badges */
        .gold-badge { background:#FFD700; color:black; padding:5px; border-radius:4px; font-weight:bold; text-align:center; margin-bottom:5px; }
        .silver-badge { background:#C0C0C0; color:black; padding:5px; border-radius:4px; font-weight:bold; text-align:center; margin-bottom:5px; }
        .bronze-badge { background:#CD7F32; color:white; padding:5px; border-radius:4px; font-weight:bold; text-align:center; margin-bottom:5px; }
    </style>
""", unsafe_allow_html=True)

# =========================
# 🛠️ 2. UTILITIES
# =========================
def clean_title(title, max_len=30):
    if len(title) > max_len: return title[:max_len] + "..."
    return title

def format_views(num):
    """
    Smart formatting for view counts.
    31,392,500 -> 31.4M
    500,000    -> 500.0K
    """
    if num >= 1_000_000:
        return f"{num/1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{num/1_000:.1f}K"
    else:
        return str(num)

def ui_error_message(title, message, details=None):
    st.error(f"**{title}**")
    st.write(message)
    if details:
        with st.expander("🔍 Technical Trace"): st.code(details)

# =========================
# 🔐 3. AUTHENTICATION
# =========================
@st.cache_resource
def get_bigquery_client():
    try:
        if "gcp_service_account" in st.secrets:
            creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
            return bigquery.Client(credentials=creds, project=creds.project_id)
        
        key_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "service_key.json")
        if os.path.exists(key_path):
            creds = service_account.Credentials.from_service_account_file(key_path)
            return bigquery.Client(credentials=creds, project=creds.project_id)
            
        raise FileNotFoundError("Missing 'service_key.json' or secrets.toml")
    except Exception as e:
        ui_error_message("Authentication Failed", "Check credentials.", str(e))
        st.stop()

# =========================
# 📥 4. DATA LOADING
# =========================
@st.cache_data(ttl=300)
def load_data():
    client = get_bigquery_client()
    query = """
        SELECT *
        FROM `youtube_analytics.fact_video_metrics`
        WHERE snapshot_at >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 7 DAY)
        ORDER BY snapshot_at ASC
    """
    try:
        df = client.query(query).to_dataframe()
        if df.empty: return pd.DataFrame()
        
        # 🟢 TIMEZONE CONVERSION (UTC -> IST)
        ist = pytz.timezone('Asia/Kolkata')
        
        def convert_to_ist(series):
            if series.dt.tz is None:
                series = series.dt.tz_localize('UTC')
            return series.dt.tz_convert(ist)

        df["snapshot_at"] = convert_to_ist(pd.to_datetime(df["snapshot_at"]))
        df["published_at"] = convert_to_ist(pd.to_datetime(df["published_at"]))
        df["upload_time_str"] = df["published_at"].dt.strftime('%d %b, %I:%M %p')

        # Engagement Rate
        df["engagement_rate"] = np.where(
            df["view_count"] > 0,
            ((df["like_count"] + df["comment_count"]) / df["view_count"]) * 100,
            0
        )

        df["short_title"] = df["video_title"].apply(lambda x: clean_title(x))
        df["is_caps"] = df["video_title"].apply(lambda x: 1 if x.isupper() else 0)
        df["publish_hour"] = df["published_at"].dt.hour
        df["publish_day"] = df["published_at"].dt.day_name()
        
        return df

    except Exception as e:
        ui_error_message("Data Error", "Could not load data.", str(e))
        st.stop()

# =========================
# 🧠 5. DASHBOARD ENGINE
# =========================
def main():
    df_raw = load_data()
    if df_raw.empty:
        st.warning("⚠️ No data. Run `python -m src.etl`")
        st.stop()

    # --- SIDEBAR ---
    with st.sidebar:
        st.header("⚡ Control Center")
        sectors = ["All"] + sorted(df_raw["sector"].unique().tolist())
        selected_sector = st.selectbox("Industry Sector:", sectors)
        
        if selected_sector != "All":
            df_filtered = df_raw[df_raw["sector"] == selected_sector]
        else:
            df_filtered = df_raw.copy()
            
        st.markdown("---")
        st.info("**Narendra Bhandari**\n\nv4.0 Smart Formatting")

    # --- FILTER LOGIC (Latest Batch) ---
    latest_ts = df_filtered["snapshot_at"].max()
    cutoff_time = latest_ts - timedelta(minutes=15)
    
    df_latest = df_filtered[df_filtered["snapshot_at"] >= cutoff_time].copy()
    df_latest = df_latest.sort_values("snapshot_at", ascending=False).drop_duplicates(subset="video_id")

    refresh_time_str = latest_ts.strftime('%d %b, %I:%M %p')

    # --- HEADER ---
    col_title, col_time = st.columns([3, 1])
    with col_title:
        st.title(f"🚀 {selected_sector} Velocity Tracker")
    with col_time:
        st.metric("🕒 Last DB Update (IST)", refresh_time_str)

    # --- MAIN KPI ROW ---
    if not df_latest.empty:
        top_vid = df_latest.sort_values("view_count", ascending=False).iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        # 🟢 UPDATED: Smart Format for Viral King
        with c1: st.metric("🔥 Viral King", format_views(top_vid['view_count']), top_vid['channel_name'])
        with c2: st.metric("❤️ Avg Engagement", f"{df_latest['engagement_rate'].mean():.2f}%")
        with c3:
            caps_df = df_latest[df_latest['is_caps'] == 1]
            norm_df = df_latest[df_latest['is_caps'] == 0]
            if not caps_df.empty and not norm_df.empty:
                lift = ((caps_df['view_count'].mean() - norm_df['view_count'].mean()) / norm_df['view_count'].mean()) * 100
                st.metric("📣 CAPS Lift", f"{lift:+.1f}%", "Vs Normal")
            else:
                st.metric("📣 CAPS Lift", "N/A", "Need Data")
        with c4: st.metric("⏱️ Active Assets", len(df_latest))

    st.markdown("---")

    # --- TABS ---
    tab_velocity, tab_color, tab_heat, tab_table, tab_india = st.tabs([
        "📈 Growth Velocity", 
        "🎨 Color Psychology", 
        "🔥 Strategy Heatmap", 
        "🏆 Top 3 Podium",
        "🇮🇳 India Gallery"
    ])

    # 1. VELOCITY CHART
    with tab_velocity:
        if df_filtered["snapshot_at"].nunique() < 2:
            st.warning("⚠️ **Not enough history for Velocity Chart.** Run ETL again in 1 hour.")
        else:
            fig = px.line(
                df_filtered, 
                x="snapshot_at", 
                y="view_count", 
                color="short_title", 
                hover_name="video_title",
                markers=True,
                height=600,
                template="plotly_white",
                title="View Growth Over Time (IST)"
            )
            fig.update_layout(
                hovermode="x unified",
                legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.02, title="Assets"),
                margin=dict(r=150)
            )
            st.plotly_chart(fig, theme="streamlit", use_container_width=True) 

    # 2. COLOR PSYCHOLOGY
    with tab_color:
        if "dominant_color" in df_latest.columns:
            fig_color = px.scatter(
                df_latest,
                x="view_count",
                y="like_count",
                size="engagement_rate",
                color="dominant_color",
                color_discrete_map="identity",
                template="plotly_dark",
                height=500,
                log_x=True, log_y=True,
                title="Thumbnail Color Performance"
            )
            st.plotly_chart(fig_color, theme="streamlit", use_container_width=True)

    # 3. HEATMAP
    with tab_heat:
        df_heat = df_latest.groupby(['publish_day', 'publish_hour']).size().reset_index(name='count')
        days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        fig_heat = px.density_heatmap(
            df_heat, x="publish_hour", y="publish_day", z="count", 
            nbinsx=24, category_orders={"publish_day": days_order}, color_continuous_scale="Viridis"
        )
        st.plotly_chart(fig_heat, theme="streamlit", use_container_width=True)

    # 4. PODIUM
    with tab_table:
        st.subheader("🏆 The Podium")
        top_3 = df_latest.sort_values("view_count", ascending=False).head(3)
        cols = st.columns(3)
        badges = ['<div class="gold-badge">🥇 GOLD</div>', '<div class="silver-badge">🥈 SILVER</div>', '<div class="bronze-badge">🥉 BRONZE</div>']
        
        for i, (idx, vid) in enumerate(top_3.iterrows()):
            if i < 3:
                with cols[i]:
                    st.markdown(badges[i], unsafe_allow_html=True)
                    st.image(vid["thumbnail_url"])
                    st.markdown(f"**{vid['short_title']}**")
                    # 🟢 UPDATED: Smart Format for Podium
                    st.caption(f"👀 {format_views(vid['view_count'])} | 📅 {vid['upload_time_str']}")

        st.divider()
        st.subheader("📋 Full Data")
        st.dataframe(
            df_latest[["thumbnail_url", "channel_name", "video_title", "view_count", "engagement_rate", "upload_time_str"]].sort_values("view_count", ascending=False),
            column_config={
                "thumbnail_url": st.column_config.ImageColumn("Preview"),
                "view_count": st.column_config.NumberColumn("Views", format="%d"),
                "engagement_rate": st.column_config.ProgressColumn("Engagement", format="%.2f%%", min_value=0, max_value=10),
            },
            hide_index=True,
            use_container_width=True
        )

    # 5. 🇮🇳 INDIA GALLERY
    with tab_india:
        # A. Calculate India Specific KPIs
        india_latest = df_latest[df_latest['sector'] == 'india_top'].copy()
        
        if india_latest.empty:
            st.info("No 'India Top' data found. Select the sector in the sidebar or check ETL.")
        else:
            st.markdown("### 🇮🇳 India Top YouTube Channel Performance")
            
            # India KPIs
            k1, k2, k3, k4 = st.columns(4)
            with k1: 
                avg_eng_in = india_latest["engagement_rate"].mean()
                st.metric("❤️ Avg Engagement", f"{avg_eng_in:.2f}%")
            with k2:
                caps_in = india_latest[india_latest['is_caps'] == 1]
                norm_in = india_latest[india_latest['is_caps'] == 0]
                if not caps_in.empty and not norm_in.empty:
                    lift_in = ((caps_in['view_count'].mean() - norm_in['view_count'].mean()) / norm_in['view_count'].mean()) * 100
                    st.metric("📣 CAPS Lift", f"{lift_in:+.1f}%", "Vs Normal")
                else:
                    st.metric("📣 CAPS Lift", "N/A")
            with k3: st.metric("⏱️ Active Assets", len(india_latest))
            # 🟢 UPDATED: Smart Format for KPI
            with k4: st.metric("🔥 Top View Count", format_views(india_latest['view_count'].max()))

            st.markdown("---")
            
            # 🟢 VIDEO GRID
            cols = st.columns(4)
            india_sorted = india_latest.sort_values("view_count", ascending=False)

            for i, (index, row) in enumerate(india_sorted.iterrows()):
                col = cols[i % 4]
                with col:
                    with st.container():
                        st.markdown('<div class="video-card">', unsafe_allow_html=True)
                        st.image(row['thumbnail_url'], use_container_width=True)
                        st.markdown(f"**{row['short_title']}**")
                        st.caption(f"📺 {row['channel_name']}")
                        
                        # Metrics Row inside Card
                        m1, m2 = st.columns(2)
                        # 🟢 UPDATED: Smart Format for Grid
                        with m1: st.metric("Views", format_views(row['view_count']))
                        with m2: st.metric("Eng.", f"{row['engagement_rate']:.1f}%")
                        
                        st.markdown('</div>', unsafe_allow_html=True)

if __name__ == "__main__":
    main()