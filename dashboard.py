# =========================
# 📊 YOUTUBE GROWTH DASHBOARD
# =========================

import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import logging

# =========================
# 🔧 APP CONFIG
# =========================
st.set_page_config(
    page_title="YouTube Growth Tracker",
    page_icon="📺",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title(" Influencer Growth Tracker")
st.caption("Industry-grade analytics powered by BigQuery")

# =========================
# 🧾 LOGGING
# =========================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================
# 🔐 CREDENTIALS & CLIENT
# =========================

# In dashboard.py
@st.cache_resource
def get_bigquery_client():
    # Attempt to load from Streamlit Secrets (Works on Cloud AND Local now!)
    if "gcp_service_account" in st.secrets:
        creds = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        return bigquery.Client(credentials=creds, project=creds.project_id)
    else:
        raise RuntimeError("❌ Missing Secrets! Check .streamlit/secrets.toml")

# =========================
# 📥 DATA LOADING
# =========================
@st.cache_data(ttl=600)
def load_data():
    client = get_bigquery_client()

    query = """
        SELECT
            date,
            channel_name,
            subscribers,
            total_views
        FROM `youtube_analytics.top_channels_stats`
        ORDER BY date ASC
    """

    df = client.query(query).to_dataframe()
    df["date"] = pd.to_datetime(df["date"])
    return df


# =========================
# 🧮 FEATURE ENGINEERING
# =========================
def enrich_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["channel_name", "date"])
    df["subscriber_growth"] = df.groupby("channel_name")["subscribers"].diff()
    df["view_growth"] = df.groupby("channel_name")["total_views"].diff()
    
    # FILL NAN WITH 0 (So Day 1 doesn't look broken)
    df.fillna(0, inplace=True) 
    
    return df


# =========================
# 🚦 MAIN APP
# =========================
try:
    df = load_data()

    if df.empty:
        st.warning("No data available.")
        st.stop()

    df = enrich_metrics(df)

    latest_date = df["date"].max()
    latest = df[df["date"] == latest_date]

    # =========================
    # 📌 KPI METRICS
    # =========================
    st.subheader(f"📅 Latest Snapshot — {latest_date.date()}")

    metric_cols = st.columns(len(latest))
    for col, (_, row) in zip(metric_cols, latest.iterrows()):
        with col:
            st.metric(
                label=row["channel_name"],
                value=f"{row['subscribers'] / 1_000_000:.2f}M subs",
                delta=f"+{row['subscriber_growth'] / 1_000:.1f}K"
            )

    st.divider()

    # =========================
    # 📊 TABS
    # =========================
    tab1, tab2, tab3, tab4 = st.tabs(
        ["📈 Growth Trends", "🏆 Rankings", "📊 Distribution", "🔥 Heatmap"]
    )

    # =========================
    # 📈 LINE CHARTS
    # =========================
    with tab1:
        st.subheader("Subscribers Over Time")
        fig = px.line(
            df,
            x="date",
            y="subscribers",
            color="channel_name",
            markers=True
        )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Views Over Time")
        fig = px.line(
            df,
            x="date",
            y="total_views",
            color="channel_name",
            markers=True
        )
        st.plotly_chart(fig, use_container_width=True)

    # =========================
    # 🏆 RANKING BAR CHART
    # =========================
    with tab2:
        st.subheader("Top Channels by Subscribers")
        fig = px.bar(
            latest.sort_values("subscribers", ascending=False),
            x="channel_name",
            y="subscribers",
            text_auto=".2s"
        )
        st.plotly_chart(fig, use_container_width=True)

    # =========================
    # 📊 PIE / SHARE CHART
    # =========================
    with tab3:
        st.subheader("Subscriber Share")
        fig = px.pie(
            latest,
            names="channel_name",
            values="subscribers",
            hole=0.45
        )
        st.plotly_chart(fig, use_container_width=True)

    # =========================
    # 🔥 HEATMAP
    # =========================
    with tab4:
        st.subheader("Subscriber Growth Heatmap")

        pivot = df.pivot_table(
            index="channel_name",
            columns="date",
            values="subscriber_growth"
        )

        fig = px.imshow(
            pivot,
            aspect="auto",
            color_continuous_scale="RdYlGn"
        )
        st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    logger.exception(e)
    st.error("❌ Application failed. Check logs for details.")


st.markdown("---")
st.caption("Developed by Narendra Bhandari @2026")
# =========================