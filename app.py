# app.py
import streamlit as st
import plotly.express as px
from streamlit_autorefresh import st_autorefresh
from utils.connect_minio import get_minio_connection
import pandas as pd
from zoneinfo import ZoneInfo

st.set_page_config(page_title="Citi Bike Analytics", layout="wide")

# Trim default Streamlit padding so each tab uses more of the visible screen
st.markdown("""
    <style>
        .block-container {padding-top: 1.5rem; padding-bottom: 1rem;}
    </style>
""", unsafe_allow_html=True)

# Refresh every 30,000 ms (30 seconds) — matches the GBFS poll interval
st_autorefresh(interval=30000, key="live_data_refresh")
st.title("🚲 Citi Bike Analytics")

CHART_HEIGHT = 340  # shared height so each tab fits without scrolling


# --- 1. CACHED BATCH DATA (Runs only once, then loads instantly from memory) ---
@st.cache_data
def load_batch_analytics():
    con = get_minio_connection('citibike_dbt/warehouse.duckdb')

    trips_by_day = con.execute("""
        SELECT d.date_key as date, COUNT(f.ride_id) as trip_count
        FROM fact_trips f JOIN dim_date d ON f.start_date_key = d.date_key
        GROUP BY d.date_key ORDER BY d.date_key
    """).fetchdf()

    busiest_stations = con.execute("""
        SELECT s.station_name, COUNT(f.ride_id) as trip_count
        FROM fact_trips f JOIN dim_station s ON f.start_station_id = s.station_id
        GROUP BY s.station_name ORDER BY trip_count DESC LIMIT 10
    """).fetchdf()

    user_split = con.execute("""
        SELECT member_casual, COUNT(ride_id) as trip_count
        FROM fact_trips WHERE member_casual IS NOT NULL GROUP BY member_casual
    """).fetchdf()

    durations = con.execute("""
        SELECT duration_minutes FROM fact_trips
        WHERE duration_minutes > 0 AND duration_minutes <= 80
    """).fetchdf()

    return trips_by_day, busiest_stations, user_split, durations


trips_by_day, busiest_stations, user_split, durations = load_batch_analytics()


# --- 2. LIVE STREAMING DATA (Runs fresh on every 30-second refresh) ---
@st.cache_resource
def get_live_connection():
    con = get_minio_connection('citibike_dbt/warehouse.duckdb')
    try:
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(
            "ATTACH 'dbname=bikeshare user=warehouse password=warehouse "
            "host=localhost port=5433' AS livedb (TYPE postgres);"
        )
    except Exception as e:
        # Surface connection problems instead of silently swallowing them —
        # otherwise later queries fail with a confusing "table not found"
        # instead of the real cause.
        st.warning(f"Could not attach live database: {e}")
    return con


con_live = get_live_connection()

# Headline metric: how many stations are completely empty right now
zero_count_df = con_live.execute("""
    SELECT COUNT(*) AS cnt
    FROM livedb.station_rolling_availability
    WHERE window_start = (SELECT MAX(window_start) FROM livedb.station_rolling_availability)
    AND avg_bikes_available = 0
""").fetchdf()
zero_count = zero_count_df['cnt'].iloc[0] if not zero_count_df.empty else 0

# Live lowest-availability snapshot
live_lowest = con_live.execute("""
    SELECT COALESCE(d.station_name, sr.station_name) AS display_name,
           sr.short_name AS station_id,
           ROUND(l.avg_bikes_available, 0) AS avg_bikes_available,
           l.window_start
    FROM livedb.station_rolling_availability l
    JOIN livedb.station_reference sr ON l.station_id = sr.gbfs_station_id
    LEFT JOIN dim_station d ON sr.short_name = d.station_id
    WHERE l.window_start = (SELECT MAX(window_start) FROM livedb.station_rolling_availability)
    ORDER BY avg_bikes_available ASC
    LIMIT 10
""").fetchdf()

# Live highest-availability snapshot
live_highest = con_live.execute("""
    SELECT COALESCE(d.station_name, sr.station_name) AS display_name,
           sr.short_name AS station_id,
           ROUND(l.avg_bikes_available, 0) AS avg_bikes_available,
           l.window_start
    FROM livedb.station_rolling_availability l
    JOIN livedb.station_reference sr ON l.station_id = sr.gbfs_station_id
    LEFT JOIN dim_station d ON sr.short_name = d.station_id
    WHERE l.window_start = (SELECT MAX(window_start) FROM livedb.station_rolling_availability)
    ORDER BY avg_bikes_available DESC
    LIMIT 10
""").fetchdf()

# Chronic shortage analytics — cumulative time spent empty.
# Deliberately unbounded (no time filter): unlike Redpanda's 48h topic retention,
# this table retains full history so "chronic" shortage is meaningful across the
# table's whole lifetime, not just a recent window. See DECISIONS.md.
chronic_starved_time = con_live.execute("""
    SELECT
        COALESCE(d.station_name, sr.station_name) AS display_name,
        COUNT(l.window_start) AS empty_windows
    FROM livedb.station_rolling_availability l
    JOIN livedb.station_reference sr ON l.station_id = sr.gbfs_station_id
    LEFT JOIN dim_station d ON sr.short_name = d.station_id
    WHERE l.avg_bikes_available = 0
    GROUP BY display_name, sr.short_name
    ORDER BY empty_windows DESC
    LIMIT 10
""").fetchdf()

if not chronic_starved_time.empty:
    # Each row represents a 5-minute window; convert count -> total hours empty
    chronic_starved_time['hours_empty'] = round((chronic_starved_time['empty_windows'] * 5) / 60, 1)

# Convert the live window's timestamp to NYC local time for display.
# Pipeline internals stay UTC throughout — conversion happens only here,
# at the presentation layer (see DECISIONS.md: Timestamps).
if not live_lowest.empty and 'window_start' in live_lowest.columns:
    raw_utc = live_lowest['window_start'].iloc[0]
    dt_utc = pd.to_datetime(raw_utc)
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.tz_localize("UTC")
    dt_nyc = dt_utc.tz_convert(ZoneInfo("America/New_York"))
    window_time_str = dt_nyc.strftime("%Y-%m-%d %I:%M:%S %p %Z")
else:
    window_time_str = "N/A"


# --- Dashboard Layout: tabbed so each view fits on screen without scrolling ---
tab1, tab2, tab3 = st.tabs(["📊 Historical Overview", "🔴 Live Status", "📈 Operational Bottlenecks"])

with tab1:
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Trips per Day")
        fig = px.line(trips_by_day, x='date', y='trip_count')
        fig.update_layout(height=CHART_HEIGHT, margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Member vs Casual")
        fig = px.pie(user_split, names='member_casual', values='trip_count', hole=0.4)
        fig.update_layout(height=CHART_HEIGHT, margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)
    with col3:
        st.subheader("Top 10 Busiest Start Stations")
        fig_bar = px.bar(busiest_stations, x='trip_count', y='station_name', orientation='h')
        fig_bar.update_yaxes(categoryorder='total ascending')
        fig_bar.update_layout(height=CHART_HEIGHT, margin=dict(t=20, b=20))
        st.plotly_chart(fig_bar, use_container_width=True)

    with col4:
        st.subheader("Trip Duration Distribution (0-2 hours)")
        fig = px.histogram(durations, x='duration_minutes', nbins=40)
        fig.update_layout(height=CHART_HEIGHT, margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.caption(f"Live window: {window_time_str} · Bridged via GBFS short_name")

    st.metric(
        label="Stations Currently Empty (0 Bikes)",
        value=zero_count,
        help="Total number of stations with an average of 0 available bikes in the latest 5-minute window"
    )

    live_col1, live_col2 = st.columns(2)
    with live_col1:
        st.markdown("#### Running Lowest (Need Bikes)")
        if not live_lowest.empty:
            st.bar_chart(live_lowest.set_index('display_name')['avg_bikes_available'], height=CHART_HEIGHT)
        else:
            st.info("No live data yet")

    with live_col2:
        st.markdown("#### Running Highest (Need Docks Freed)")
        if not live_highest.empty:
            st.bar_chart(live_highest.set_index('display_name')['avg_bikes_available'], height=CHART_HEIGHT)
        else:
            st.info("No live data yet")

with tab3:
    st.subheader("Long-Term Operational Bottlenecks")
    if not chronic_starved_time.empty:
        num_cols = st.columns(min(3, len(chronic_starved_time)))
        for idx, col in enumerate(num_cols):
            if idx < len(chronic_starved_time):
                row = chronic_starved_time.iloc[idx]
                col.metric(
                    label=f"Rank #{idx+1}: {row['display_name']}",
                    value=f"{row['hours_empty']} hrs",
                    help=f"Spent {row['hours_empty']} total hours with 0 available bikes"
                )

        st.markdown("<br>", unsafe_allow_html=True)

        table_df = chronic_starved_time[['display_name', 'hours_empty']].rename(
            columns={
                'display_name': 'Station Name',
                'hours_empty': 'Total Hours Empty (hrs)'
            }
        )
        st.dataframe(table_df, use_container_width=True, hide_index=True)
    else:
        st.info("No historical zero-bike data available yet.")