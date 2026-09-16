# app.py
import streamlit as st
import plotly.express as px
from utils.connect_minio import get_minio_connection

con = get_minio_connection('citibike_dbt/warehouse.duckdb')

st.set_page_config(page_title="Citi Bike Analytics", layout="wide")
st.title("🚲 Citi Bike Analytics")

# 1. Trips per day (line chart)
trips_by_day = con.execute("""
    SELECT 
        d.date_key as date, 
        COUNT(f.ride_id) as trip_count
    FROM fact_trips f
    JOIN dim_date d ON f.start_date_key = d.date_key
    GROUP BY d.date_key 
    ORDER BY d.date_key
""").fetchdf()

# 2. Busiest stations (bar chart)
busiest_stations = con.execute("""
    SELECT 
        s.station_name, 
        COUNT(f.ride_id) as trip_count
    FROM fact_trips f
    JOIN dim_station s ON f.start_station_id = s.station_id
    GROUP BY s.station_name 
    ORDER BY trip_count DESC 
    LIMIT 10
""").fetchdf()

# 3. Member vs. casual split (pie chart)
user_split = con.execute("""
    SELECT 
        member_casual, 
        COUNT(ride_id) as trip_count
    FROM fact_trips
    WHERE member_casual IS NOT NULL
    GROUP BY member_casual
""").fetchdf()

# 4. Trip duration distribution (histogram)
# Capping at 120 minutes to keep the histogram visually readable
durations = con.execute("""
    SELECT duration_minutes 
    FROM fact_trips 
    WHERE duration_minutes > 0 AND duration_minutes <= 80
""").fetchdf()

# --- Dashboard Layout ---

col1, col2 = st.columns(2)

with col1:
    st.subheader("Trips per Day")
    st.plotly_chart(px.line(trips_by_day, x='date', y='trip_count'), use_container_width=True)
    
with col2:
    st.subheader("Member vs Casual")
    st.plotly_chart(px.pie(user_split, names='member_casual', values='trip_count', hole=0.4), use_container_width=True)

col3, col4 = st.columns(2)

with col3:
    st.subheader("Top 10 Busiest Start Stations")
    # Horizontal bar chart sorted by volume
    fig_bar = px.bar(busiest_stations, x='trip_count', y='station_name', orientation='h')
    fig_bar.update_yaxes(categoryorder='total ascending')
    st.plotly_chart(fig_bar, use_container_width=True)
    
with col4:
    st.subheader("Trip Duration Distribution (0-2 hours)")
    st.plotly_chart(px.histogram(durations, x='duration_minutes', nbins=40), use_container_width=True)