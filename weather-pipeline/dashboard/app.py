import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os
from datetime import datetime, timedelta

# Thêm đường dẫn gốc của dự án vào sys.path để import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
try:
    from config.settings import GCP_PROJECT_ID, BIGQUERY_DATASET, BIGQUERY_TABLES, CITIES
except ImportError:
    # Fallback nếu không import được config
    GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
    BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'weather_data')
    BIGQUERY_TABLES = {
        'STREAMING': 'weather_streaming',
        'STREAMING_HOURLY': 'weather_streaming_hourly',
        'ANALYTICS': 'weather_analytics'
    }
    CITIES = ['Hanoi', 'Ho Chi Minh City', 'Da Nang', 'Can Tho', 'Hue']

# Set page config
st.set_page_config(
    page_title="Weather Data Dashboard",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title
st.title("Weather Data Dashboard")
st.markdown("A dashboard for visualizing weather data from the pipeline")

# Sidebar
st.sidebar.header("Filters")
selected_city = st.sidebar.selectbox("Select City", CITIES)
days = st.sidebar.slider("Days to show", 1, 30, 7)
metric = st.sidebar.selectbox("Select Metric", ["temperature", "humidity", "pressure", "wind_speed"])

# Main content
st.header(f"Weather Data for {selected_city}")

# Placeholder for data
st.info("This is a placeholder dashboard. In a real implementation, it would connect to BigQuery and display actual data.")

# Create some sample data
dates = [datetime.now().date() - timedelta(days=i) for i in range(days)]
values = [20 + i % 5 for i in range(days)]  # Sample temperature values

# Create a DataFrame
df = pd.DataFrame({
    'date': dates,
    'value': values
})

# Create a chart
fig = px.line(df, x='date', y='value', title=f"{metric.capitalize()} over time")
st.plotly_chart(fig, use_container_width=True)

# Display some stats
st.subheader("Statistics")
col1, col2, col3 = st.columns(3)
col1.metric("Average", f"{sum(values)/len(values):.1f}")
col2.metric("Min", f"{min(values):.1f}")
col3.metric("Max", f"{max(values):.1f}")

# About
st.sidebar.markdown("---")
st.sidebar.info("This dashboard is part of the Weather Data Pipeline project.")
