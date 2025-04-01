import requests
import sqlite3
import pyarrow as pa
from pathlib import Path
from pprint import pprint

import plotly.graph_objects as go
import sqlite3

# Plot usage data vs weather data

# Connect to the database
filename_db = "power_usage_vs_weather.db"
filepath_db = f"./db/{filename_db}"
# Create database connection
conn = sqlite3.connect(filepath_db)
cursor = conn.cursor()

# Fetch data from the table
cursor.execute("SELECT date, afternoon_temp_k, usage_kwh FROM power_usage_vs_weather ORDER BY date")

# Separate data into lists
dates = []
temp_afternoon = []
temps_min = []
temps_max = []
temps_median_no_minmax = []
temps_median = []
temps_morning = []
temps_evening = []
temps_night = []
humidity = []
precipitation = []
wind_sped_max = []
wind_direction = []
usage = []

for row in cursor:
    date, temp, use = row
    dates.append(date)
    temp_afternoon.append(temp)
    usage.append(use)

# Close the connection
conn.close()

# Create Plotly figure
fig = go.Figure([
    go.Scatter(x=dates, y=temp_afternoon, mode="lines", name="Temperature"),
    go.Scatter(x=dates, y=usage, mode="lines", name="Electricity Usage (kWh)", yaxis="y2")
])

# Configure dual y-axes
fig.update_layout(
    title="Electricity Usage vs. Temperature",
    xaxis_title="Date",
    yaxis=dict(title="Temperature (K)", side="left"),
    yaxis2=dict(title="Electricity Usage (kWh)", overlaying="y", side="right"),
)

# Show the plot
fig.show()
