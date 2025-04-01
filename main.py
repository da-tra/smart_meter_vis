import requests
import sqlite3
import pyarrow as pa
from pathlib import Path
from pprint import pprint

# Plot usage data vs weather data
import plotly.graph_objects as go
import sqlite3

# Connect to the database
conn = sqlite3.connect("power_usage_vs_weather.db")
cursor_plot = conn.cursor()

# Fetch data from the table
cursor_plot.execute("SELECT date, afternoon_temp_k, usage_kwh FROM power_usage_vs_weather ORDER BY date")

# Separate data into lists
dates = []
temps = []
usage = []

for row in cursor_plot:
    date, temp, use = row
    dates.append(date)
    temps.append(temp)
    usage.append(use)

# Close the connection
conn.close()

# Create Plotly figure
fig = go.Figure([
    go.Scatter(x=dates, y=temps, mode="lines", name="Temperature"),
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
