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
cursor.execute("""SELECT date, min_temp_k, max_temp_k, median_temp_k, morning_temp_k, 
                       afternoon_temp_k, evening_temp_k, night_temp_k, humidity, 
                       precipitation, wind_speed, wind_direction, usage_kwh 
                FROM power_usage_vs_weather ORDER BY date""")

# Separate data into lists
dates = []
temps_min = []
temps_max = []
temps_median = []
temps_morning = []
temps_afternoon = []
temps_evening = []
temps_night = []
humidity = []
precipitation = []
wind_speed = []
wind_direction = []
usage = []

for row in cursor:
    (date, temp_min, temp_max, temp_median, temp_morning, temp_afternoon,
     temp_evening, temp_night, hum, precip, wind_spd, wind_dir, use) = row
    
    dates.append(date)
    temps_min.append(temp_min)
    temps_max.append(temp_max)
    temps_median.append(temp_median)
    temps_morning.append(temp_morning)
    temps_afternoon.append(temp_afternoon)
    temps_evening.append(temp_evening)
    temps_night.append(temp_night)
    humidity.append(hum)
    precipitation.append(precip)
    wind_speed.append(wind_spd)
    wind_direction.append(wind_dir)
    usage.append(use)

# Close the connection
conn.close()

# Create Plotly figure
fig = go.Figure([
    go.Scatter(x=dates, y=temps_min, mode="lines", name="Min Temperature"),
    go.Scatter(x=dates, y=temps_max, mode="lines", name="Max Temperature"),
    go.Scatter(x=dates, y=temps_median, mode="lines", name="Median Temp"),
    go.Scatter(x=dates, y=temps_morning, mode="lines", name="Morning Temperature"),
    go.Scatter(x=dates, y=temps_afternoon, mode="lines", name="Afternoon Temperature"),
    go.Scatter(x=dates, y=temps_evening, mode="lines", name="Evening Temperature"),
    go.Scatter(x=dates, y=temps_night, mode="lines", name="Night Temperature"),
    go.Scatter(x=dates, y=humidity, mode="lines", name="Humidity (%)", yaxis="y3"),
    go.Scatter(x=dates, y=precipitation, mode="lines", name="Precipitation (mm)", yaxis="y4"),
    go.Scatter(x=dates, y=wind_speed, mode="lines", name="Wind Speed (m/s)", yaxis="y5"),
    go.Scatter(x=dates, y=usage, mode="lines", name="Electricity Usage (kWh)", yaxis="y2"),
    go.Scatter(x=dates, y=usage, mode="lines", name="Usage (Inverted)", yaxis="y6")  # No need to multiply by -1
])

# Configure multiple y-axes
fig.update_layout(
    title="Electricity Usage vs. Weather Conditions",
    xaxis_title="Date",
    yaxis=dict(title="Temperature (K)", side="left"),  # Keep label
    yaxis2=dict(title="Electricity Usage (kWh)", overlaying="y", side="right"),  # Keep label
    yaxis3=dict(overlaying="y", side="left", showticklabels=False),  # Hide label
    yaxis4=dict(overlaying="y", side="right", showticklabels=False),  # Hide label
    yaxis5=dict(overlaying="y", side="right", showticklabels=False),  # Hide label
    yaxis6=dict(overlaying="y", side="right", showticklabels=False, autorange="reversed")  # Fix for inverted usage
)
# Show the plot
fig.show()