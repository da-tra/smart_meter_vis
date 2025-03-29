
# Plot usage data vs weather data
import plotly.graph_objects as go
import sqlite3

# Connect to the database
conn = sqlite3.connect("joined_data.db")
cursor_plot = conn.cursor()

# Fetch data from the table
cursor_plot.execute("SELECT date, afternoon_temp_k, usage_kwh FROM joined_usage_weather ORDER BY date")
data = cursor_plot.fetchall()  # List of tuples

# Close the connection
conn.close()

# Separate data into lists
dates, temps, usage = zip(*data)  # Unpack tuples into separate lists

# Create Plotly figure
fig = go.Figure([
    go.Scatter(x=dates, y=temps, mode="lines", name="Temperature (K)"),
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
