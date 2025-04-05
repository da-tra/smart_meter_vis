"""Generate a visualisation of your smart meter data in .csv format.

The visualisation includes graphs for weather data for Vienna.
"""
import csv
import sqlite3
from datetime import datetime
from statistics import median

import plotly.graph_objects as go
import requests

from utils import utils

# Read CSV with usage data (generated via customer profile at https://smartmeter-web.wienernetze.at/ ) 
# load the data with the module csv

# TODO generalise this section to include any .csv file in the specified folder
filename_csv = "TAGESWERTE-20220325-bis-20250324.csv"
filepath_csv = f"./smart_meter_data/{filename_csv}"
with open(filepath_csv, mode="r", encoding="utf-8") as f:  # noqa: PTH123
    usage_data = csv.reader(f, delimiter=";")
    # Load the header row and advance to next row
    header = next(usage_data)
    print(f" header: {header}")

    # Store usage data in an SQL table
    # Define name of database file
    filename_db = "power_usage_vs_weather.db"
    # Create path based on database filename
    # All databases are to be stored in the directory "./db"
    filepath_db = f"./db/{filename_db}"
    # Create database connection
    conn = sqlite3.connect(filepath_db)
    cursor = conn.cursor()

    # Define table name
    table_name = "power_usage_vs_weather"
    # Define the column names and their types in a dictionary ("column": "TYPE")
    columns_usage = {
        "id": "INTEGER PRIMARY KEY",
        "date": "TEXT UNIQUE",
        "usage_kwh": "REAL",
        }

    # Construct the SQL query for creating a table if it doesn't exist
    columns_block = utils.build_columns_string(columns_usage)

    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_block}
    )
    """
    # Execure the query and commit the results to the database
    cursor.execute(query)
    conn.commit()

    # Write usage data from CSV object to the newly created SQL table
    for row in usage_data:
        # Get date from first column and format to YYYY-MM-DD
        date_csv = row[0]
        date_csv = datetime.strptime(date_csv, "%d.%m.%y")
        date_csv = date_csv.strftime("%Y-%m-%d")

        # Get power consumption from second column
        usage = row[1]

        # If the current row of the usage csv is empty:
        #   i) report missing data
        #   ii) leave loop, work to next row
        if not usage:
            print(f"Skipped: {date_csv} (No data)")  # noqa: T201
            continue
        # Otherwise: reformat usage data from 1,12 to 1.23
        else:
            usage = float(usage.replace(",", "."))

        # Only write usage data for selected date to the SQL table if there is none yet
        #   Query the SQL table for data for the date from the csv
        query = f"SELECT usage_kwh FROM {table_name} WHERE date = ?"
        cursor.execute(query, (date_csv,))
        #   Examine the result for content
        rows_sql = cursor.fetchone()

        if not rows_sql:
            # If there is now content, add the new data
            print(f"Adding usage data for {date_csv} to {filepath_db}:{table_name}")
        else:
            # Otherwise end this loop iteration early
            for row_sql in rows_sql:
                print(f"{usage} kWh on {date_csv}: (Already in database)")
            continue

        #   Write row to sql table
        query = f"""
                INSERT INTO {table_name} (date, usage_kwh)
                VALUES (?, ?)
                """  # noqa: S608
        cursor.execute(query, (date_csv, usage))

    conn.commit()
    conn.close()


# This next section...
# - defines details for an API GET call to Openweathermap.org for daily weather data
# - expands the SQL table by adding new columns to accommodate data received via API

# API Details: Get API key for weather app
with open("api_key.txt", "r") as f:  # noqa: PTH123
    API_KEY = f.read().strip()
# Set longitude and latitude to Vienna, AT
LAT, LON = 48.2083537, 16.3725042

# Limit API costs by limiting the number API calls
API_CALL_LIMIT = 9  # Change this number as needed

# Define new columns with their respective types. These are defined by the API response
# For JSON schema see API documentation: https://openweathermap.org/api/one-call-3#hist_agr_parameter
columns = {
    "min_temp_k": "REAL",
    "max_temp_k": "REAL",
    "temp_median_no_minmax_k": "REAL",
    "median_temp_k": "REAL",
    "morning_temp_k": "REAL",
    "afternoon_temp_k": "REAL",
    "evening_temp_k": "REAL",
    "night_temp_k": "REAL",
    "humidity": "REAL",
    "precipitation": "REAL",
    "wind_speed": "REAL",
    "wind_direction": "REAL",
    "retrieval_date": "TEXT",
    }

# Add new weather-related columns to the table (if they don't exist)

conn = sqlite3.connect(filepath_db)
cursor = conn.cursor()

for column, col_type in columns.items():
    query_add_column = f"ALTER TABLE {table_name} ADD COLUMN {column} {col_type}"

    try:
        cursor.execute(query_add_column)
    except sqlite3.OperationalError:
        print(f"operational error: {column} already exists")
        # Column already exists

# Commit changes
conn.commit()

# Fetch missing data from API
cursor.execute("SELECT date FROM power_usage_vs_weather WHERE retrieval_date IS NULL OR retrieval_date = '' LIMIT ?", (API_CALL_LIMIT,))
rows = cursor.fetchall()

api_calls_made = 0  # Track number of API calls

for row in rows:
    if api_calls_made >= API_CALL_LIMIT:
        print("API call limit reached. Stopping further requests.")
        break

    stored_date = row[0]
    print(f"Fetching data for {stored_date}...")

    # API Call
    api_url = f"https://api.openweathermap.org/data/3.0/onecall/day_summary?lat={LAT}&lon={LON}&date={stored_date}&appid={API_KEY}"
    response = requests.get(api_url)

    if response.status_code == 200:
        data = response.json()  # Parse JSON

        # Compute temperature medians
        temp_values_no_minmax = [
            data["temperature"]["morning"],
            data["temperature"]["afternoon"],
            data["temperature"]["evening"],
            data["temperature"]["night"]
        ]
        temp_median_no_minmax = median(temp_values_no_minmax)

        temp_values = [
            data["temperature"]["min"],
            data["temperature"]["max"],
            *temp_values_no_minmax
        ]
        temp_median = median(temp_values)

        # Prepare update data
        retrieval_date = datetime.today().strftime('%Y-%m-%d')

        # **Step 3: Update database with API data**
        cursor.execute("""
            UPDATE power_usage_vs_weather
            SET min_temp_k = ?, max_temp_k = ?, temp_median_no_minmax_k = ?, median_temp_k = ?, 
                morning_temp_k = ?, afternoon_temp_k = ?, evening_temp_k = ?, night_temp_k = ?, 
                humidity = ?, precipitation = ?, wind_speed = ?, wind_direction = ?, retrieval_date = ?
            WHERE date = ?
            """, (
                data["temperature"]["min"], data["temperature"]["max"],
                temp_median_no_minmax, temp_median,
                data["temperature"]["morning"], data["temperature"]["afternoon"],
                data["temperature"]["evening"], data["temperature"]["night"],
                data["humidity"]["afternoon"], data["precipitation"]["total"],
                data["wind"]["max"]["speed"], data["wind"]["max"]["direction"],
                retrieval_date, stored_date
                )
            )

        conn.commit()
        print(f"Updated: {stored_date}")
        api_calls_made += 1
    else:
        print(f"API call failed for {stored_date}: {response.status_code}")

# Close connection
conn.close()

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