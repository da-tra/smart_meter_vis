import sqlite3
import requests
from datetime import datetime
import csv
from statistics import median

# API Details
with open("api_key.txt", "r") as f:
    API_KEY = f.read().strip()
LAT, LON = 48.2083537, 16.3725042

# Set API call limit
API_CALL_LIMIT = 750  # Change this number as needed

# Create database connection
conn = sqlite3.connect("usage_data.db")
cursor = conn.cursor()

# Create table if not exists
cursor.execute("""
    CREATE TABLE IF NOT EXISTS usage (
        id INTEGER PRIMARY KEY,
        date TEXT UNIQUE,
        usage_kwh REAL
    )
""")
conn.commit()

# Read and insert data from CSV
csv_usage = open("TAGESWERTE-20220331-bis-20250330.csv")
usage_data = csv.reader(csv_usage, delimiter=";")
header = next(usage_data)

for row in usage_data:
    date = datetime.strptime(row[0], "%d.%m.%Y").strftime("%Y-%m-%d")
    usage = row[1]

    if usage:
        usage = float(usage.replace(",", "."))
        print(f"Stored in DB: {date}: {usage} kWh")
    else:
        print(f"Skipped: {date} (No data)")
        continue

    cursor.execute("""
        INSERT INTO usage (date, usage_kwh)
        VALUES (?, ?)
    """, (date, usage))

conn.commit()

# **Step 1: Add new weather-related columns (only if they don't exist)**
columns = [
    "min_temp_k", "max_temp_k", "temp_median_no_minmax_k", "median_temp_k",
    "morning_temp_k", "afternoon_temp_k", "evening_temp_k", "night_temp_k",
    "humidity", "precipitation", "wind_speed", "wind_direction", "retrieval_date"
]

for column in columns:
    try:
        cursor.execute(f"ALTER TABLE usage ADD COLUMN {column} REAL")
    except sqlite3.OperationalError:
        pass  # Column already exists

conn.commit()

# **Step 2: Fetch missing data from API**
cursor.execute("SELECT date FROM usage WHERE retrieval_date IS NULL OR retrieval_date = '' LIMIT ?", (API_CALL_LIMIT,))
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
            UPDATE usage
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
        ))

        conn.commit()
        print(f"Updated: {stored_date}")
        api_calls_made += 1
    else:
        print(f"API call failed for {stored_date}: {response.status_code}")

# Close resources
csv_usage.close()
conn.close()