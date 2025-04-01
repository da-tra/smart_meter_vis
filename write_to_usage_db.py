import sqlite3
import requests
from datetime import datetime
import csv
from statistics import median




# Read CSV with usage data (generated via customer profile at https://smartmeter-web.wienernetze.at/ ) load the data with the module csv

# TODO everything using a with statement for opening the csv
# TODO generalise this section to include any .csv file in the specified folder
filename_csv = "TAGESWERTE-20220325-bis-20250324.csv"
filepath_csv = f"./smart_meter_data/{filename_csv}"
csv_usage = open(filepath_csv)
usage_data = csv.reader(csv_usage, delimiter=";")
header = next(usage_data)

# The next section stores usage data in an sql table 
# Define name of db file
filename_db = "power_usage_vs_weather.db"
filepath_db = f"./db/{filename_db}"
# Create database connection
conn = sqlite3.connect(filepath_db)
cursor = conn.cursor()

# Define table name
table_name = "power_usage_vs_weather"

# Construct the SQL query using .format() if it doesn't exist
query = """
    CREATE TABLE IF NOT EXISTS {} (
        id INTEGER PRIMARY KEY,
        date TEXT UNIQUE,
        usage_kwh REAL
    )
    """.format(table_name)
cursor.execute(query)

conn.commit()

# Write usage data from CSV object to SQL table
for row in usage_data:
    # Get date from first column 
    date_csv = row[0]
    date_csv = datetime.strptime(date_csv, "%d.%m.%y")
    date_csv = date_csv.strftime("%Y-%m-%d")

    # Get power consumption from second column
    usage = row[1]

    # TODO change from printing to logging:

    # If there is data the csv:
    #   1) change format from 1,12 to 1.23
    #   2) print successful storage
    # Otherwise:
    #   i) report missing data
    #   ii) Leave loop, work to next row

    if usage:
        usage = float(usage.replace(",", "."))
        print(f"Stored in DB: {date_csv}: {usage} kWh")
    else:
        print(f"Skipped: {date_csv} (No data)")
        continue

    # (ctd.) If there is data in the csv:
    #   3) check if the same data already exists in the sql table
    query = f"SELECT usage_kwh FROM {table_name} WHERE date = ?"
    cursor.execute(query, (date_csv,))

    rows_sql = cursor.fetchone()

    if not rows_sql:
        print(f"No data in {filepath_db}:{table_name} for {date_csv}")
        # add the new data!
    else:
        for row_sql in rows_sql:
            print(f"{usage} on {date_csv}: {row} (Already in database)")
        # restart loop
        continue
    #   4) write row sql table using .format()
    query = """
        INSERT INTO {} (date, usage_kwh)
        VALUES (?, ?)
    """.format(table_name)  # Use format only for table name

    cursor.execute(query, (date_csv, usage))  # Pass values safely
conn.commit()
conn.close()


# The next section 
# 1) Defines details for an API GET call to Openweathermap.org
# 2) Adds new columns for the expected data shape to the SQL table power_usage_vs_weather

# API Details: Get API key for weather app
with open("api_key.txt", "r") as f:
    API_KEY = f.read().strip()
# Set longitude and latitude (for API call) to Vienna, AT
LAT, LON = 48.2083537, 16.3725042

# Limit costs by limiting API call  
API_CALL_LIMIT = 900  # Change this number as needed

# Define new columns with their respective types
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

# Define new column names for weather data and 
# add new weather-related columns to the table (only if they don't exist)

conn = sqlite3.connect(filepath_db)
cursor = conn.cursor()
for column, col_type in columns.items():
    sql_query = f"ALTER TABLE {table_name} ADD COLUMN {column} {col_type}"
    print(f"query: {sql_query}")
    try:
        cursor.execute(sql_query)
    except sqlite3.OperationalError:
        print(f"operational error: {column} already exists")
        # Column already exists

# Commit changes and close connection
conn.commit()

# conn = sqlite3.connect(filepath_db)
# cursor = conn.cursor()

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

# Close resources
csv_usage.close()
conn.close()