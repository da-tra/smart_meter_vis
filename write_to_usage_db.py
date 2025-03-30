import sqlite3
from datetime import datetime
import csv
from statistics import median


# API Details
with open("api_key.txt", "r") as f:
    API_KEY = f.read().strip()
LAT, LON = 48.2083537, 16.3725042

# create table
conn = sqlite3.connect("usage_data.db")
cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS usage (
        id INTEGER PRIMARY KEY,
        date TEXT,
        usage_kwh REAL
        )
    """)
conn.commit()

# write data to table

csv_usage = open("TAGESWERTE-20220331-bis-20250330.csv")
usage_data = csv.reader(csv_usage, delimiter=";")
header = next(usage_data)

for row in usage_data:
    date = row[0]
    date = datetime.strptime(date, "%d.%m.%Y").strftime("%Y-%m-%d")
    usage = row[1]
    if usage:
        usage = float(usage.replace(",", "."))
        print(f"stored in db: {date}: {usage}kWh")
    else:
        print(f"skipped: {date} (No data)")
        continue

    # print(f"type after treatment {type(usage)}")

    cursor.execute("""
        INSERT INTO usage (date, usage_kwh)
        VALUES (?, ?)
    """, (
        date,
        usage
    ))
    # Fetch and print results
    
    # print(row[1])

conn.commit()


# TODO add new cweather olumns and data here
cursor.execute("""
    ALTER TABLE usage
        ADD     min_temp_k REAL,
                max_temp_k REAL,
                temp_median_no_minmax_k REAL,
                median_temp_k REAL,
                morning_temp_k REAL,
                afternoon_temp_k REAL,
                evening_temp_k REAL,
                night_temp_k REAL,
                humidity REAL,
                precipitation REAL,
                wind_speed REAL,
                wind_direction REAL,
                retrieval_date TEXT 

    """
    )
conn.commit()

# TODO Make switch for free mode or not
# TODO run max 750 queries (due to limit of 1000 free 

cursor.execute("SELECT date FROM usage WHERE retrieval_date IS NULL OR retrieval_date = ''")
rows = cursor.fetchall()

# Step 3: Process each row
for row in rows:
    stored_date = row[0]  # Extract date from query result

    print(f"Fetching data for {stored_date}...")

    # Step 4: Make API call using the stored date
    api_url = f"https://api.openweathermap.org/data/3.0/onecall/day_summary?lat={LAT}&lon={LON}&date={date}&appid={API_KEY}"
    response = requests.get(api_url)

    if response.status_code == 200:
        data = response.json()  # Parse API response

        temp_values_no_minmax =[
            data["temperature"]["morning"], 
            data["temperature"]["afternoon"],
            data["temperature"]["evening"],
            data["temperature"]["night"],
            ]

        temp_median_no_minmax = median(temp_values_no_minmax)
        
        temp_values = data["temperature"].values()
        temp_median = median(temp_values)
        retrieval_date = datetime.today().strftime('%Y-%m-%d')

        # Step 6: Update the existing row in the `usage` table
        cursor.execute("""
            ALTER TABLE usage
                ADD     min_temp_k REAL,
                        max_temp_k REAL,
                        temp_median_no_minmax_k REAL,
                        median_temp_k REAL,
                        morning_temp_k REAL,
                        afternoon_temp_k REAL,
                        evening_temp_k REAL,
                        night_temp_k REAL,
                        humidity REAL,
                        precipitation REAL,
                        wind_speed REAL,
                        wind_direction REAL,
                        retrieval_date TEXT 

            """
    
csv_usage.close()

