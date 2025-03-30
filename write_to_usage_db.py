import sqlite3
from datetime import datetime
import csv

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
# TODO add new columns and data here
cursor.execute("SELECT date FROM usage WHERE retrieval_date IS NULL OR retrieval_date = ''")
rows = cursor.fetchall()

# Step 3: Process each row
for row in rows:
    stored_date = row[0]  # Extract date from query result

    print(f"Fetching data for {stored_date}...")

    # Step 4: Make API call using the stored date
    url = f"https://api.openweathermap.org/data/3.0/onecall/day_summary?lat={LAT}&lon={LON}&date={date}&appid={API_KEY}"
csv_usage.close()

