import sqlite3
from datetime import datetime
import csv

# TODO determine start date somehow, eliminate lines without data
# TODO
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
    # Fetch data and convert from comma ...
    usage = row[1]
    # print(f"pre-con: {type(date)}: {type(usage)}")
    # print(f"pre-con: {date}: {usage}")
    # print(f"type before treatment {type(usage)}")
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


csv_usage.close()

