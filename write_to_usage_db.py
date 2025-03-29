import sqlite3
from datetime import datetime
import csv

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

csv_usage = open("TAGESWERTE-20220325-bis-20250324.csv")
usage_data = csv.reader(csv_usage, delimiter=";")
header = next(usage_data)

for row in usage_data:
    date = row[0]
    # TODO refactor the following line so it 
    # uses datetime for formatting
    date = f"20{date[-2:]}-{date[3:5]}-{date[0:2]}"
    # Fetch data and convert from comma ...
    usage = row[1]
    # print(f"pre-con: {type(date)}: {type(usage)}")
    # print(f"pre-con: {date}: {usage}")
    # print(f"type before treatment {type(usage)}")
    if usage:
        usage = float(usage.replace(",", "."))
    print(f"{date}: {usage}")

    print(f"type after treatment {type(usage)}")

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

