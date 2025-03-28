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
    date = f"20{date[-2:]}-{date[3:5]}-{date[0:2]}"
    usage = row[1]
    cursor.execute("""
        INSERT INTO usage (date, usage_kwh)
        VALUES (?, ?)
    """, (
        date,
        usage
    ))
conn.commit()

csv_usage.close()