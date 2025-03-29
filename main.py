import requests
import sqlite3
import pyarrow as pa
from pathlib import Path
from pprint import pprint


# join databases

# Connect to the first database (weather data)
conn = sqlite3.connect("weather_data.db")
cursor = conn.cursor()

# Attach the second database (usage data)
cursor.execute("ATTACH DATABASE 'usage_data.db' AS usage_db")

# TODO determine which join type is best for maintaining scale visually
# I suspect it would be an outer join
# SQLite does this by combining a left and right join and
# removing duplicates. https://stackoverflow.com/questions/4796872/how-can-i-do-a-full-outer-join-in-mysql
 
# Perform INNER JOIN on date column
query = """
    SELECT w.date, w.afternoon_temp_k, u.usage_kwh
    FROM weather w
    INNER JOIN usage_db.usage u
    ON w.date = u.date
"""

cursor.execute(query)

# Fetch and print results
results = cursor.fetchall()
for row in results:
    print(row)


# Connect to the new database (joined data)
conn_new = sqlite3.connect("joined_data.db")
cursor_new = conn_new.cursor()

# Create table in the new database
cursor_new.execute("""
    CREATE TABLE IF NOT EXISTS joined_usage_weather (
        id INTEGER PRIMARY KEY,
        date TEXT,
        afternoon_temp_k REAL,
        usage_kwh REAL
    )
""")
conn_new.commit()

# Insert the joined data into the new table
cursor_new.executemany("""
    INSERT INTO joined_usage_weather (date, afternoon_temp_k, usage_kwh)
    VALUES (?, ?, ?)
""", results)

# Commit and close connections
conn_new.commit()
conn_new.close()
conn.close()

print("Joined data has been successfully saved to 'joined_data.db'.")
