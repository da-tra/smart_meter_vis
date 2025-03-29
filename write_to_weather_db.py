import requests
import sqlite3
import csv
import pandas as pd
from datetime import datetime, timedelta
from statistics import median

# TODO Make switch for free mode or not
# TODO run max 750 queries (due to limit of 1000 free 
# API calls per day)
# TODO start date is last date from db (if exists) plus one
# TODO if db is empty, get start date from usage db

# API Details
with open("api_key.txt", "r") as f:
    API_KEY = f.read().strip()
LAT, LON = 48.2083537, 16.3725042
#Check if there is something in the weather DB
conn = sqlite3.connect("weather_data.db")
cursor = conn.cursor()
query = "SELECT name FROM sqlite_master"
cursor.execute(query)
results= cursor.fetchall()
print(results)
if not results:
    csv_usage = open("TAGESWERTE-20220325-bis-20250324.csv")
    usage_data = csv.reader(csv_usage, delimiter=";")
    header = next(usage_data)
    row = next(usage_data)
    START_DATE = datetime.strptime(row[0], "%d.%m.%y")
else:
    #TODO turn this into a function
    # get last entry date in weather data
    query = f"SELECT date from WEATHER order by rowid desc LIMIT 1"
    df = pd.read_sql(query, conn)
    if df.empty:
        csv_usage = open("TAGESWERTE-20220325-bis-20250324.csv")
        usage_data = csv.reader(csv_usage, delimiter=";")
        header = next(usage_data)
        row = next(usage_data)
        START_DATE = datetime.strptime(row[0], "%d.%m.%y")
    else:
        START_DATE = datetime.strptime(df.iat[0, 0], "%Y-%m-%d")  + timedelta(days=1)


DAYS = 5  # Fetch data for a week

# Create Table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS weather (
        id INTEGER PRIMARY KEY,
        date TEXT,
        min_temp_k REAL,
        max_temp_k REAL,
        afternoon_temp_k REAL,
        humidity REAL,
        precipitation REAL,
        wind_speed REAL,
        wind_direction REAL,
        retrieval_date TEXT
        )
    """)
conn.commit()

# Fetch and Store Data
for i in range(DAYS):
    date = (START_DATE + timedelta(days=i)).strftime("%Y-%m-%d")
    if date == (datetime.today() - timedelta(days=3)).strftime("%Y-%m-%d"):
        conn.commit()
        break
    url = f"https://api.openweathermap.org/data/3.0/onecall/day_summary?lat={LAT}&lon={LON}&date={date}&appid={API_KEY}"
    response = requests.get(url)
    data = response.json()
    # TODO calculate temp_median here
    # temp_values = []
    # temp_values += data['temperature']['min'], data['temperature']['max'], data['temperature']['afternoon'], data['temperature']['morning'], data['temperature']['night']
    # temp_median = median(temp_values) 
    

    if response.status_code == 200:
        # TODO add column for temp morning, temp night, median temp, temp [C], tz, units, 
        # temp_median: calculate in python and add to table
        # TODO change wind column name to wind_max_speed and ..._direction
        # TODO change name to temp_min , ...._max
        try:
            cursor.execute("""
                INSERT INTO weather (date, min_temp_k, max_temp_k, afternoon_temp_k, humidity, precipitation, wind_speed, wind_direction, retrieval_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data.get('date'),
                data['temperature']['min'],
                data['temperature']['max'],
                data['temperature']['afternoon'],
                data['humidity']['afternoon'],
                data['precipitation']['total'],
                data['wind']['max']['speed'],
                data['wind']['max']['direction'],
                datetime.today().strftime('%Y-%m-%d')

            ))
            conn.commit()
        except KeyError as e:
            print(f"Missing key {e} in response for date {date}")
    else:
        print(f"Failed to fetch data for {date}")
        print(f"HTTP Response code: {response.status_code}")

# Close Connection
conn.close()