import requests
import sqlite3
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
START_DATE = datetime(2023, 4, 18).date()
DAYS = 100  # Fetch data for a week

# SQLite Setup
conn = sqlite3.connect("weather_data.db")
cursor = conn.cursor()

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