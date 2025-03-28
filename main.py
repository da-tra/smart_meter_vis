import requests
import pandas as pd
import pyarrow as pa
from pathlib import Path


# Read the private API key from the non-synced file api_key.txt
with open('api_key.txt', 'r') as f:
    #Store content in a variable:
    api_key = f.read()



# Get data for an API call that collects weather information about a given day
lon = 16.3725042
lat = 48.2083537
date = "2025-03-18"

url = f"https://api.openweathermap.org/data/3.0/onecall/day_summary?lat={lat}537&lon={lon}&date={date}&appid={api_key}"
response = requests.get(url=url)
# print(response.json())

with open("TAGESWERTE-20220325-bis-20250324.csv") as csv_usage:
    usage_data = pd.read_csv(csv_usage, sep=';')

usage_ser = usage_data.iloc[:, 1]
dates_ser = pd.to_datetime(usage_data.iloc[:, 0], format="%d.%m.%y")

start_date = dates_ser.iloc[0]
end_date = dates_ser.iloc[-1]
