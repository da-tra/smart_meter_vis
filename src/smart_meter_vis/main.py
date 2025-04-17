"""Generate a visualisation of your smart meter data in .csv format.

The visualisation includes graphs for weather data for Vienna.
"""
import csv
import sqlite3
from datetime import datetime
from statistics import median
from pprint import pprint

import plotly.graph_objects as go
import requests
from importlib.resources import files
import os
from smart_meter_vis.utils import utils


############################
# Read CSV with usage data #
############################

# Data generated via customer profile at https://smartmeter-web.wienernetze.at/ ) 

# Generate absolute file path for directory containing 
# CSV files with smart meter data
csv_folder = files("smart_meter_vis.meter_data")

# For all csv files: generate absolute file path
filepaths = utils.find_csv_filepaths(
    folder_csv=csv_folder,
    )
# print(filepaths)

# For all filepaths to csv files: collect contained smart meter data in dictionary
smart_meter_data_dict = utils.load_csv_meter_data(
    filepaths=filepaths,
    )
# pprint(smart_meter_data)

##################################################
# Storing electricity usage data in SQL database #
##################################################

# Store usage data in an SQL table
# Generate absolute file path for directory containing SQL database
sql_folder = files("smart_meter_vis.db")
# Define name of database file
filename_db = "vienna_weather_and_electricity.db"
# Define table name
table_name = "weather_and_usage"
# Define the column names and their types in a dictionary ("column": "TYPE")
columns_usage = {
    "id": "INTEGER PRIMARY KEY",
    "date": "TEXT UNIQUE",
    "usage_kwh": "REAL",
    }

# Create a table for electricity usage (if it doesn't exist yet)
utils.create_sql_table(
    folder_db=sql_folder,
    name_db=filename_db,
    name_table=table_name,
    columns_name_type=columns_usage,
    )

# Prevent duplicates in SQL DB: check usage dict for entries already in SQL DB

dict_only_new_usage_data = {}
for key in smart_meter_data_dict:
    value = smart_meter_data_dict[key]
    already_stored = utils.check_sql_for_value(
        folder_db=sql_folder,
        name_db=filename_db,
        name_table=table_name,
        label_name="date",
        feature_name="usage_kwh",
        label=key,
        )
    if not already_stored:
        dict_only_new_usage_data[key] = value

smart_meter_data_dict = dict_only_new_usage_data

# Write usage data from dict to SQL table
utils.store_in_sql(
    folder_db=sql_folder,
    name_db=filename_db,
    name_table=table_name,
    data=smart_meter_data_dict,
    column_names = {"label": "date",
                    "observations": ["usage_kwh"]},
    )

###############################################
# Extend SQL database to receive weather data #
###############################################

# Define new columns with their respective types. These are defined by the API response
# For JSON schema see API documentation: https://openweathermap.org/api/one-call-3#hist_agr_parameter
columns_weather_data = {
    "temp_min": "REAL",
    "temp_max": "REAL",
    "temp_median_no_minmax": "REAL",
    "temp_median": "REAL",
    "temp_morning": "REAL",
    "temp_afternoon": "REAL",
    "temp_evening": "REAL",
    "temp_night": "REAL",
    "humidity": "REAL",
    "precipitation": "REAL",
    "wind_speed": "REAL",
    "wind_direction": "REAL",
    "retrieval_date": "TEXT",
    }

# Add new weather-related columns to the table (if they don't exist)

utils.add_new_columns(
    folder_db=sql_folder,
    name_db=filename_db,
    name_table=table_name,
    columns=columns_weather_data,
    )

#################################
# Retrieve weather data via API #
#################################

##### Parameters for API call to get weather data #####

# Get API key for weather app.
# The API key is to be stored at top level, i.e. smarter_meter_vis/api_key.txt
with open("api_key.txt", "r") as f:  # noqa: PTH123
    API_KEY = f.read().strip()

# Set longitude and latitude to Vienna, AT
LAT, LON = 48.2083537, 16.3725042

# TODO remove this feature
# Define the number of days for which data is requested
api_get_limit = 9  # Change this number as needed

# Limit number API calls per day to limit API costs
API_DAILY_LIMIT = 1000

# Turn off cost protection by setting limit_costs to False
limit_costs = True
if limit_costs:
    assert api_get_limit <= API_DAILY_LIMIT  # noqa: S101

api_call_count_today = utils.sql_count_value_in_column(
    folder_db=sql_folder,
    name_db=filename_db,
    name_table=table_name,
    count_value=datetime.today().strftime("%Y-%m-%d"),
    column_name="retrieval_date"
    )

if api_call_count_today + api_get_limit > API_DAILY_LIMIT:
    if not utils.user_choice_api_call:
        break

# api_calls_made = 0  # Track number of API calls

# ## Get SQL rows without weather data
# sql_no_weather_data = f"SELECT date, retrieval_date FROM {table_name} WHERE retrieval_date IS NULL"
# cursor.execute(sql_no_weather_data)

# cursor.execute(sql_no_weather_data)
# rows = cursor.fetchall()

# for row in rows:
#     print(f"API calls made in this run: {api_calls_made}")
#     print(f"API calls made today: {count_calls_day + api_calls_made}")

#     print(f"API call daily limit: {API_DAILY_LIMIT}")

#     # End fetching when API_DAILY_LIMIT is reached
#     if count_calls_day + api_calls_made >= API_DAILY_LIMIT:
#         print("Daily API call limit reached. Stopping further requests.")
#         break

#     # End fetching when api_get_limit is reached
#     if api_calls_made >= api_get_limit:
#         break

#     stored_date = row[0]
#     print(f"Requesting data for {stored_date}...")

#     # API Call
#     api_url = f"https://api.openweathermap.org/data/3.0/onecall/day_summary?lat={LAT}&lon={LON}&date={stored_date}&appid={API_KEY}"
#     response = requests.get(api_url)

#     if response.status_code == 200:
#         response_json = response.json()  # Parse JSON
#         print(response_json)
#         response_dict = dict(response_json)

#         print(f"response type {type(response_dict)}")
#         print(response_dict)
#         # Caclulate temp median for row; without temp min & max, and one with min & max
#         temp_values_no_minmax = [
#             response_dict["temperature"]["morning"],
#             response_dict["temperature"]["afternoon"],
#             response_dict["temperature"]["evening"],
#             response_dict["temperature"]["night"],
#             ]
#         temp_median_no_minmax = median(temp_values_no_minmax)

#         temp_values = [
#             response_dict["temperature"]["min"],
#             response_dict["temperature"]["max"],
#             ]
#         temp_values += temp_values_no_minmax
#         temp_median = median(temp_values)

#         response_dict["temperature"]["median__temp_no_min_max_k"] = temp_median_no_minmax
#         response_dict["temperature"]["median_temp"] = temp_median

#         # Get current date to store as retrieval date
#         retrieval_date = datetime.today().strftime('%Y-%m-%d')


#         # Update database with API data
#         columns_weather_data = [
#            "min_temp",
#            "max_temp",
#            "median_temp_no_minmax",
#            "median_temp",
#            "morning_temp",
#            "afternoon_temp",
#            "evening_temp",
#            "night_temp",
#            "humidity",
#            "precipitation",
#            "wind_speed",
#            "wind_direction",
#            "retrieval_date",
#            ]


#         # Build the SQL query string dynamically
#         columns_weather_string = ", ".join(f"{col} = ?" for col in columns_weather_data)

#         sql_update_query = f"""
#                             UPDATE {table_name}
#                                 SET {columns_weather_string}
#                                 WHERE date = ?
#                             """

#         # Prepare data (list of tuples) for sqlite3 executemany
#         data_to_update = [
#             (
#                 response_dict["temperature"]["min"],
#                 response_dict["temperature"]["max"],
#                 temp_median_no_minmax,
#                 temp_median,
#                 response_dict["temperature"]["morning"],
#                 response_dict["temperature"]["afternoon"],
#                 response_dict["temperature"]["evening"],
#                 response_dict["temperature"]["night"],
#                 response_dict["humidity"]["afternoon"],
#                 response_dict["precipitation"]["total"],
#                 response_dict["wind"]["max"]["speed"],
#                 response_dict["wind"]["max"]["direction"],
#                 retrieval_date,
#                 stored_date,  # `WHERE date = ?` goes last
#                 )
#             for value in response_dict
#         ]

#         # Execute all updates at once
#         cursor.executemany(sql_update_query, data_to_update)
#         print(f"Updated data for: {stored_date}")
#         print("- - - ")
#         # Increment counter of api calls made in this run
#         api_calls_made += 1
#     else:
#         print(f"API call failed for {stored_date}: {response.status_code}")
# print("All weather data gathered. Waiting to commit do database")
# # Commit changes to database
# conn.commit()
# print("Changes committed to SQL database")
# # Close connection
# conn.close()

###################################
# Store weather data in SQL table #
###################################

# # TODO calculate correlation between weather and usage
# # TODO plot (or show?) only the data with relevant correlation

# # Plot usage data vs weather data

# # Connect to SQL database
# filename_db = "power_usage_vs_weather.db"
# filepath_db = f"./db/{filename_db}"
# # Create database connection
# conn = sqlite3.connect(filepath_db)
# cursor = conn.cursor()

# # Define and fetch data for visualising from the SQL table
# columns = ["date",
#            "min_temp",
#            "max_temp",
#            "median_temp",
#            "morning_temp",
#            "afternoon_temp",
#            "evening_temp",
#            "night_temp",
#            "humidity",
#            "precipitation",
#            "wind_speed",
#            "wind_direction",
#            "usage_kwh",
#            ]

# columns_string = ", ".join(columns)
# sql_data_for_vis = f"""SELECT {columns_string} FROM {table_name}
#                         ORDER BY date"""
# cursor.execute(sql_data_for_vis)

# # Separate data into lists
# dates = []
# temps_min = []
# temps_max = []
# temps_median = []
# temps_morning = []
# temps_afternoon = []
# temps_evening = []
# temps_night = []
# humidity = []
# precipitation = []
# wind_speed = []
# wind_direction = []
# usage = []

# for row in cursor:
#     (date, temp_min, temp_max, temp_median, temp_morning, temp_afternoon,
#      temp_evening, temp_night, hum, precip, wind_spd, wind_dir, use) = row
    
#     dates.append(date)
#     temps_min.append(temp_min)
#     temps_max.append(temp_max)
#     temps_median.append(temp_median)
#     temps_morning.append(temp_morning)
#     temps_afternoon.append(temp_afternoon)
#     temps_evening.append(temp_evening)
#     temps_night.append(temp_night)
#     humidity.append(hum)
#     precipitation.append(precip)
#     wind_speed.append(wind_spd)
#     wind_direction.append(wind_dir)
#     usage.append(use)

# # Close the connection
# conn.close()

# # Create Plotly figure
# fig = go.Figure([
#     go.Scatter(x=dates, y=temps_min, mode="lines", name="Min Temperature"),
#     go.Scatter(x=dates, y=temps_max, mode="lines", name="Max Temperature"),
#     go.Scatter(x=dates, y=temps_median, mode="lines", name="Median Temp"),
#     go.Scatter(x=dates, y=temps_morning, mode="lines", name="Morning Temperature"),
#     go.Scatter(x=dates, y=temps_afternoon, mode="lines", name="Afternoon Temperature"),
#     go.Scatter(x=dates, y=temps_evening, mode="lines", name="Evening Temperature"),
#     go.Scatter(x=dates, y=temps_night, mode="lines", name="Night Temperature"),
#     go.Scatter(x=dates, y=humidity, mode="lines", name="Humidity (%)", yaxis="y3"),
#     go.Scatter(x=dates, y=precipitation, mode="lines", name="Precipitation (mm)", yaxis="y4"),
#     go.Scatter(x=dates, y=wind_speed, mode="lines", name="Wind Speed (m/s)", yaxis="y5"),
#     go.Scatter(x=dates, y=usage, mode="lines", name="Electricity Usage (kWh)", yaxis="y2"),
#     go.Scatter(x=dates, y=usage, mode="lines", name="Usage (Inverted)", yaxis="y6")  # No need to multiply by -1
# ])

# # Configure multiple y-axes
# fig.update_layout(
#     title="Electricity Usage vs. Weather Conditions",
#     xaxis_title="Date",
#     yaxis=dict(title="Temperature (K)", side="left"),  # Keep label
#     yaxis2=dict(title="Electricity Usage (kWh)", overlaying="y", side="right"),  # Keep label
#     yaxis3=dict(overlaying="y", side="left", showticklabels=False),  # Hide label
#     yaxis4=dict(overlaying="y", side="right", showticklabels=False),  # Hide label
#     yaxis5=dict(overlaying="y", side="right", showticklabels=False),  # Hide label
#     yaxis6=dict(overlaying="y", side="right", showticklabels=False, autorange="reversed")  # Fix for inverted usage
# )
# # Show the plot
# fig.show()