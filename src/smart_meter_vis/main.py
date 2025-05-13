"""Generate a visualisation of your smart meter data in .csv format.

The visualisation includes graphs for weather data for Vienna.
"""
import csv
import json
import sqlite3
import pandas as pd
from datetime import datetime
from statistics import median
from pprint import pprint

import plotly.graph_objects as go
import requests
from importlib.resources import files
import os
from smart_meter_vis.utils import utils

#################
#  Definitions  #
#################

#  Constants for API call to get weather data 

# Get API key for weather app.
# The API key is to be stored at top level, i.e. smarter_meter_vis/api_key.txt
with open("api_key.txt", "r") as f:  # noqa: PTH123
    API_KEY = f.read().strip()

# Set longitude and latitude to Vienna, AT
LAT, LON = 48.2083537, 16.3725042

# Limit number API calls per day to limit API costs
API_DAILY_LIMIT = 1000

# Define the number of days for which data is requested
API_GET_LIMIT = 10
  # Change this number as needed

# Turn off cost protection by setting limit_costs to False
LIMIT_COSTS = True

## Failsafe for limitting costs:
# if LIMIT_COSTS:
#     assert API_GET_LIMIT <= API_DAILY_LIMIT  # noqa: S101

############################
# Read CSV with usage data #
############################

# Data generated via customer profile at https://smartmeter-web.wienernetze.at/ ) 

# Generate absolute file path for directory containing 
# CSV files with smart meter data
csv_folder = files("smart_meter_vis.meter_data")

# For all csv files: generate absolute file path
filepaths = utils.find_csv_paths_abs(
    folder_csv=csv_folder,
    )
# print(filepaths)

# For all filepaths to csv files: collect contained smart meter data in dictionary
smart_meter_data_dict = utils.load_csv_meter_data(
    paths_abs_list=filepaths,

    )
# pprint(smart_meter_data)

##################################################
# Storing electricity usage data in SQL database #
##################################################

# Store usage data in an SQL table
# Generate absolute file path for directory containing SQL database
sql_folder = files("smart_meter_vis.db")
# Define name of database file
filename_db = "vienna_weather_and_electricity_testwo.db"
# Define table name
table_name_electricity = "electricity"
# Define the column names and their types in a dictionary ("column": "TYPE")
columns_usage = {
    "usage_date": "TEXT UNIQUE",
    "usage_kwh": "REAL",
    }

# Create a table for electricity usage (if it doesn't exist yet)
utils.create_sql_table(
    folder_db=sql_folder,
    name_db=filename_db,
    name_table=table_name_electricity,
    columns_name_type=columns_usage,
    )


# Prevent duplicates in SQL DB: check usage dict for entries already in SQL DB

dict_only_new_usage_data = {}
for key in smart_meter_data_dict:
    # Access the dictionary containing data for a specific date (the key)
    data = smart_meter_data_dict[key]
    # Get the keys (column labels) from the inner dictionary
    column_labels = data.keys()
    # Get the values from the inner dictionary
    values = data.values()
    # Assuming the values are ordered as date and then usage, unpack them
    date, usage = values
    # Assuming the labels are ordered as 'date' and then 'usage_kwh', unpack them
    label_name, feature_name = column_labels
    # Check if data for this date is already stored in the SQL database
    already_stored = utils.check_sql_cell_not_null(
        folder_db=sql_folder,
        name_db=filename_db,
        name_table=table_name_electricity,
        label_name=label_name,  # The column name for the date
        feature_name=feature_name,  # The column name for the usage value
        label=date,  # The specific date to check
        )
    # If the data is not already stored in the database
    if not already_stored:
        # Add this new usage data to the dictionary of only new data
        dict_only_new_usage_data[key] = data

# Drop duplicate data
smart_meter_data_dict = dict_only_new_usage_data


# Write usage data from dict to SQL table

for entry in smart_meter_data_dict.items():
    # Unpack the key-value pair from the dictionary item.
    # 'outer_dict' will be the key (which is the date string).
    # 'inner_dict' will be the value (which is the dictionary containing 'usage_date' and 'usage_kwh').
    outer_dict, inner_dict = entry
    # print(inner_dict)
    # Use the utility function to insert a new row into the SQL table.
    utils.sql_insert_row(
        folder_db=sql_folder,  # The folder where the SQLite database is located
        name_db=filename_db,  # The name of the SQLite database file
        name_table=table_name_electricity,  # The name of the table to insert data into
        data=inner_dict,  # The dictionary containing the column names as keys and the values to insert
        )


###############################################
# Extend SQL database to receive weather data #
###############################################

# Connect to SQL database and define a table name for weather data

path_db = f"{sql_folder}/{filename_db}"
conn = sqlite3.connect(path_db)
cursor = conn.cursor()
table_name_weather = "weather"


# Define new columns with their respective types. These are defined by the API response
# For JSON schema see API documentation: https://openweathermap.org/api/one-call-3#hist_agr_parameter
columns_weather_data = {
    "weather_date": "TEXT",
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

# Create a table for weather data (if it doesn't exist yet)
utils.create_sql_table(
    folder_db=sql_folder,
    name_db=filename_db,
    name_table=table_name_weather,
    columns_name_type=columns_weather_data,
    )

#################################
# Retrieve weather data via API #
#################################

api_calls_made = 0  # Track number of API calls

api_call_count_today = utils.sql_count_value_in_column(
    folder_db=sql_folder,
    name_db=filename_db,
    name_table=table_name_weather,
    count_value=datetime.today().strftime("%Y-%m-%d"),
    column_name="retrieval_date"
    )


# Check if making API calls is allowed (or if it would exceed limits set by user)
if LIMIT_COSTS == False: # don"t limit costs
    make_api_calls = True
elif LIMIT_COSTS == True: # Limit costs
    if api_call_count_today + API_GET_LIMIT < API_DAILY_LIMIT: # Set number of calls is fine
        make_api_calls = True

    # If API_DAILY_LIMIT would be exceeded: ask user if they want to continue regardless
    else: # Set number of calls is fine
        accept_charges = utils.user_choice_api_call(
            performed_calls=api_call_count_today,
            limit=API_DAILY_LIMIT,
            )
        if accept_charges == True: # User has overridden the cost limitation
            make_api_calls = True
        elif accept_charges == False: # User respects cost limitation
            print("Stopping API calls to avoid charges.")
            make_api_calls = False

# Determine dates for which the SQL weather table contains no data, yet.
comparison_data = {
    "reference_table": "electricity",
    "incomplete_table": "weather",
    "reference_column": "usage_date",
    "incomplete_column": "weather_date",
    }

missing_dates = utils.sql_subtract_column_values(
    folder_db=sql_folder,
    name_db=filename_db,
    data=comparison_data,
    )
# print(missing_dates)

# A dictionary for collecting raw JSON data
api_get_results_aggreg = {}

# TODO: fix problem with median: sql table show same value vor all rows of a call series
# Perform API calls if not forbidden. NUmber of API calls limitted to API_GET_LIMIT
if make_api_calls == True:
    if API_GET_LIMIT > len(missing_dates): # Avoid index error
        print("The number of planned API calls exceeds the number of dates in usage data.")
    else:
        for _ in range(API_GET_LIMIT):
            # The dates for which data is requested are defined in the list missing_dates
            next_date = missing_dates[_]

            print(f"API calls made in this run: {api_calls_made}")
            print(f"API calls made today: {api_call_count_today + api_calls_made}")
            print(f"API call daily limit: {API_DAILY_LIMIT}")
            print(f"Fetching data via api for {next_date}")

            response = utils.api_get(
                url="https://api.openweathermap.org/data/3.0/onecall/day_summary",
                api_params={
                    "lat": LAT,
                    "lon": LON,
                    "date": next_date,
                    "appid": API_KEY,
                    "units": "metric"
                    },
                )
            
            if response.status_code == 200:
                response_json = response.json()  # Parse JSON
                response_dict = dict(response_json)

                # Backup new JSON responses to file            
                utils.add_to_json_file_if_is_not_key(
                    filepath="api_responses.json",
                    key=next_date,
                    value=response_json)

                # Caclulate temp median for row; without temp min & max, and one with min & max
                temp_values_no_minmax = [
                    response_dict["temperature"]["morning"],
                    response_dict["temperature"]["afternoon"],
                    response_dict["temperature"]["evening"],
                    response_dict["temperature"]["night"],
                    ]
                temp_median_no_minmax = median(temp_values_no_minmax)

                temp_values = [
                    response_dict["temperature"]["min"],
                    response_dict["temperature"]["max"],
                    ]
                temp_values += temp_values_no_minmax
                temp_median = median(temp_values)

                response_dict["temperature"]["median_no_minmax"] = temp_median_no_minmax
                response_dict["temperature"]["median"] = temp_median

                # Get current date to store as retrieval date
                retrieval_date = datetime.today().strftime('%Y-%m-%d')

                # Add fetched data to dict for all data fetched in this loop
                api_get_results_aggreg[next_date] = response_dict

        # Increment counter of api calls made in this run
        api_calls_made += 1


###################################
# Store weather data in SQL table #
###################################

# Insert weather data into weather table
# Prepare data (list of tuples) for SQL executemany in functin sql_insert_mulitple_from_json
weather_data_insert = [
    (
        key,
        value["temperature"]["min"],
        value["temperature"]["max"],
        value["temperature"]["median_no_minmax"],
        value["temperature"]["median"],
        value["temperature"]["morning"],
        value["temperature"]["afternoon"],
        value["temperature"]["evening"],
        value["temperature"]["night"],
        value["humidity"]["afternoon"],
        value["precipitation"]["total"],
        value["wind"]["max"]["speed"],
        value["wind"]["max"]["direction"],
        retrieval_date,
        )
    for key, value in api_get_results_aggreg.items()
]
utils.sql_insert_multiple_from_json_as_list(
    folder_db=sql_folder,
    name_db=filename_db,
    name_table=table_name_weather,
    column_names=columns_weather_data,
    data_to_insert=weather_data_insert,
    )

#########################
# Calculate correlation #
#########################

query_usage = f"SELECT * FROM {table_name_electricity}"
query_weather = f"SELECT * FROM {table_name_weather}"
path_db = f"{sql_folder}/{filename_db}"
conn = sqlite3.connect(path_db)

df_usage = pd.read_sql_query(sql=query_usage, con=conn, parse_dates="usage_date")
df_weather = pd.read_sql_query(sql=query_weather, con=conn, parse_dates="weather_date")

df_merged = pd.merge(left=df_usage, right=df_weather, how="outer", left_on="usage_date",right_on="weather_date")
df_merged_puredata = df_merged.dropna()

target = df_merged_puredata["usage_kwh"]
feature_labels = [
    "temp_min",
    "temp_max",
    # "temp_median_no_minmax",
    # "temp_median",
    "temp_morning",
    "temp_afternoon",
    "temp_evening",
    "temp_night", 
    "humidity",
    "precipitation",
    "wind_speed",
    "wind_direction",
    ]
features = [feature for feature in feature_labels]
correlations_dict = {}
for feature in features:
    correlation = target.corr(other=df_merged_puredata[feature])
    correlations_dict[feature] = correlation
# print(correlations_dict)
df_correlations = pd.DataFrame(
    correlations_dict.items(), 
    columns=["target", "correlation"]
    )
df_correlations["Abs correlation"] = df_correlations["correlation"].abs()
print(df_correlations.sort_values("Abs correlation", ascending=False))

strongest_correlation = df_correlations.sort_values("Abs correlation", ascending=False).iloc[0]["target"]

#################
# Plotting data #
#################


# # TODO plot only the data with relevant correlation
fig = go.Figure([
    go.Scatter(
        x=df_merged_puredata["usage_date"],
        y=df_merged_puredata[strongest_correlation],
        mode="lines",
        name=strongest_correlation,
        ),
    go.Scatter(
        x=df_merged_puredata["usage_date"],
        y=df_merged_puredata["usage_kwh"],
        mode="lines",
        name="Electricity Usage (kWh)",
        yaxis="y2",
        ),  
    ])

# df_merged_puredata

# TODO: define a dictionary holds data for all features. i.e. column name, x, y, mode, name, own y axis?, updlayout: title, title, side,showticklabels,
# TODO: define a function that plots graphs for the features that appear in first n positions of correlation column.
# TODO: make sure there is no double grid in background of graph

# # Configure multiple y-axes
fig.update_layout(
    title="Electricity Usage vs. Weather Conditions",
    xaxis_title="Date",
    yaxis=dict(title="Temperature (C)", side="left"),  # Keep label
    yaxis2=dict(title="Electricity Usage (kWh)", overlaying="y", side="right"),  # Keep label
    # yaxis6=dict(overlaying="y", side="right", showticklabels=False, autorange="reversed")  # Fix for inverted usage
    )
# Show the plot
fig.show()
