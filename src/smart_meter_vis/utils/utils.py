import os
from importlib.resources import files
import csv
from datetime import datetime

def build_columns_string(columns_dict):
    """Return a string for specifying SQL columns and their types.

    Input a dictionary that defines {"column_name": "TYPE"} pairs.
    Output format: column_name1 TYPE1, column_name2 TYPE2.\n
    Example Input: 
        d = { 
            "id": "INTEGER PRIMARY KEY",
            "date": "TEXT UNIQUE",
            "observation": "REAL",
            }
    Example output:
        id INTEGER PRIMARY KEY,
        date TEXT UNIQUE,
        observation REAL
    """
    return ",\n    ".join(f"{key} {value}" for key, value in columns_dict.items())

def find_csv_filepaths(path_to_dir: str) -> list[str]:
    # Get all filenames in directory
    filenames = os.listdir(path_to_dir)
    # Keep only CSV filenames
    suffix = ".csv"
    filenames = [filename for filename in filenames if filename.endswith(suffix)]
    # Get absolute paths
    filepaths = [f"{path_to_dir}/{filename}" for filename in filenames]
    return filepaths

def load_csv_meter_data(filepaths: list[str]):
    # Define dictionary to store data loaded from CSV 
    smart_meter_dict = {}
    # Process all present CSV files:
    for filepath in filepaths:
        with open(filepath, mode="r", encoding="utf-8") as f:  # noqa: PTH123
            # Read the individual CSV file
            usage_data = csv.reader(f, delimiter=";")
            # Load the header row and advance to next row
            header = next(usage_data)
            for row in usage_data:
                # print(row)
                # Get date from first column and format to YYYY-MM-DD
                date_csv = row[0]
                date_csv = datetime.strptime(date_csv, "%d.%m.%Y")
                date_csv = date_csv.strftime("%Y-%m-%d")

                # Get power consumption from second column
                usage = row[1]

                # If the current row of the usage csv is empty:
                #   i) report missing data
                #   ii) leave loop, work to next row
                if not usage:
                    print(f"Skipped: {date_csv} (No data)")  # noqa: T201
                    continue
                # Otherwise: reformat usage data from 1,12 to 1.23
                else:
                    usage = float(usage.replace(",", "."))
                
                smart_meter_dict[date_csv] = usage
                # print(date_csv, ": ", usage)

    return smart_meter_dict