import os
from importlib.resources import files
import csv
from datetime import datetime
import sqlite3

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

def find_csv_filepaths(folder_csv: str) -> list[str]:
    # Get all filenames in directory
    filenames = os.listdir(folder_csv)
    # Keep only CSV filenames
    suffix = ".csv"
    filenames = [filename for filename in filenames if filename.endswith(suffix)]
    # Get absolute paths
    filepaths = [f"{folder_csv}/{filename}" for filename in filenames]
    return filepaths

def load_csv_meter_data(filepaths: list[str]) -> dict:
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

def check_sql_for_value(folder_db, name_db, name_table, label_name, feature_name, label):
    filepath = f"{folder_db}/{name_db}"
    conn = sqlite3.connect(filepath)
    cursor = conn.cursor()

    query = f"SELECT {feature_name} FROM {name_table} WHERE {label_name} = ?"
    cursor.execute(query, (label, ))
    row_sql = cursor.fetchone()
    if not row_sql:
        return False
    elif row_sql:
        return True

def create_sql_table(folder_db, name_db, name_table, columns_name_type) -> None:
    """Connect to SQLite3 file and CREATE TABLE IF NOT EXIST"""
    filepath = f"{folder_db}/{name_db}"
    conn = sqlite3.connect(filepath)
    cursor = conn.cursor()

    # Construct the SQL query for creating a table if it doesn't exist
    columns_block = build_columns_string(columns_name_type)

    query = f"""
        CREATE TABLE IF NOT EXISTS {name_table} (
            {columns_block}
        )
        """
    # Execute the query and commit the results to the database
    cursor.execute(query)
    conn.commit()
    conn.close


def store_in_sql(
        path_db: str,
        name_db: str,
        name_table: str,
        data: dict,
        column_names: dict[str, str | list[str]],
        ) -> None:
    """Connect to SQLite3 file store data from dictionary.
    
    Stores usage data only if no data exists for that day, yet.
    """
    # Store all new values from data in SQL table
    # Connect to db
    filepath = f"{path_db}/{name_db}"
    conn = sqlite3.connect(filepath)
    cursor = conn.cursor()


    # Iterate over all dates and data points in usage dictionary
    for date in data.keys():
        # Name values from usage dictionary
        usage = data[date]

        # TODO make new_function that removes duplicate values from dict
        # Skip date if the SQL table alredy has data for it
        value_exists = check_sql_for_value(
            folder_db=filepath,
            name_db=name_db,
            name_table=name_table,
            label_name=date,
            feature_name=usage,
            )
            
        if value_exists:
            # If the data already exists: end this loop iteration early
            print(f"{usage} kWh on {date}: (Already in database)")
            continue
        else:
             # If there is no content, add the new data
            print(f"Adding usage data for {date} to {filepath}:{name_table}")
            

        #   Write row to sql table
        label = column_names["label"]
        observations = column_names["observations"]
        observation_names = ", ".join(observations)
 
        
        columns = ", ".join(label, observation_names)
        query = f"""
                INSERT INTO {name_table} ({columns})
                VALUES (?, ?)
                """  # noqa: S608
        cursor.execute(query, (date, usage))

    conn.commit()
    conn.close()


