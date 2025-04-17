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
            "value": "REAL",
            }
    Example output:
        id INTEGER PRIMARY KEY,
        date TEXT UNIQUE,
        value REAL
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

def load_csv_meter_data(filepaths: list[str]) -> dict[str, float]:
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

def check_sql_for_value(
        folder_db: str,
        name_db: str,
        name_table: str,
        label_name: str,
        feature_name: str,
        label: str,
        ) -> bool:
    """Check if value in column feature_name exists and return bool
    
    If the value exists in the specified SQL table, return True. If the value is not
    found, return False.
    """
    filepath = f"{folder_db}/{name_db}"
    conn = sqlite3.connect(filepath)
    cursor = conn.cursor()

    query = f"SELECT {feature_name} FROM {name_table} WHERE {label_name} = ?"
    cursor.execute(query, (label, ))
    row_sql = cursor.fetchone()
    if not row_sql:
        # print(f"{label_name} {label}: no entry for field '{feature_name}'")
        return False
    elif row_sql:
        print(f"{feature_name} kWh on {label}: (Already in database)")
        return True

def sql_get_column_as_list(
        folder_db: str,
        name_db: str,
        name_table: str,
        column_name: str,
        ) -> list[str | int | float]:
    filepath = f"{folder_db}/{name_db}"
    conn = sqlite3.connect(filepath)
    cursor = conn.cursor()

    query = f"SELECT {column_name} FROM {name_table}"
    cursor.execute(query)
    result = cursor.fetchall()
    rows_as_list = [row[0] for row in result]
    # for row in rows:
    #     print(row)
    return rows_as_list

def sql_count_value_in_column(
        folder_db: str,
        name_db: str,
        name_table: str,
        count_value: str,
        column_name: str,
        ) -> int | None:
    filepath = f"{folder_db}/{name_db}"
    conn = sqlite3.connect(filepath)
    cursor = conn.cursor()

    query = f"""SELECT COUNT (*) FROM {name_table}
                    WHERE {column_name} = ?
                    GROUP BY {column_name} 
                    ORDER BY {column_name}"""
    cursor.execute(query, (count_value, ))
    row = cursor.fetchone()
    if row == None:
        return 0
    return row[0] # fetch row content without 

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

def add_new_columns(
        folder_db: str,
        name_db: str,
        name_table: str,
        columns: dict[str],
        ):
    """Add new columns to an SQL table.
    
    Specify the column names and types in a dictionary as follows:
        d = {
            "first_column_name": "FIRST COLUMN TYPE",
            "second_column_type": "SECOND COLUMN TYPE",
            }
    """
    filepath = f"{folder_db}/{name_db}"
    conn = sqlite3.connect(filepath)
    cursor = conn.cursor()

    for column, col_type in columns.items():
        query_add_column = f"ALTER TABLE {name_table} ADD COLUMN {column} {col_type}"

        try:
            cursor.execute(query_add_column)
        except sqlite3.OperationalError:
            # Column already exists
            # print(f"operational error: {column} already exists")
            pass

    # Commit changes
    conn.commit()


def store_in_sql(
        folder_db: str,
        name_db: str,
        name_table: str,
        data: dict,
        column_names: dict[str, str | list[str]],
        ) -> None:
    """Connect to SQLite3 file and store data from dictionary.
    
    Stores observation data only if no data exists for that observation, yet.
    The column_names are to be provided as a dictionary.
    Example: 
    column_names = {'label': 'date',
                    'observations': ['usage_kwh']},

    """
    # Store all new values from data in SQL table
    # Connect to db
    path_db = f"{folder_db}/{name_db}"
    conn = sqlite3.connect(path_db)
    cursor = conn.cursor()


    # Iterate over all dates and data points in usage dictionary
    for date in data.keys():
        # Name values from usage dictionary
        usage = data[date]

        # Skip date if the SQL table alredy has data for it
        value_exists = check_sql_for_value(
            folder_db=folder_db,
            name_db=name_db,
            name_table=name_table,
            label_name="date",
            feature_name="usage_kwh",
            label=date,
            )
            
        if not value_exists:
            # If there is no content, add the new data
            print(f"Adding usage data for {date} to {name_db}: {name_table}")
            

        #   Write row to sql table
        label = column_names["label"]
        observations = column_names["observations"]
        observation_names = ", ".join(observations)
 
        
        columns = f"{label}, {observation_names}"
        query = f"""
                INSERT INTO {name_table} ({columns})
                VALUES (?, ?)
                """  # noqa: S608
        cursor.execute(query, (date, usage))

    conn.commit()
    conn.close()

def user_choice_api_call():
    print("The next API request to OpenWeatherMap will exceed the free tier.")
    print("To continue with the request, press ENTER.")
    print("Press any other key to abort.")
    choice = input()
    if choice != "":
        return False
    elif choice == "":
        return True

