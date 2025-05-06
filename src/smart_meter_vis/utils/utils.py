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

def find_csv_paths_abs(folder_csv: str) -> list[str]:
    """Get a list of absolute paths for all CSV files in a directory.

    Args:
        folder_csv (str): The path to the directory containing the CSV files.

    Returns:
        list[str]: A list of absolute file paths for the CSV files.
    """
    # Get all filenames in directory
    filenames = os.listdir(folder_csv)
    # Keep only CSV filenames
    suffix = ".csv"
    filenames = [filename for filename in filenames if filename.endswith(suffix)]
    # Get absolute paths
    paths_abs_list = [f"{folder_csv}/{filename}" for filename in filenames]
    return paths_abs_list

def load_csv_meter_data(paths_abs_list: list[str]) -> dict[str, dict[str, float | str]]:
    """Load smart meter data from a list of CSV file paths.

    Reads CSV files, extracts date and usage information, and stores it
    in a dictionary. Dates are formatted to 'YYYY-MM-DD', and usage is
    converted to a float. Skips rows with missing usage data.

    Args:
        paths_abs_list (list[str]): A list of absolute paths to the CSV files.

    Returns:
        dict[str, dict[str, float | str]]: A dictionary where keys are dates
        ('YYYY-MM-DD') and values are dictionaries containing the 'date' and
        'usage_kwh'.
    """
    # Define dictionary to store data loaded from CSV
    smart_meter_dict = {}
    # Process all present CSV files:
    for path_abs in paths_abs_list:
        with open(path_abs, mode="r", encoding="utf-8") as f:  # noqa: PTH123
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

                smart_meter_dict[date_csv] = {"date": date_csv, "usage_kwh": usage}
                # print(date_csv, ": ", usage)

    return smart_meter_dict

def check_sql_cell_not_null(
        folder_db: str,
        name_db: str,
        name_table: str,
        label_name: str,
        feature_name: str,
        label: str,
        ) -> bool:
    """Check if a value exists in a specific cell of an SQL table.

    Queries the SQL table to see if there is any entry in the specified
    'feature_name' column for the given 'label' in the 'label_name' column.

    Args:
        folder_db (str): The path to the directory containing the database file.
        name_db (str): The name of the SQLite database file.
        name_table (str): The name of the table to query.
        label_name (str): The name of the column containing the label.
        feature_name (str): The name of the column to check for a non-null value.
        label (str): The value of the label to search for.

    Returns:
        bool: True if a non-null value exists for the given label and feature,
              False otherwise.
    """
    path_abs_db = f"{folder_db}/{name_db}"
    conn = sqlite3.connect(path_abs_db)
    cursor = conn.cursor()

    query = f"SELECT {feature_name} FROM {name_table} WHERE {label_name} = ?"
    cursor.execute(query, (label, ))
    row_sql = cursor.fetchone()
    if not row_sql:
        # print(f"{label_name} {label}: no entry for field '{feature_name}'")
        return False
    elif row_sql:
        # print(f"{feature_name} kWh on {label}: (Already in database)")
        return True

def sql_get_column_as_list(
        folder_db: str,
        name_db: str,
        name_table: str,
        column_name: str,
        ) -> list[str | int | float]:
    """Fetch all values from a specified column in an SQL table as a list.

    Args:
        folder_db (str): The path to the directory containing the database file.
        name_db (str): The name of the SQLite database file.
        name_table (str): The name of the table to query.
        column_name (str): The name of the column to retrieve.

    Returns:
        list[str | int | float]: A list containing all the values from the specified column.
                                 The data type of the elements in the list will match
                                 the data type of the column in the database.
    """
    path_abs_db = f"{folder_db}/{name_db}"
    conn = sqlite3.connect(path_abs_db)
    cursor = conn.cursor()

    query = f"SELECT {column_name} FROM {name_table}"
    cursor.execute(query)
    result = cursor.fetchall()
    rows_as_list = [row[0] for row in result]
    # for row in rows:
    #     print(row)
    return rows_as_list


def sql_filter_where(
        folder_db: str,
        name_db: str,
        name_table: str,
        filter_col_and_value: dict[str, str | int | float],
        columns_select_list: list[str],
        ) -> list:
    """Filter an SQL table based on specified conditions in the WHERE clause
    and select specific columns.

    Args:
        folder_db (str): The path to the directory containing the database file.
        name_db (str): The name of the SQLite database file.
        name_table (str): The name of the table to query.
        filter_col_and_value (dict[str, str | int | float]): A dictionary where keys
            are column names for filtering and values are the corresponding filter values.
            Multiple key-value pairs will be combined with 'AND' in the WHERE clause.
        columns_select_list (list[str]): A list of column names to select from the table.
            If a string is provided, it will be treated as a single-element list.

    Returns:
        list: A list of tuples, where each tuple represents a row that matches the
              filter conditions and contains the selected columns.
    """

    path_abs_db = f"{folder_db}/{name_db}"
    conn = sqlite3.connect(path_abs_db)
    cursor = conn.cursor()

    if type(columns_select_list) is str:
        columns_select_list = [columns_select_list]

    columns_where_str = " AND ".join(f"{col_name} = ?" for col_name, value in filter_col_and_value.items())
    values_filter = tuple(filter_col_and_value.values())

    print(f"values filter: {values_filter}")
    query = f"SELECT {", ".join(columns_select_list)} FROM {name_table} WHERE {columns_where_str}"
    # print(query, values_filter)
    cursor.execute(query, values_filter)
    result = cursor.fetchall()
    return result

def sql_filter_is_none(
        folder_db: str,
        name_db: str,
        name_table: str,
        columns_is_none: dict[str, str | int | float],
        columns_select_list: list[str],
        ) -> list:
    """Filter an SQL table to find rows where specified columns are NULL
    and select specific columns.

    Args:
        folder_db (str): The path to the directory containing the database file.
        name_db (str): The name of the SQLite database file.
        name_table (str): The name of the table to query.
        columns_is_none (dict[str, str | int | float]): A dictionary where keys are
            column names to check for NULL values. The values in the dictionary
            are not used but are included for consistency with other filter functions.
            Multiple column names will be combined with 'AND' in the WHERE clause.
        columns_select_list (list[str]): A list of column names to select from the table.
            If a string is provided, it will be treated as a single-element list.

    Returns:
        list: A list of tuples, where each tuple represents a row where the
              specified columns are NULL and contains the selected columns.
    """
    path_abs_db = f"{folder_db}/{name_db}"
    conn = sqlite3.connect(path_abs_db)
    cursor = conn.cursor()

    if type(columns_select_list) is str:
        columns_select_list = [columns_select_list]

    columns_is_none_str = " AND ".join(f"{col_name} IS NULL" for col_name in columns_is_none.keys())

    query = f"SELECT {", ".join(columns_select_list)} FROM {name_table} WHERE {columns_is_none_str}"
    print(query)
    cursor.execute(query)
    result = cursor.fetchall()
    return result

def sql_count_value_in_column(
        folder_db: str,
        name_db: str,
        name_table: str,
        count_value: str,
        column_name: str,
        ) -> int | None:
    """Count the number of occurrences of a specific value in a given column of an SQL table.

    Args:
        folder_db (str): The path to the directory containing the database file.
        name_db (str): The name of the SQLite database file.
        name_table (str): The name of the table to query.
        count_value (str): The value to count in the specified column.
        column_name (str): The name of the column to search within.

    Returns:
        int | None: The number of times the `count_value` appears in the
                    `column_name`. Returns 0 (instead of None) if the value is not found. Returns None if there's an issue executing the query.
    """
    path_abs_db = f"{folder_db}/{name_db}"
    conn = sqlite3.connect(path_abs_db)
    cursor = conn.cursor()

    query = f"""SELECT COUNT (*) FROM {name_table}
                    WHERE {column_name} = ?
                    GROUP BY {column_name}
                    ORDER BY {column_name}"""
    cursor.execute(query, (count_value, ))
    row = cursor.fetchone()
    if row is None:
        return 0
    return row[0] # fetch without row content

def create_sql_table(folder_db: str, name_db: str, name_table: str, columns_name_type: dict[str, str]) -> None:
    """Connect to SQLite3 file and CREATE TABLE IF NOT EXISTS.

    Args:
        folder_db (str): The path to the directory where the database file will be located.
        name_db (str): The name of the SQLite database file.
        name_table (str): The name of the table to create.
        columns_name_type (dict[str, str]): A dictionary defining the columns and their SQL data types,
                                            e.g., {"column_name": "TEXT", "value": "REAL"}.
    """
    path_abs_db = f"{folder_db}/{name_db}"
    conn = sqlite3.connect(path_abs_db)
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
    conn.close()

def add_new_columns(
        folder_db: str,
        name_db: str,
        name_table: str,
        columns: dict[str, str],
        ) -> None:
    """Add new columns to an existing SQL table.

    Args:
        folder_db (str): The path to the directory containing the database file.
        name_db (str): The name of the SQLite database file.
        name_table (str): The name of the table to modify.
        columns (dict[str, str]): A dictionary where keys are the new column names
                                   and values are their corresponding SQL data types,
                                   e.g., {"new_column": "INTEGER", "another_field": "TEXT"}.
                                   If a column already exists, it will be skipped without error.
    """
    path_abs_db = f"{folder_db}/{name_db}"
    conn = sqlite3.connect(path_abs_db)
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
    conn.close()

def sql_insert_row(
        folder_db: str,
        name_db: str,
        name_table: str,
        data: dict,
        ) -> None:
    """
    Insert n values (dict values) into n columns (dict keys) in SQL table.
    column_one, column_two, ....., column_n
    data =  {
            column_name_one: value_one,
            column_name_two: value_two,
            ....
            column_name_n: value_n,
            }
    
    INSERT INTO table (column_one, column_two) VALUES (?, ?)
    """
    path_db = f"{folder_db}/{name_db}"
    conn = sqlite3.connect(path_db)
    cursor = conn.cursor()

    values = tuple(data.values())
    column_names_str = ", ".join([key for key in data.keys()])
    placeholder_str = ", ".join(["?" for _ in range(len(data))])

    query = f"INSERT INTO {name_table} ({column_names_str}) VALUES ({placeholder_str})"
    # print(f"{query}{values}")
    cursor.execute(query, values) 
    conn.commit()




def store_in_sql(
        folder_db: str,
        name_db: str,
        name_table: str,
        data: dict,
        column_names: dict[str, str | list[str]],
        ) -> None:
    """Connect to SQLite3 file and store data from dictionary.
    
    Stores observation data from a dictionary.
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
    print("Press any other key to stop making API calls.")
    choice = input()
    if choice != "":
        return False
    elif choice == "":
        return True

