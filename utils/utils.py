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
print(build_columns_string({
        "id": "INTEGER PRIMARY KEY",
        "date": "TEXT UNIQUE",
        "usage_kwh": "REAL",
        }))