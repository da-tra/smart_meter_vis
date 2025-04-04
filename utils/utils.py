def build_columns_string(columns_dict):
    return ",\n    ".join(f"{key} {value}" for key, value in columns_dict.items())