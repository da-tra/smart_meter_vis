This project provides a tool to visualize your smart meter data in CSV format, enhanced with weather data for Vienna, Austria. It fetches weather information using the OpenWeatherMap API and integrates it with your electricity usage data to create insightful visualizations.

## Features

* **CSV Data Input:** Reads smart meter data from CSV files (tested only with data by Wiener Netze).
* **Weather Data Integration:** Fetches historical weather data for Vienna based on the dates in your smart meter data. Allows you to limit the number of API calls to avoid exceeding the free tier of the API service.
* **Data Storage:** Utilizes an SQLite database to store both electricity usage and weather data. This helps in efficient data management and prevents redundant API calls.
* **Correlation Analysis:** Calculates the correlation between electricity usage and various weather parameters to identify the most influential weather conditions.
* **Interactive Plotting:** Generates interactive plots using Plotly to visualize electricity usage against the weather parameter with the strongest correlation.
* **Cost Management:** Includes options to limit the number of daily API calls to manage costs associated with the OpenWeatherMap API.

## Getting Started

### Prerequisites

* **Python >= 3.11.:**
* **[uv](https://github.com/astral-sh/uv)**
* **[Requests](https://github.com/psf/requests)**
* **[pandas](https://github.com/pandas-dev/pandas):**
* **OpenWeatherMap API Key:** You need to sign up for a free account at [https://openweathermap.org/](https://openweathermap.org/) to obtain an API key. This key is required to fetch weather data.

### Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/da-tra/smart_meter_vis.git
    cd smart_meter_vis
    ```

2.  **Install required Python packages using `uv`:**
    ```bash
    uv pip sync requirements.txt
    ```
    *(Note: Ensure you have `uv` installed. If not, you can install it using `pip install uv` or following the instructions on the [uv GitHub page](https://github.com/astral-sh/uv).

3.  **Create `api_key.txt`:**
    Create a file named `api_key.txt` in the root directory of the repository and paste your OpenWeatherMap API key into it.

### Usage

1.  **Place your smart meter data CSV files:** Put the CSV files containing your electricity usage data into the `smart_meter_vis/meter_data` directory. The script is designed to find and process all `.csv` files within this folder.

2.  **Run the main script:**
    ```bash
    python smart_meter_vis/main.py
    ```

3.  **View the visualization:** The script will generate an interactive plot (in your default web browser) showing the electricity usage and the weather parameter with the strongest correlation over time.

## Configuration

* **`api_key.txt`:** As mentioned, this file stores your OpenWeatherMap API key.
* **`API_GET_LIMIT`:** You can modify the `API_GET_LIMIT` variable in the script (likely in `smart_meter_vis/main.py` or a similar file) to control the number of days of historical weather data fetched in a single run.
* **`LIMIT_COSTS`:** The `LIMIT_COSTS` variable allows you to enable or disable the daily API call limit to help manage potential costs.
* **Database:** The SQLite database (`vienna_weather_and_electricity_testwo.db`) will be created in the `smart_meter_vis/db` directory.

## Data Format

The script expects your smart meter data CSV files to contain at least a date column and an electricity usage column (in kWh). Ensure the date format in your CSV files is consistent and can be parsed by the script. *(You might want to provide a sample of the expected CSV format here.)*

## Potential Improvements

* **Configuration File:** Instead of hardcoding variables, a separate configuration file (e.g., `config.yaml` or `.env`) could be used for API keys, file paths, and other settings.
* **More Visualization Options:** Allow users to select different weather parameters to plot against electricity usage. Or the n most strongly related ones.

## Acknowledgements

* [OpenWeatherMap](https://openweathermap.org/) for providing the weather data API.
* [Plotly](https://plotly.com/) for the interactive plotting library.
* [pandas](https://pandas.pydata.org/) for data manipulation and analysis.
