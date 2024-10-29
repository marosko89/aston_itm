# Data Processing with Apache Spark

## Project Overview

This project is focused on processing airport and flight data using Apache Spark. It loads data from two JSON files, `adsb.json` and `oag.json`, calculates metrics like average speed and delayed flights, and displays the latest flight updates.

## Project Structure

```Bash
.
├── data/
│   ├── adsb.json             # Original JSON file for ADS-B data
│   └── oag.json              # Original JSON file for OAG data
├── src/
│   ├── data_processing.py    # Data loading, transformation, and processing functions
│   ├── metrics.py            # Functions to calculate metrics (e.g., delays, average speed)
│   └── utils.py              # Helper functions (e.g., error handling, logging)
├── main.py                   # Main script to run the data processing pipeline
├── README.md                 # Project documentation and instructions
├── requirements.txt          # Dependencies
└── Pipfile                   # Pipenv configuration
```

## Requirements

- Python 3.8
- Apache Spark (PySpark 3.5)
- Jupyter Notebook (optional, for interactive use)

## Installation

Install dependencies using pipenv or pip:

1. To install dependencies with pipenv run: `pipenv install`
2. Or, to install from `requirements.txt` using `pip` run: `pip install -r requirements.txt`


## Running the Application

To run the data processing pipeline, execute the main script as follows:
```Bash
    python main.py
```

### Expected Output
Upon running `main.py`, the program loads and processes the flight data, computing the following  
key outputs:

1. **Average Speed by Origin** - Displays the average speed for flights from each origin airport.

```
+------+---------+
|Origin|avg_speed|
+------+---------+
|   GUA|    170.0|
|   DOH|    250.0|
|   SGN|    234.2|
|   IAD|    170.0|
+------+---------+

```

2. **Total Delayed Flights** - Calculates the total number of delayed flights for arrivals and departures.
```
Total Delayed Flights: Arrival: 4, Departure: 5
```

3. **Latest Flight Updates** - Shows the latest update for each flight, with columns for `FlightId` and `LastUpdate` (timestamp formatted as `yyyy-MM-dd HH:mm`).

```
Latest Flight Updates:
+--------+----------------+
|FlightId|      LastUpdate|
+--------+----------------+
|  AAL476|2023-10-03 18:36|
|   BA484|2023-10-03 01:47|
|  LXJ476|2023-10-03 18:25|
|   QR476|2023-10-03 01:12|
+--------+----------------+
```

4. **Flight Updates Table** - A detailed table with all updates for each `FlightId`.

```
Flight Updates Table:
+--------+----------------+
|FlightId|      LastUpdate|
+--------+----------------+
|   BA484|2023-10-02 22:27|
|   BA484|2023-10-02 22:37|
|   BA484|2023-10-02 22:53|
...
```

## Additional Notes

**Data**: Ensure that `adsb.json` and `oag.json` are located in the data/ folder before running the script.

## Suggestions for improvement

- Implement data paging for large datasets.
- Define a structured schema for the JSON files to handle nested data more efficiently.
- Enhance error handling and logging for better tracking and maintenance.

### Generate requirements.txt output from lock file

`pipenv requirements > requirements.txt`
