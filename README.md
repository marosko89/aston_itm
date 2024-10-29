# Data Processing with Apache Spark

## Project Overview

This project processes airport and flight data using Apache Spark, specifically focusing on calculating metrics, filtering data, and identifying the most recent flight updates.

## Requirements

- Python 3.8
- Apache Spark (PySpark 3.5)
- Jupyter Notebook (optional, for interactive use)

## Initialize Spark session

```Python
pyspark
```

## Airport and Flight Data Processing

```Python
# Load datasets
adsb_df = spark.read.option("multiLine", "true").json("adsb.json")
raw_df = spark.read.option("multiLine", "true").json("oag.json")

# Dispalay the schema + show data
adsb_df.printSchema()
adsb_df.show(28)

# Explode and expand data fields
from pyspark.sql.functions import explode
exploded_df = raw_df.select(explode("data").alias("flight_data"))
oag_df = exploded_df.select("flight_data.*")
oag_df.show()
oag_df.first().aircraftType

# Data Cleaning
## Remove records with missing essential fields
# adsb_df_clean = adsb_df.dropna(subset=["RadarId"])


# 3.a: Compute Average Speed for each airport
from pyspark.sql.functions import avg
avg_speed_df = adsb_df.groupBy("Origin").agg(avg("speed")).alias("avg_speed")
avg_speed_df.show()

# 3.b: Calculate the Total Number of delayed flights
status_details_exploded = oag_df.select("statusDetails")\
    .selectExpr("explode(statusDetails) as status_detail")

# Filter for arrival & departure delays
from pyspark.sql import functions as F
arrival_delays = status_details_exploded\
    .filter(F.col("status_detail.arrival.actualTime.inGateTimeliness") == "Delayed")
departure_delays = status_details_exploded\
    .filter(F.col("status_detail.departure.actualTime.outGateTimeliness") == "Delayed")
arrival_delays.count()
departure_delays.count()

print(f"Total Number of Delayed Flights:\nArrival: {arrival_delays.count()}\nDeparture: {departure_delays.count()}")

```

## Filter and transform a DataFrame by applying a window function (Spark partitioning):

```Python

# Rename Flight to FlightID
adsb_df = adsb_df.withColumnRenamed("Flight", "FlightId")

from pyspark.sql import Window
from pyspark.sql.functions import row_number, desc,from_unixtime, col

# Define a window specification to get the latest entry for each FlightId
window_spec = Window.partitionBy("FlightId").orderBy(desc("LastUpdate"))
df_with_row_number = adsb_df.withColumn("row_num", row_number().over(window_spec))

# Convert 'LastUpdate' column from UNIX timestamp to a human-readable format
df_with_row_number = df_with_row_number.withColumn("LastUpdate", from_unixtime(col("LastUpdate"), "yyyy-MM-dd HH:mm"))

# Filter to keep only the latest entry: NEW, FRESH
# (Filter the DataFrame to retain only the most recent entry 
# (the one with the largest LastUpdate) for each FlightId.)
filtered_df = df_with_row_number.filter(df_with_row_number["row_num"] == 1)
result_df = filtered_df.select("FlightId", "LastUpdate")

# Show the final filtred DF containing only the FlightId and the corresponding latest LastUpdate .
result_df.show(truncate=False)

# Show the final DF containing only the FlightId and the corresponding latest LastUpdate .
df_with_row_number.select("FlightId", "LastUpdate").show(30)

## Verification
from pyspark.sql.functions import max

latest_update_adsb_df = (
    adsb_df.groupBy("FlightId")  # Group by Flight
    .agg(max("LastUpdate").alias("LastUpdate"))  # Find the max LastUpdate in each group
    .join(adsb_df, ["FlightId", "LastUpdate"])  # Join back to get full row details
    .select("FlightId", "LastUpdate")  # Select required columns
)
latest_update_adsb_df = latest_update_adsb_df.withColumn("LastUpdate", from_unixtime(col("LastUpdate"), "yyyy-MM-dd HH:mm"))
latest_update_adsb_df.show()
```

## Suggestions for improvement

- Load paging
- Define schema for nested JSON (The schema will include `StructType` and `ArrayType`)

### Generate requirements.txt output from lock file

`pipenv requirements > requirements.txt`
