from pyspark.sql import SparkSession
from src.data_processing import load_data, preprocess_data, get_latest_flight_updates
from src.metrics import compute_average_speed, count_delayed_flights
from src.utils import log_error

# Initialize Spark session
spark = SparkSession.builder.appName("FlightDataProcessing").getOrCreate()

try:
    # Load datasets
    adsb_df = load_data(spark, "data/adsb.json")
    oag_df = load_data(spark, "data/oag.json")
except Exception as e:
    log_error(f"Failed to load data: {e}")
    spark.stop()
    exit(1)

# Rename Flight to FlightID
adsb_df = adsb_df.withColumnRenamed("Flight", "FlightId")

# Process OAG data
processed_oag_df = preprocess_data(oag_df)

# Get metrics
avg_speed_df = compute_average_speed(adsb_df)
arrival_delays, departure_delays = count_delayed_flights(processed_oag_df)

# Retrieve the latest flight updates
latest_updates_df, updates_df_all = get_latest_flight_updates(adsb_df)

# Show results
print("Average Speed by Airport:")
avg_speed_df.show()
print(f"\nTotal Delayed Flights: Arrival: {arrival_delays}, Departure: {departure_delays}")

print("\nLatest Flight Updates:")
latest_updates_df.show()
print("\nFlight Updates Table:")
updates_df_all.show(30)

# Stop Spark session
spark.stop()
