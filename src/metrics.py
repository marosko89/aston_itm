from pyspark.sql.functions import avg, col, explode

# Status Details
DELAYED_STATUS = "Delayed"

def compute_average_speed(adsb_df):
    """Compute average speed for each origin airport."""
    return adsb_df.groupBy("Origin").agg(avg("Speed").alias("avg_speed"))

def count_delayed_flights(oag_df):
    """Count the total number of delayed flights by arrival and departure."""

    # Explode the statusDetails array to create a row for each entry
    status_details = oag_df.select(explode("statusDetails").alias("status_detail"))

    arrival_delays = status_details.filter(col("status_detail.arrival.actualTime.inGateTimeliness") == DELAYED_STATUS)
    departure_delays = status_details.filter(col("status_detail.departure.actualTime.outGateTimeliness") == DELAYED_STATUS)

    return arrival_delays.count(), departure_delays.count()
