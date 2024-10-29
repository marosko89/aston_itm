from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, col, desc, from_unixtime, row_number
from pyspark.sql import Window
from typing import Tuple

DATETIME_FORMAT = "yyyy-MM-dd HH:mm"

# Column Names
FLIGHT_ID = "FlightId"
LAST_UPDATE = "LastUpdate"

def load_data(spark, file_path):
    """Load JSON data into a Spark DataFrame."""
    return spark.read.option("multiLine", "true").json(file_path)

def preprocess_data(raw_df):
    """Explode nested JSON structure and select relevant fields."""
    exploded_df = raw_df.select(explode("data").alias("flight_data"))
    return exploded_df.select("flight_data.*")

def get_latest_flight_updates(adsb_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Filter the DataFrame to retain only the most recent entry for each FlightId.
    Also returns the complete DataFrame with columns [FlightId, LastUpdate].

    Args:
        adsb_df (DataFrame): Input DataFrame containing flight data with columns 
                             including 'FlightId' and 'LastUpdate'.

    Returns:
        Tuple[DataFrame, DataFrame]: A tuple containing:
            - DataFrame with the latest updates per FlightId with columns [FlightId, LastUpdate].
            - Complete DataFrame with all updates with columns [FlightId, LastUpdate].
    """

    # Ensure required columns exist
    required_columns = [FLIGHT_ID, LAST_UPDATE]
    if not all(col in adsb_df.columns for col in required_columns):
        raise ValueError(f"The input DataFrame must contain the following columns: {required_columns}")

    # Define a window specification to get the latest entry for each FlightId
    window_spec = Window.partitionBy(FLIGHT_ID).orderBy(desc(LAST_UPDATE))

    # Add a row number to each entry in the window
    df_with_row_number = adsb_df.withColumn("row_num", row_number().over(window_spec))

    # Select the latest updates and all updates
    latest_flight_updates = df_with_row_number.filter(df_with_row_number["row_num"] == 1)\
                            .select(FLIGHT_ID, LAST_UPDATE)\
                            .withColumn(LAST_UPDATE, from_unixtime(col(LAST_UPDATE), DATETIME_FORMAT))
    flight_updates = df_with_row_number.select(FLIGHT_ID, LAST_UPDATE)\
                                       .withColumn(LAST_UPDATE, from_unixtime(col(LAST_UPDATE), DATETIME_FORMAT))

    return latest_flight_updates, flight_updates
