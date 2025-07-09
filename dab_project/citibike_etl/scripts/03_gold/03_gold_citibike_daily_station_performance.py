from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, round
from databricks.connect import DatabricksSession
import sys


def process_daily_station_performance(
    spark: SparkSession,
    catalog: str,
    source_table: str = "silver.jc_citibike",
    target_table: str = "gold.daily_station_performance",
) -> None:
    """
    Aggregates average trip duration and trip count per day and station from the Silver layer
    and writes results to the Gold layer.

    Args:
        spark: SparkSession
        catalog: Unity Catalog name
        source_table: Source table in the Silver layer
        target_table: Target table in the Gold layer
    """

    # Read source data
    df = spark.read.table(f"{catalog}.{source_table}")

    # Perform aggregation
    performance_df = df.groupBy("trip_start_date", "start_station_name").agg(
        round(avg("trip_duration_mins"), 2).alias("avg_trip_duration_mins"),
        count("ride_id").alias("total_trips"),
    )

    # Save results to Gold table
    performance_df.write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{catalog}.{target_table}")


if __name__ == "__main__":
    # Read catalog name from CLI argument
    catalog = sys.argv[1]

    # Initialize Spark session
    spark = DatabricksSession.builder.getOrCreate()

    # Run the transformation
    process_daily_station_performance(spark=spark, catalog=catalog)
