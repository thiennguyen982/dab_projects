from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import max, min, avg, count, round
from databricks.connect import DatabricksSession
import sys


def process_daily_ride_summary(
    spark: SparkSession,
    catalog: str,
    source_table: str = "silver.jc_citibike",
    target_table: str = "gold.daily_ride_summary",
) -> None:
    """
    Aggregates trip statistics per day from the Silver layer and saves them to the Gold layer.

    Args:
        spark: SparkSession
        catalog: Unity Catalog name
        source_table: Table name in the Silver layer
        target_table: Destination table in the Gold layer
    """

    # Load source data
    df = spark.read.table(f"{catalog}.{source_table}")

    # Aggregate trip metrics
    summary_df = df.groupBy("trip_start_date").agg(
        round(max("trip_duration_mins"), 2).alias("max_trip_duration_mins"),
        round(min("trip_duration_mins"), 2).alias("min_trip_duration_mins"),
        round(avg("trip_duration_mins"), 2).alias("avg_trip_duration_mins"),
        count("ride_id").alias("total_trips"),
    )

    # Write result to Gold layer
    summary_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{catalog}.{target_table}"
    )


if __name__ == "__main__":
    # Get catalog from command-line argument
    catalog = sys.argv[1]

    # Initialize Spark session
    spark = DatabricksSession.builder.getOrCreate()

    # Run transformation
    process_daily_ride_summary(spark=spark, catalog=catalog)
