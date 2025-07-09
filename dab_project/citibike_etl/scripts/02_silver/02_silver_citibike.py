from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, create_map
from databricks.connect import DatabricksSession
from citibike.citibike_utils import get_trip_duration
from utils.datetime_utils import timestamp_to_date_col
import sys


def process_citibike_data(
    spark: SparkSession,
    catalog: str,
    pipeline_id: str,
    run_id: str,
    task_id: str,
    processed_timestamp: str,
    source_table: str = "bronze.jc_citibike",
    target_table: str = "silver.jc_citibike",
) -> None:
    """
    Processes CitiBike data by calculating trip duration, extracting trip start date,
    and adding metadata, then writes it to the Silver table.

    Args:
        spark: SparkSession
        catalog: Unity Catalog name
        pipeline_id: ID of the pipeline
        run_id: ID of the pipeline run
        task_id: ID of the task
        processed_timestamp: Timestamp when the pipeline was run
        source_table: Source table path in bronze layer
        target_table: Target table path in silver layer

    """

    # Read source data
    df = spark.read.table(f"{catalog}.{source_table}")

    # Compute trip duration
    df = get_trip_duration(spark, df, "started_at", "ended_at", "trip_duration_mins")

    # Extract date from timestamp
    df = timestamp_to_date_col(spark, df, "started_at", "trip_start_date")

    # Add metadata
    df = df.withColumn(
        "metadata",
        create_map(
            lit("pipeline_id"),
            lit(pipeline_id),
            lit("run_id"),
            lit(run_id),
            lit("task_id"),
            lit(task_id),
            lit("processed_timestamp"),
            lit(processed_timestamp),
        ),
    )

    # Select relevant columns
    df = df.select(
        "ride_id",
        "trip_start_date",
        "started_at",
        "ended_at",
        "start_station_name",
        "end_station_name",
        "trip_duration_mins",
        "metadata",
    )

    # Write to target table in silver layer
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{catalog}.{target_table}"
    )


if __name__ == "__main__":
    # Command-line arguments
    pipeline_id = sys.argv[1]
    run_id = sys.argv[2]
    task_id = sys.argv[3]
    processed_timestamp = sys.argv[4]
    catalog = sys.argv[5]

    # Initialize Spark session
    spark = DatabricksSession.builder.getOrCreate()

    # Execute processing function
    process_citibike_data(
        spark=spark,
        catalog=catalog,
        pipeline_id=pipeline_id,
        run_id=run_id,
        task_id=task_id,
        processed_timestamp=processed_timestamp,
    )
