from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
    TimestampType,
)
from pyspark.sql.functions import create_map, lit
from databricks.connect import DatabricksSession
import sys


def ingest_landing_to_bronze(
    spark: SparkSession,
    catalog: str,
    pipeline_id: str,
    run_id: str,
    task_id: str,
    processed_timestamp: str,
    volume_path: str = "landing/source_citibike_data",
    target_table: str = "bronze.jc_citibike",
) -> None:
    """
    Reads raw CitiBike data from the landing volume, enriches it with metadata, and writes to the Bronze table.

    Args:
        spark: SparkSession
        catalog: Unity Catalog name
        pipeline_id: Pipeline ID
        run_id: Pipeline run ID
        task_id: Task ID
        processed_timestamp: ISO-formatted timestamp string
        volume_path: Path in the volume to read the raw data from
        target_table: Destination table path in the bronze layer
    """

    schema = StructType(
        [
            StructField("ride_id", StringType(), True),
            StructField("rideable_type", StringType(), True),
            StructField("started_at", TimestampType(), True),
            StructField("ended_At", TimestampType(), True),
            StructField("start_station_name", StringType(), True),
            StructField("start_station_id", StringType(), True),
            StructField("end_station_name", StringType(), True),
            StructField("end_station_id", StringType(), True),
            StructField("start_lat", DecimalType(), True),
            StructField("start_lng", DecimalType(), True),
            StructField("end_lat", DecimalType(), True),
            StructField("end_lng", DecimalType(), True),
            StructField("member_casual", StringType(), True),
        ]
    )

    # Load data from volume
    df = spark.read.csv(f"/Volumes/{catalog}/{volume_path}", header=True, schema=schema)

    # Add metadata column
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

    # Write to bronze table
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

    # Initialize Spark
    spark = DatabricksSession.builder.getOrCreate()

    # Run ingestion
    ingest_landing_to_bronze(
        spark=spark,
        catalog=catalog,
        pipeline_id=pipeline_id,
        run_id=run_id,
        task_id=task_id,
        processed_timestamp=processed_timestamp,
    )
