from datetime import datetime
import pytest
from pyspark.sql.types import StructType, StructField, TimestampType, DateType
from pyspark.sql import Row
from src.utils.datetime_utils import timestamp_to_date_col

def test_timestamp_to_date_col(spark):
    data = [
        Row(event_time=datetime(2025, 7, 21, 10, 0, 0)),
        Row(event_time=datetime(2025, 7, 21, 23, 59, 59)),
    ]
    df = spark.createDataFrame(data)

    result_df = timestamp_to_date_col(spark, df, "event_time", "event_date")

    expected_data = [
        (datetime(2025, 7, 21, 10, 0, 0), datetime(2025, 7, 21).date()),
        (datetime(2025, 7, 21, 23, 59, 59), datetime(2025, 7, 21).date()),
    ]
    expected_schema = StructType([
        StructField("event_time", TimestampType(), True),
        StructField("event_date", DateType(), True),
    ])
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    result = result_df.collect()
    expected = expected_df.collect()

    assert result == expected