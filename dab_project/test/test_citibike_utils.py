import os
import sys

sys.path.append(os.getcwd())

os.environ.pop("SPARK_REMOTE", None)
os.environ.pop("DATABRICKS_RUNTIME_VERSION", None)
os.environ.pop("DATABRICKS_HOST", None)
os.environ.pop("DATABRICKS_TOKEN", None)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Row
from src.citibike.citibike_utils import get_trip_duration


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[1]")
        .appName("TestTripDuration")
        .getOrCreate()
    )


def test_get_trip_duration(spark):
    data = [
        Row(
            ride_id="1",
            started_at="2025-07-14 10:00:00",
            ended_at="2025-07-14 10:30:00",
        ),
        Row(
            ride_id="2",
            started_at="2025-07-14 09:45:00",
            ended_at="2025-07-14 10:00:00",
        ),
    ]

    schema = StructType(
        [
            StructField("ride_id", StringType(), True),
            StructField("started_at", StringType(), True),
            StructField("ended_at", StringType(), True),
        ]
    )

    df = spark.createDataFrame(data, schema)

    result_df = get_trip_duration(
        spark, df, "started_at", "ended_at", "trip_duration_mins"
    )

    result = result_df.select("ride_id", "trip_duration_mins").collect()

    expected = {
        "1": 30.0,
        "2": 15.0,
    }

    for row in result:
        assert (
            pytest.approx(row["trip_duration_mins"], 0.01) == expected[row["ride_id"]]
        )
