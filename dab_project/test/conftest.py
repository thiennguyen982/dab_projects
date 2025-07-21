import os
import sys
from pyspark.sql import SparkSession


import pytest

sys.path.append(os.getcwd())

os.environ.pop("SPARK_REMOTE", None)
os.environ.pop("DATABRICKS_RUNTIME_VERSION", None)
os.environ.pop("DATABRICKS_HOST", None)
os.environ.pop("DATABRICKS_TOKEN", None)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[1]")
        .appName("TestTripDuration")
        .getOrCreate()
    )
