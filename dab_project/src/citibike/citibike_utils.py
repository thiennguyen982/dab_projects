from pyspark.sql.functions import col


def get_trip_duration(spark, df, start_col, end_col, output_col):
    return df.withColumn(
        output_col,
        (
            col(end_col).cast("timestamp").cast("long")
            - col(start_col).cast("timestamp").cast("long")
        )
        / 60,
    )
