from pyspark.sql.functions import to_date, col


def timestamp_to_date_col(spark, df, timestamp_col, output_col):
    return df.withColumn(output_col, to_date((col(timestamp_col))))
