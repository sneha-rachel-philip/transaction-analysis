from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, dense_rank, sum as _sum
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_data(spark: SparkSession, path: str) -> DataFrame:
    """Read CSV data from the given path and return a DataFrame."""
    logger.info(f"Reading CSV input data from HDFS path {path}")
    return spark.read.option("header", True).option("delimiter", ";").csv(path)


def transform_data(input_df: DataFrame) -> DataFrame:
    """Transform the input data as per the business logic and return a DataFrame."""
    logger.info("Transforming input data")

    # Grouping and aggregating data
    df = input_df.select(col("user_id"), col("counterparty_id"), col("amount")) \
        .groupBy("user_id", "counterparty_id") \
        .agg(_sum("amount").alias("amount"))

    # Applying window function to rank the counterparty with the highest amount per user_id
    window_spec = Window.partitionBy("user_id").orderBy(col("amount").desc())
    df = df.withColumn("rank", dense_rank().over(window_spec)) \
        .where(col("rank") == 1) \
        .drop("rank")

    return df


def write_data(df: DataFrame, path: str):
    """Write the DataFrame as Parquet files to the given path."""
    logger.info(f"Writing output DataFrame as Parquet files to path: {path}")

    # Write the DataFrame to the specified location as Parquet
    df.write.mode("overwrite").parquet(path)
