"""
PySpark job: raw sync history → feature store.

Features engineered per (connector_id, table, sync_time):
  - rows_current, rows_previous, rows_delta, rows_pct_change
  - rolling_mean_7d, rolling_std_7d, rolling_mean_30d
  - z_score  (current vs 30d rolling stats)
  - day_of_week, hour_of_day, is_weekend
  - sync_duration_mins, sync_frequency_mins
  - lag_1, lag_2, lag_3  (previous row counts)
"""
from __future__ import annotations
import logging
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, TimestampType, IntegerType,
)

log = logging.getLogger(__name__)

RAW_SCHEMA = StructType([
    StructField("connector_id",    StringType(),    False),
    StructField("connector_name",  StringType(),    True),
    StructField("table_name",      StringType(),    False),
    StructField("sync_time",       TimestampType(), False),
    StructField("rows_updated",    LongType(),      True),
    StructField("sync_status",     StringType(),    True),
    StructField("sync_frequency",  IntegerType(),   True),
])

def get_spark(app_name: str = "fivetran-feature-engineering") -> SparkSession:
    from src.utils.config import get_settings
    cfg = get_settings().spark
    return (
        SparkSession.builder
        .appName(app_name)
        .master(cfg.master)
        .config("spark.executor.instances", str(cfg.executor_instances))
        .config("spark.executor.cores",     str(cfg.executor_cores))
        .config("spark.executor.memory",    cfg.executor_memory)
        .config("spark.driver.memory",      cfg.driver_memory)
        # MinIO / S3A config
        .config("spark.hadoop.fs.s3a.endpoint",               _minio_endpoint())
        .config("spark.hadoop.fs.s3a.access.key",             _minio_key())
        .config("spark.hadoop.fs.s3a.secret.key",             _minio_secret())
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

def _minio_endpoint() -> str:
    from src.utils.config import get_settings
    return get_settings().storage.endpoint_url

def _minio_key() -> str:
    from src.utils.config import get_settings
    return get_settings().storage.access_key

def _minio_secret() -> str:
    from src.utils.config import get_settings
    return get_settings().storage.secret_key

def _s3_path(prefix: str, partition: str = "") -> str:
    from src.utils.config import get_settings
    bucket = get_settings().storage.bucket
    return f"s3a://{bucket}/{prefix}{partition}"

def read_raw(spark: SparkSession, partition: str) -> DataFrame:
    """Read raw JSON for a given hourly partition (YYYY/MM/DD/HH)."""
    path = _s3_path("raw/", partition)
    log.info("Reading raw data from %s", path)
    raw = spark.read.json(path)
    # Explode nested structure: connector + history entries
    exploded = (
        raw
        .select(
            F.col("connector.id").alias("connector_id"),
            F.col("connector.schema").alias("connector_name"),
            F.col("connector.sync_frequency").alias("sync_frequency"),
            F.col("ingested_at"),
            F.explode_outer("history").alias("sync_event"),
        )
        .filter(F.col("sync_event.status") == "SUCCESSFUL")
        .select(
            "connector_id", "connector_name", "sync_frequency",
            F.to_timestamp("ingested_at").alias("sync_time"),
            F.col("sync_event.data").alias("table_data"),
        )
    )
    # Explode table map → one row per (connector, table, sync)
    return (
        exploded
        .select(
            "connector_id", "connector_name", "sync_frequency", "sync_time",
            F.explode_outer("table_data").alias("table_name", "table_stats"),
        )
        .select(
            "connector_id", "connector_name", "sync_frequency", "sync_time",
            "table_name",
            F.coalesce(
                F.col("table_stats.rows_updated"),
                F.col("table_stats.rows_inserted"),
                F.lit(0)
            ).cast(LongType()).alias("rows_updated"),
        )
    )

def engineer_features(df: DataFrame) -> DataFrame:
    """Apply all feature transformations."""
    # Window specs
    w_connector_table = Window.partitionBy("connector_id", "table_name").orderBy("sync_time")
    w_7d  = w_connector_table.rowsBetween(-7,  -1)
    w_30d = w_connector_table.rowsBetween(-30, -1)

    return (
        df
        # Lag features
        .withColumn("rows_lag1", F.lag("rows_updated", 1).over(w_connector_table))
        .withColumn("rows_lag2", F.lag("rows_updated", 2).over(w_connector_table))
        .withColumn("rows_lag3", F.lag("rows_updated", 3).over(w_connector_table))

        # Delta features
        .withColumn("rows_delta",
            F.col("rows_updated") - F.coalesce(F.col("rows_lag1"), F.col("rows_updated")))
        .withColumn("rows_pct_change",
            F.when(F.col("rows_lag1") > 0,
                (F.col("rows_delta") / F.col("rows_lag1") * 100)
            ).otherwise(F.lit(0.0)).cast(DoubleType()))

        # Rolling statistics (7-day)
        .withColumn("rolling_mean_7d",  F.mean("rows_updated").over(w_7d))
        .withColumn("rolling_std_7d",   F.stddev("rows_updated").over(w_7d))
        .withColumn("rolling_min_7d",   F.min("rows_updated").over(w_7d))
        .withColumn("rolling_max_7d",   F.max("rows_updated").over(w_7d))

        # Rolling statistics (30-day)
        .withColumn("rolling_mean_30d", F.mean("rows_updated").over(w_30d))
        .withColumn("rolling_std_30d",  F.stddev("rows_updated").over(w_30d))

        # Z-score vs 30d baseline
        .withColumn("z_score",
            F.when(F.col("rolling_std_30d") > 0,
                (F.col("rows_updated") - F.col("rolling_mean_30d")) / F.col("rolling_std_30d")
            ).otherwise(F.lit(0.0)).cast(DoubleType()))

        # Time features
        .withColumn("day_of_week",  F.dayofweek("sync_time"))
        .withColumn("hour_of_day",  F.hour("sync_time"))
        .withColumn("is_weekend",
            F.when(F.dayofweek("sync_time").isin([1, 7]), 1).otherwise(0))
        .withColumn("week_of_year", F.weekofyear("sync_time"))

        # Fill nulls on first-row edge cases
        .fillna({
            "rows_lag1": 0, "rows_lag2": 0, "rows_lag3": 0,
            "rolling_mean_7d": 0.0, "rolling_std_7d": 0.0,
            "rolling_mean_30d": 0.0, "rolling_std_30d": 0.0,
            "z_score": 0.0, "rows_pct_change": 0.0,
        })
    )

def run_feature_job(partition: str) -> str:
    """
    Main entry point for the Spark feature engineering job.
    partition format: YYYY/MM/DD/HH
    Returns output S3 path.
    """
    log.info("Starting feature engineering for partition: %s", partition)
    spark = get_spark()
    try:
        raw_df      = read_raw(spark, partition)
        features_df = engineer_features(raw_df)
        out_path    = _s3_path("features/", f"dt={partition.replace('/', '-')}/")
        features_df.write.mode("overwrite").parquet(out_path)
        count = features_df.count()
        log.info("Wrote %d feature rows → %s", count, out_path)
        return out_path
    finally:
        spark.stop()

# ── Spark submit entry point ──────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--partition", required=True,
                        help="Hourly partition e.g. 2024/01/15/14")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    out = run_feature_job(args.partition)
    print(f"Output: {out}")
