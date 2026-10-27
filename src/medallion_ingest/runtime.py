"""Spark session, logging and Unity Catalog bootstrap helpers."""

from __future__ import annotations

import logging
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from medallion_ingest.config import LakehouseConfig

LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"


def get_logger(name: str) -> logging.Logger:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, stream=sys.stdout, format=LOG_FORMAT)
    # py4j / Spark Connect are very chatty at INFO.
    for noisy in ("py4j", "pyspark", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return logging.getLogger(name)


def get_spark() -> SparkSession:
    """Return the active Spark session.

    On Databricks (jobs, notebooks, serverless) a session already exists. Locally,
    Databricks Connect is used when installed (``uv sync --group connect``) so the
    same code runs against a remote workspace.
    """
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    try:
        from databricks.connect import DatabricksSession
    except ImportError:
        return SparkSession.builder.getOrCreate()
    return DatabricksSession.builder.getOrCreate()


def ensure_bronze_objects(spark: SparkSession, config: LakehouseConfig) -> None:
    """Create the bronze schema and its landing / checkpoint volumes (idempotent)."""
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {config.bronze_schema_fqn} "
        "COMMENT 'Bronze layer: raw FacilityOps data, append-only, with ingestion metadata'"
    )
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS {config.volume_fqn(config.landing_volume)} "
        "COMMENT 'Landing zone for raw source extracts (parquet)'"
    )
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS {config.volume_fqn(config.checkpoint_volume)} "
        "COMMENT 'Structured Streaming / Auto Loader checkpoints'"
    )
