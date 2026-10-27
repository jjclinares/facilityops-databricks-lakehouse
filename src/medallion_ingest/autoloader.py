"""Incremental bronze ingestion from the landing volume with Auto Loader."""

from __future__ import annotations

from typing import TYPE_CHECKING

from medallion_ingest.runtime import get_logger
from medallion_ingest.schemas import SCHEMAS

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from medallion_ingest.config import LakehouseConfig

logger = get_logger(__name__)


def ingest_table(spark: SparkSession, config: LakehouseConfig, table: str) -> int:
    """Append new landing files of ``table`` to its bronze Delta table.

    * ``trigger(availableNow=True)`` processes everything new and stops, so the
      stream behaves like an incremental batch job (and runs on serverless).
    * Checkpoints guarantee exactly-once: re-running never duplicates a file.
    * Contract tables are read with their declared schema; unexpected columns
      are captured in ``_rescued_data`` instead of failing the pipeline. Tables
      outside the contract fall back to schema inference + evolution.

    Returns the number of rows appended.
    """
    from pyspark.sql import functions as F

    checkpoint = f"{config.checkpoint_path}/{table}"
    reader = spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").option("pathGlobFilter", "*.parquet")
    if table in SCHEMAS:
        reader = reader.schema(SCHEMAS[table]).option("rescuedDataColumn", "_rescued_data")
    else:
        reader = reader.option("cloudFiles.schemaLocation", f"{checkpoint}/_schema").option(
            "cloudFiles.schemaEvolutionMode", "addNewColumns"
        )

    stream = (
        reader.load(f"{config.landing_path}/{table}")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    query = (
        stream.writeStream.queryName(f"bronze_{table}")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(config.table(table))
    )
    query.awaitTermination()

    rows = sum(int(progress.get("numInputRows", 0)) for progress in query.recentProgress)
    logger.info("bronze %-24s +%d rows", table, rows)
    return rows
