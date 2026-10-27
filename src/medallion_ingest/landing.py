"""Write synthetic extracts to the landing volume, one folder per table and run."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

from medallion_ingest.runtime import get_logger
from medallion_ingest.schemas import SCHEMAS

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from medallion_ingest.config import LakehouseConfig

logger = get_logger(__name__)


def write_landing(
    spark: SparkSession,
    config: LakehouseConfig,
    tables: dict[str, list[tuple]],
    run_id: str,
    max_workers: int = 8,
) -> dict[str, int]:
    """Write each table as parquet under ``<landing>/<table>/<run_id>/``.

    Every run lands a full snapshot in a new folder, exactly like a nightly
    export from an operational database. Auto Loader then picks up only the new
    files, and silver keeps the latest version of each record.
    """

    def write(table: str) -> tuple[str, int]:
        rows = tables[table]
        path = f"{config.landing_path}/{table}/{run_id}"
        spark.createDataFrame(rows, schema=SCHEMAS[table]).write.mode("overwrite").parquet(path)
        logger.info("landed %-24s %8d rows -> %s", table, len(rows), path)
        return table, len(rows)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        return dict(pool.map(write, tables))
