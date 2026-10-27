"""Snapshot ingestion from a PostgreSQL source over JDBC.

Small tables are read in one query. Large tables are split into parallel range
reads on an integer key, so a 50 GB table is pulled by many Spark tasks instead
of a single connection. Partition counts are derived from the table size
reported by ``pg_total_relation_size``.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

from medallion_ingest.runtime import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from medallion_ingest.config import LakehouseConfig

logger = get_logger(__name__)

INTEGER_TYPES = {"smallint", "integer", "bigint"}


@dataclass(frozen=True)
class JdbcSource:
    url: str
    user: str
    password: str
    schema: str = "public"
    partition_column: str = "id"
    target_partition_bytes: int = 128 * 1024 * 1024
    max_partitions: int = 64
    fetch_size: int = 10_000

    @classmethod
    def from_secrets(cls, scope: str | None, schema: str = "public") -> JdbcSource:
        """Read credentials from a Databricks secret scope, or ``PG_*`` env vars locally."""
        if scope:
            from databricks.sdk.runtime import dbutils

            def secret(key: str) -> str:
                return dbutils.secrets.get(scope=scope, key=key)

            return cls(secret("postgres-url"), secret("postgres-user"), secret("postgres-password"), schema)

        missing = [name for name in ("PG_JDBC_URL", "PG_USER", "PG_PASSWORD") if not os.getenv(name)]
        if missing:
            raise ValueError(f"Provide --secret-scope or set {', '.join(missing)}")
        return cls(os.environ["PG_JDBC_URL"], os.environ["PG_USER"], os.environ["PG_PASSWORD"], schema)


def plan_partitions(size_bytes: int, target_partition_bytes: int, max_partitions: int) -> int:
    """Number of parallel JDBC reads for a table of ``size_bytes``."""
    if size_bytes <= target_partition_bytes:
        return 1
    return min(max_partitions, math.ceil(size_bytes / target_partition_bytes))


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _read(spark: SparkSession, source: JdbcSource, **options: str) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", source.url)
        .option("user", source.user)
        .option("password", source.password)
        .option("fetchsize", str(source.fetch_size))
        .options(**options)
        .load()
    )


def discover_tables(spark: SparkSession, source: JdbcSource) -> dict[str, int]:
    """Return ``{table: size_bytes}`` for every table in the source schema."""
    query = f"""
        select c.relname as table_name, pg_total_relation_size(c.oid) as size_bytes
        from pg_catalog.pg_class c
        join pg_catalog.pg_namespace n on n.oid = c.relnamespace
        where c.relkind in ('r', 'p') and n.nspname = {_literal(source.schema)}
    """
    return {row.table_name: int(row.size_bytes) for row in _read(spark, source, query=query).collect()}


def _range_options(spark: SparkSession, source: JdbcSource, table: str, partitions: int) -> dict[str, str]:
    """JDBC partitioning options, or ``{}`` when the table has no integer key."""
    column_type = _read(
        spark,
        source,
        query=f"""
            select data_type from information_schema.columns
            where table_schema = {_literal(source.schema)}
              and table_name = {_literal(table)}
              and column_name = {_literal(source.partition_column)}
        """,
    ).collect()
    if not column_type or column_type[0].data_type not in INTEGER_TYPES:
        logger.warning("%s has no integer %r column; reading it in a single partition", table, source.partition_column)
        return {}

    column = _quote(source.partition_column)
    bounds = _read(
        spark,
        source,
        query=f"select min({column}) as lo, max({column}) as hi from {_quote(source.schema)}.{_quote(table)}",
    ).first()
    if bounds is None or bounds.lo is None:
        return {}
    return {
        "partitionColumn": source.partition_column,
        "lowerBound": str(bounds.lo),
        "upperBound": str(bounds.hi),
        "numPartitions": str(partitions),
    }


def ingest_table(spark: SparkSession, config: LakehouseConfig, source: JdbcSource, table: str, size_bytes: int) -> int:
    """Overwrite the bronze table with a fresh snapshot of the source table."""
    from pyspark.sql import functions as F

    partitions = plan_partitions(size_bytes, source.target_partition_bytes, source.max_partitions)
    options = _range_options(spark, source, table, partitions) if partitions > 1 else {}

    snapshot = (
        _read(spark, source, dbtable=f"{_quote(source.schema)}.{_quote(table)}", **options)
        .withColumn("_source_file", F.lit(None).cast("string"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    snapshot.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(config.table(table))

    rows = spark.table(config.table(table)).count()
    logger.info("bronze %-24s %d rows (%d partition(s), %.1f MB)", table, rows, max(partitions, 1), size_bytes / 1e6)
    return rows
