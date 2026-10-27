"""Console entry points used by the Databricks job (``python_wheel_task``).

* ``generate-data``  - synthesize a FacilityOps snapshot into the landing volume
* ``ingest-bronze``  - load bronze Delta tables from the landing volume (Auto Loader)
                       or straight from PostgreSQL (JDBC)
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime

from medallion_ingest.config import LakehouseConfig
from medallion_ingest.runtime import ensure_bronze_objects, get_logger, get_spark
from medallion_ingest.schemas import SCHEMAS

logger = get_logger("medallion_ingest")

SOURCES = ("synthetic", "landing", "postgres")


def _base_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--catalog", default="workspace", help="Unity Catalog catalog (default: workspace)")
    parser.add_argument("--schema-prefix", default="", help="Prefix for the layer schemas, e.g. dev_jane_")
    return parser


def _config(args: argparse.Namespace) -> LakehouseConfig:
    return LakehouseConfig(catalog=args.catalog, schema_prefix=args.schema_prefix)


def generate(argv: list[str] | None = None) -> None:
    from medallion_ingest.landing import write_landing
    from medallion_ingest.synthetic import generate as generate_tables

    parser = _base_parser("Generate a synthetic FacilityOps snapshot into the landing volume.")
    parser.add_argument("--scale", type=float, default=1.0, help="Data volume multiplier (1.0 = 25 buildings)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (same seed + date = same data)")
    parser.add_argument("--as-of", type=date.fromisoformat, default=None, help="Snapshot date, YYYY-MM-DD (default: today)")
    args = parser.parse_args(argv)

    config = _config(args)
    spark = get_spark()
    ensure_bronze_objects(spark, config)

    tables = generate_tables(seed=args.seed, scale=args.scale, as_of=args.as_of)
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    counts = write_landing(spark, config, tables, run_id)
    logger.info("run %s landed %d tables / %d rows in %s", run_id, len(counts), sum(counts.values()), config.landing_path)


def _run_parallel(tables: Iterable[str], task: Callable[[str], int], max_workers: int) -> dict[str, int]:
    """Run ``task`` per table concurrently; fail at the end if any table failed."""
    results, failures = {}, {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(task, table): table for table in tables}
        for future in as_completed(futures):
            table = futures[future]
            try:
                results[table] = future.result()
            except Exception as error:  # keep ingesting the other tables
                logger.error("bronze %s FAILED: %s", table, error)
                failures[table] = error
    if failures:
        raise RuntimeError(f"{len(failures)} table(s) failed: {', '.join(sorted(failures))}")
    return results


def ingest(argv: list[str] | None = None) -> None:
    parser = _base_parser("Ingest FacilityOps tables into the bronze layer.")
    parser.add_argument("--source", choices=SOURCES, default="synthetic", help="synthetic/landing = Auto Loader, postgres = JDBC")
    parser.add_argument("--tables", default="", help="Comma-separated subset of tables (default: all contract tables)")
    parser.add_argument("--max-workers", type=int, default=8, help="Tables ingested concurrently")
    parser.add_argument("--secret-scope", default="", help="[postgres] secret scope with postgres-url/-user/-password")
    parser.add_argument("--pg-schema", default="public", help="[postgres] source schema")
    args = parser.parse_args(argv)

    config = _config(args)
    spark = get_spark()
    ensure_bronze_objects(spark, config)
    selected = [table.strip() for table in args.tables.split(",") if table.strip()] or list(SCHEMAS)

    if args.source == "postgres":
        from medallion_ingest import jdbc

        source = jdbc.JdbcSource.from_secrets(args.secret_scope or None, schema=args.pg_schema)
        sizes = jdbc.discover_tables(spark, source)
        missing = sorted(set(selected) - set(sizes))
        if missing:
            logger.warning("not found in %s: %s", args.pg_schema, ", ".join(missing))
        results = _run_parallel(
            [table for table in selected if table in sizes],
            lambda table: jdbc.ingest_table(spark, config, source, table, sizes[table]),
            args.max_workers,
        )
    else:
        from medallion_ingest import autoloader

        results = _run_parallel(selected, lambda table: autoloader.ingest_table(spark, config, table), args.max_workers)

    logger.info("bronze done: %d tables, %d rows written to %s", len(results), sum(results.values()), config.bronze_schema_fqn)


if __name__ == "__main__":
    ingest()
