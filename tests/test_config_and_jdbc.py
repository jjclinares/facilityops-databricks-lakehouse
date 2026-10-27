import pytest

from medallion_ingest.config import LakehouseConfig
from medallion_ingest.jdbc import plan_partitions
from medallion_ingest.schemas import SCHEMAS, column_names

MB = 1024 * 1024


def test_paths_and_names_follow_prefix():
    config = LakehouseConfig(catalog="lakehouse", schema_prefix="dev_jane_")
    assert config.bronze_schema == "dev_jane_bronze"
    assert config.table("units") == "`lakehouse`.`dev_jane_bronze`.`units`"
    assert config.landing_path == "/Volumes/lakehouse/dev_jane_bronze/landing"
    assert config.checkpoint_path == "/Volumes/lakehouse/dev_jane_bronze/checkpoints"


@pytest.mark.parametrize("kwargs", [{"catalog": "bad-name"}, {"schema_prefix": "x; drop"}, {"catalog": ""}])
def test_unsafe_identifiers_are_rejected(kwargs):
    with pytest.raises(ValueError):
        LakehouseConfig(**kwargs)


@pytest.mark.parametrize(
    ("size", "expected"),
    [(0, 1), (100 * MB, 1), (128 * MB, 1), (129 * MB, 2), (1024 * MB, 8), (100_000 * MB, 64)],
)
def test_plan_partitions(size, expected):
    assert plan_partitions(size, target_partition_bytes=128 * MB, max_partitions=64) == expected


def test_every_contract_table_has_an_id_column():
    for table in SCHEMAS:
        assert column_names(table)[0] == "id", table
