"""Unity Catalog locations shared by every ingestion entry point."""

from __future__ import annotations

import re
from dataclasses import dataclass

_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]*$")


def _validate(name: str, value: str) -> str:
    if not _IDENTIFIER.match(value):
        raise ValueError(f"{name} must only contain letters, digits and underscores, got {value!r}")
    return value


@dataclass(frozen=True)
class LakehouseConfig:
    """Where the bronze layer lives.

    ``schema_prefix`` isolates deployments that share a catalog: the bundle's
    ``dev`` target sets it to ``dev_<user>_`` so every developer gets their own
    ``dev_<user>_bronze`` / ``_silver`` / ``_gold`` schemas, while ``prod``
    leaves it empty.
    """

    catalog: str = "workspace"
    schema_prefix: str = ""
    landing_volume: str = "landing"
    checkpoint_volume: str = "checkpoints"

    def __post_init__(self) -> None:
        if not self.catalog:
            raise ValueError("catalog is required")
        for name in ("catalog", "schema_prefix", "landing_volume", "checkpoint_volume"):
            _validate(name, getattr(self, name))

    @property
    def bronze_schema(self) -> str:
        return f"{self.schema_prefix}bronze"

    @property
    def bronze_schema_fqn(self) -> str:
        return f"`{self.catalog}`.`{self.bronze_schema}`"

    def table(self, name: str) -> str:
        """Fully qualified, quoted name of a bronze table."""
        return f"{self.bronze_schema_fqn}.`{_validate('table', name)}`"

    def volume_fqn(self, volume: str) -> str:
        return f"{self.bronze_schema_fqn}.`{volume}`"

    @property
    def landing_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.bronze_schema}/{self.landing_volume}"

    @property
    def checkpoint_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.bronze_schema}/{self.checkpoint_volume}"
