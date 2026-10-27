from datetime import date

import pytest

from medallion_ingest.schemas import SCHEMAS, column_names
from medallion_ingest.synthetic import generate

AS_OF = date(2026, 6, 15)

# child table, column -> parent table
FOREIGN_KEYS = [
    ("country_currencies", "country_id", "countries"),
    ("country_currencies", "currency_id", "currencies"),
    ("cities", "country_id", "countries"),
    ("document_types", "country_id", "countries"),
    ("clients", "document_type_id", "document_types"),
    ("clients", "city_id", "cities"),
    ("contracts", "client_id", "clients"),
    ("units", "contract_id", "contracts"),
    ("units", "manager_id", "users"),
    ("units", "city_id", "cities"),
    ("unit_team_members", "unit_id", "units"),
    ("unit_team_members", "user_id", "users"),
    ("unit_modules", "unit_id", "units"),
    ("unit_modules", "module_template_id", "module_templates"),
    ("space_templates", "module_template_id", "module_templates"),
    ("spaces", "unit_module_id", "unit_modules"),
    ("spaces", "space_template_id", "space_templates"),
    ("subsystems", "system_id", "systems"),
    ("elements", "unit_module_id", "unit_modules"),
    ("elements", "space_id", "spaces"),
    ("elements", "subsystem_id", "subsystems"),
    ("activity_plans", "unit_id", "units"),
    ("activity_plans", "element_id", "elements"),
    ("activity_plans", "owner_id", "users"),
    ("scheduled_activities", "activity_plan_id", "activity_plans"),
    ("scheduled_activities", "owner_id", "users"),
    ("element_failures", "element_id", "elements"),
    ("element_failures", "activity_plan_id", "activity_plans"),
    ("meters", "unit_id", "units"),
    ("meters", "client_id", "clients"),
    ("meter_readings", "meter_id", "meters"),
    ("ticket_subtypologies", "typology_id", "ticket_typologies"),
    ("tickets", "client_id", "clients"),
    ("tickets", "unit_id", "units"),
    ("tickets", "space_id", "spaces"),
    ("tickets", "typology_id", "ticket_typologies"),
    ("tickets", "subtypology_id", "ticket_subtypologies"),
    ("budgets", "unit_id", "units"),
    ("budget_chapters", "budget_id", "budgets"),
    ("budget_chapters", "unit_module_id", "unit_modules"),
    ("budget_items", "budget_chapter_id", "budget_chapters"),
    ("budget_subitems", "budget_item_id", "budget_items"),
    ("budget_concepts", "budget_subitem_id", "budget_subitems"),
]


@pytest.fixture(scope="module")
def tables():
    return generate(seed=7, scale=0.5, as_of=AS_OF)


def _column(tables, table, column):
    index = column_names(table).index(column)
    return [row[index] for row in tables[table]]


def test_every_contract_table_is_generated(tables):
    assert set(tables) == set(SCHEMAS)
    for table, rows in tables.items():
        assert rows, f"{table} is empty"
        assert all(len(row) == len(column_names(table)) for row in rows)


def test_primary_keys_are_unique(tables):
    for table in tables:
        ids = _column(tables, table, "id")
        assert len(ids) == len(set(ids)), table


@pytest.mark.parametrize(("child", "column", "parent"), FOREIGN_KEYS)
def test_foreign_keys_resolve(tables, child, column, parent):
    parent_ids = set(_column(tables, parent, "id"))
    orphans = {value for value in _column(tables, child, column) if value is not None} - parent_ids
    assert not orphans, f"{child}.{column} -> {parent}: {sorted(orphans)[:5]}"


def test_generation_is_deterministic():
    assert generate(seed=1, scale=0.3, as_of=AS_OF) == generate(seed=1, scale=0.3, as_of=AS_OF)
    assert generate(seed=1, scale=0.3, as_of=AS_OF) != generate(seed=2, scale=0.3, as_of=AS_OF)


def test_scale_controls_volume():
    small = generate(seed=3, scale=0.5, as_of=AS_OF)
    large = generate(seed=3, scale=2.0, as_of=AS_OF)
    assert len(large["units"]) > len(small["units"])
    assert len(large["meter_readings"]) > len(small["meter_readings"])


def test_activities_and_failures_are_chronological(tables):
    for completed, started in zip(
        _column(tables, "scheduled_activities", "completed_at"),
        _column(tables, "scheduled_activities", "started_at"),
        strict=True,
    ):
        assert completed is None or started <= completed
    for end, start in zip(
        _column(tables, "element_failures", "end_date"),
        _column(tables, "element_failures", "start_date"),
        strict=True,
    ):
        assert end is None or start <= end


def test_invalid_scale_is_rejected():
    with pytest.raises(ValueError):
        generate(scale=0)
