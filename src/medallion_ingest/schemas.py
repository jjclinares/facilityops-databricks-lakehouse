"""Source-system contract.

These are the tables exposed by the (fictional) *FacilityOps* operational
database: a property & facility-management platform that tracks buildings,
their equipment, preventive maintenance plans, failures, utility meters,
service tickets and budgets.

The synthetic generator produces exactly these tables, the JDBC connector
expects them in the source database, and the dbt sources in
``dbt/models/bronze/_bronze__sources.yml`` read them from the bronze schema.

Each schema is a Spark DDL string. Monetary values are DOUBLE on purpose:
operational systems are rarely strict about precision, so the silver layer
casts them to DECIMAL.
"""

from __future__ import annotations

MONTHS = (
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
)


def _monthly(prefix: str) -> str:
    return ", ".join(f"{prefix}_{month} DOUBLE" for month in MONTHS)


SCHEMAS: dict[str, str] = {
    # --- Geography & reference data -------------------------------------------------
    "currencies": "id BIGINT, iso_code STRING, name STRING",
    "countries": "id BIGINT, name STRING, iso_code STRING, created_at TIMESTAMP",
    "country_currencies": "id BIGINT, country_id BIGINT, currency_id BIGINT, is_primary BOOLEAN",
    "cities": "id BIGINT, name STRING, country_id BIGINT",
    "document_types": "id BIGINT, name STRING, country_id BIGINT",
    # --- Parties ---------------------------------------------------------------------
    "clients": (
        "id BIGINT, name STRING, commercial_name STRING, document_type_id BIGINT, "
        "document_number STRING, email STRING, phone_number STRING, industry STRING, "
        "city_id BIGINT, created_at TIMESTAMP, updated_at TIMESTAMP"
    ),
    "users": (
        "id BIGINT, first_name STRING, last_name STRING, email STRING, role STRING, "
        "is_active BOOLEAN, is_staff BOOLEAN, is_supplier BOOLEAN, "
        "is_purchasing_leader BOOLEAN, created_at TIMESTAMP, updated_at TIMESTAMP"
    ),
    "contracts": ("id BIGINT, client_id BIGINT, name STRING, start_date DATE, end_date DATE, created_at TIMESTAMP"),
    # --- Properties & physical assets ------------------------------------------------
    "units": (
        "id BIGINT, name STRING, unit_type STRING, contract_id BIGINT, manager_id BIGINT, "
        "city_id BIGINT, legal_name STRING, document_number STRING, phone STRING, "
        "email STRING, address_line1 STRING, address_line2 STRING, year_built INT, "
        "constructed_area DOUBLE, rentable_area DOUBLE, common_area DOUBLE, lat DOUBLE, "
        "lon DOUBLE, is_active BOOLEAN, created_at TIMESTAMP, updated_at TIMESTAMP"
    ),
    "unit_team_members": "id BIGINT, unit_id BIGINT, user_id BIGINT, created_at TIMESTAMP",
    "module_templates": "id BIGINT, name STRING",
    "unit_modules": ("id BIGINT, unit_id BIGINT, module_template_id BIGINT, cost_center_id STRING, cost_center_name STRING"),
    "space_templates": "id BIGINT, name STRING, module_template_id BIGINT",
    "spaces": ("id BIGINT, unit_module_id BIGINT, space_template_id BIGINT, created_at TIMESTAMP"),
    "systems": "id BIGINT, name STRING",
    "subsystems": "id BIGINT, name STRING, system_id BIGINT",
    "elements": (
        "id BIGINT, unit_module_id BIGINT, space_id BIGINT, subsystem_id BIGINT, "
        "name STRING, serial STRING, quantity INT, installer_name STRING, "
        "capacity_description STRING, brand STRING, manufacturer STRING, priority STRING, "
        "is_active BOOLEAN, availability_days STRING, availability_schedule STRING, "
        "receipt_date DATE, installation_date DATE, manufacturing_date DATE, "
        "created_at TIMESTAMP, updated_at TIMESTAMP"
    ),
    # --- Maintenance -----------------------------------------------------------------
    "activity_plans": (
        "id BIGINT, activity_name STRING, activity_type STRING, frequency STRING, "
        "start_date DATE, end_date DATE, unit_id BIGINT, element_id BIGINT, "
        "owner_id BIGINT, created_at TIMESTAMP"
    ),
    "scheduled_activities": (
        "id BIGINT, activity_plan_id BIGINT, activity_name STRING, description STRING, "
        "owner_id BIGINT, scheduled_at TIMESTAMP, created_at TIMESTAMP, "
        "started_at TIMESTAMP, completed_at TIMESTAMP, closed_at TIMESTAMP, "
        "archived_at TIMESTAMP"
    ),
    "element_failures": (
        "id BIGINT, element_id BIGINT, activity_plan_id BIGINT, category STRING, "
        "description STRING, needs_corrective_maintenance BOOLEAN, "
        "start_date TIMESTAMP, end_date TIMESTAMP, delivery_date TIMESTAMP, "
        "corrective_description STRING, closing_description STRING, created_at TIMESTAMP"
    ),
    # --- Utilities -------------------------------------------------------------------
    "meters": (
        "id BIGINT, unit_id BIGINT, client_id BIGINT, meter_number STRING, "
        "meter_type STRING, provider STRING, created_at TIMESTAMP"
    ),
    "meter_readings": (
        "id BIGINT, meter_id BIGINT, reading DOUBLE, reading_datetime TIMESTAMP, observations STRING, recorded_by_id BIGINT"
    ),
    # --- Service desk ----------------------------------------------------------------
    "ticket_typologies": "id BIGINT, name STRING",
    "ticket_subtypologies": "id BIGINT, typology_id BIGINT, name STRING",
    "tickets": (
        "id BIGINT, slug STRING, category STRING, status STRING, comment STRING, "
        "client_id BIGINT, unit_id BIGINT, space_id BIGINT, typology_id BIGINT, "
        "subtypology_id BIGINT, sla_response_hours INT, sla_resolution_hours INT, "
        "created_at TIMESTAMP, first_response_at TIMESTAMP, closed_at TIMESTAMP, "
        "updated_at TIMESTAMP"
    ),
    # --- Budgeting -------------------------------------------------------------------
    "budgets": "id BIGINT, unit_id BIGINT, year INT, status STRING, created_at TIMESTAMP",
    "budget_chapters": (
        "id BIGINT, budget_id BIGINT, unit_module_id BIGINT, name STRING, "
        "chapter_type STRING, sort_order INT, "
        f"{_monthly('planned_tax')}, {_monthly('spent_tax')}"
    ),
    "budget_items": (
        f"id BIGINT, budget_chapter_id BIGINT, name STRING, sort_order INT, {_monthly('planned')}, {_monthly('spent')}"
    ),
    "budget_subitems": "id BIGINT, budget_item_id BIGINT, name STRING",
    "budget_concepts": ("id BIGINT, budget_subitem_id BIGINT, name STRING, accounting_code STRING"),
}


def column_names(table: str) -> list[str]:
    """Return the ordered column names of a source table."""
    return [column.strip().split(" ", 1)[0] for column in SCHEMAS[table].split(",")]
