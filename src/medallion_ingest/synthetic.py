"""Deterministic synthetic data for the FacilityOps source system.

The generator is pure Python (no Spark), so it can be unit tested anywhere and
reused from a notebook, a job or a local script. Given the same ``seed``,
``scale`` and ``as_of`` date it always produces the same rows.

It intentionally injects the kind of noise real operational data has - stray
whitespace, inconsistent casing, empty strings, zero placeholders, missing
coordinates, rescheduled activities, meter resets - so the silver layer has
real cleaning work to do.
"""

from __future__ import annotations

import random
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta

from medallion_ingest.scheduling import FREQUENCIES, occurrences
from medallion_ingest.schemas import MONTHS, SCHEMAS, column_names

# --------------------------------------------------------------------------------------
# Reference data
# --------------------------------------------------------------------------------------

CURRENCIES = [
    ("USD", "US Dollar", 1.0),
    ("CAD", "Canadian Dollar", 1.35),
    ("MXN", "Mexican Peso", 18.0),
    ("COP", "Colombian Peso", 4000.0),
    ("PAB", "Panamanian Balboa", 1.0),
    ("EUR", "Euro", 0.92),
]

# name, iso, currencies (first = primary), vat rate, document types, cities (name, lat, lon)
COUNTRIES = [
    (
        "United States",
        "US",
        ["USD"],
        0.07,
        ["EIN", "SSN"],
        [("Austin", 30.27, -97.74), ("Denver", 39.74, -104.99), ("Miami", 25.76, -80.19)],
    ),
    ("Canada", "CA", ["CAD"], 0.13, ["BN"], [("Toronto", 43.65, -79.38), ("Vancouver", 49.28, -123.12)]),
    (
        "Mexico",
        "MX",
        ["MXN", "USD"],
        0.16,
        ["RFC"],
        [("Mexico City", 19.43, -99.13), ("Monterrey", 25.69, -100.32)],
    ),
    (
        "Colombia",
        "CO",
        ["COP"],
        0.19,
        ["NIT", "CC"],
        [("Bogota", 4.71, -74.07), ("Medellin", 6.24, -75.58), ("Cali", 3.45, -76.53)],
    ),
    ("Panama", "PA", ["USD", "PAB"], 0.07, ["RUC"], [("Panama City", 8.98, -79.52)]),
    ("Spain", "ES", ["EUR"], 0.21, ["NIF", "CIF"], [("Madrid", 40.42, -3.70), ("Barcelona", 41.39, 2.17)]),
]

FIRST_NAMES = [
    "Alex",
    "Maria",
    "John",
    "Sofia",
    "David",
    "Laura",
    "Carlos",
    "Emma",
    "Daniel",
    "Lucia",
    "Michael",
    "Valentina",
    "James",
    "Camila",
    "Robert",
    "Olivia",
    "Diego",
    "Ana",
    "Kevin",
    "Paula",
]
LAST_NAMES = [
    "Smith",
    "Garcia",
    "Johnson",
    "Martinez",
    "Brown",
    "Lopez",
    "Miller",
    "Gonzalez",
    "Wilson",
    "Rodriguez",
    "Taylor",
    "Hernandez",
    "Moore",
    "Perez",
    "Clark",
    "Sanchez",
    "Lewis",
    "Torres",
]
ROLES = [
    ("technician", 0.40),
    ("supervisor", 0.15),
    ("unit_manager", 0.15),
    ("analyst", 0.10),
    ("admin", 0.05),
    ("supplier", 0.15),
]

COMPANY_WORDS = ["Northwind", "Bluepeak", "Silverline", "Harborview", "Evergreen", "Summit", "Ironwood", "Brightstone"]
COMPANY_NOUNS = ["Properties", "Holdings", "Realty", "Group", "Capital", "Estates", "Ventures"]
COMPANY_SUFFIXES = ["LLC", "Inc.", "S.A.", "Corp.", "Ltd."]
INDUSTRIES = ["real estate", "retail", "healthcare", "education", "hospitality", "logistics", "corporate offices"]

UNIT_TYPES = {
    "office_building": "Tower",
    "shopping_mall": "Mall",
    "hospital": "Medical Center",
    "university_campus": "Campus",
    "hotel": "Hotel",
    "warehouse": "Logistics Park",
    "residential_complex": "Residences",
}
UNIT_PREFIXES = [
    "Harbor Point",
    "Riverside",
    "Central Park",
    "Oak Ridge",
    "Lakeside",
    "Sunset",
    "Pinecrest",
    "Westgate",
    "Skyline",
    "Maple",
]
STREETS = ["Main St", "Oak Ave", "Park Blvd", "5th Ave", "Market St", "Lake Rd", "Hill St", "Bay Dr"]

# module template -> (spaces, system, [(subsystem, equipment, brands)])
MODULES = {
    "HVAC": (
        ["Mechanical Room", "Rooftop", "Chiller Plant"],
        "Air Conditioning",
        [
            ("Chillers", "Chiller", ["Carrier", "Trane"]),
            ("Air Handling Units", "Air Handler", ["Daikin", "York"]),
            ("Cooling Towers", "Cooling Tower", ["Baltimore Aircoil", "Evapco"]),
        ],
    ),
    "Electrical": (
        ["Main Switchboard Room", "Generator Room", "Substation"],
        "Power Distribution",
        [
            ("Generators", "Diesel Generator", ["Caterpillar", "Cummins"]),
            ("Switchgear", "Switchboard", ["Schneider", "ABB"]),
            ("UPS", "UPS Unit", ["Eaton", "APC"]),
        ],
    ),
    "Plumbing": (
        ["Pump Room", "Water Tank Area", "Restrooms"],
        "Water Supply",
        [("Pressure Pumps", "Booster Pump", ["Grundfos", "Pedrollo"]), ("Water Heaters", "Water Heater", ["Rheem", "Bosch"])],
    ),
    "Fire Protection": (
        ["Fire Pump Room", "Stairwell", "Parking Level"],
        "Fire Suppression",
        [("Fire Pumps", "Fire Pump", ["Aurora", "Peerless"]), ("Detection", "Fire Alarm Panel", ["Notifier", "Simplex"])],
    ),
    "Elevators": (
        ["Elevator Machine Room", "Main Lobby"],
        "Vertical Transport",
        [("Passenger Elevators", "Elevator", ["Otis", "Schindler"]), ("Escalators", "Escalator", ["Kone", "Thyssenkrupp"])],
    ),
    "Security": (
        ["Control Room", "Main Entrance", "Parking Gate"],
        "Access Control",
        [("CCTV", "Camera Array", ["Hikvision", "Axis"]), ("Turnstiles", "Turnstile", ["Gunnebo", "Boon Edam"])],
    ),
    "Landscaping": (
        ["Gardens", "Terrace"],
        "Irrigation",
        [("Irrigation Controllers", "Irrigation Controller", ["Rain Bird", "Hunter"])],
    ),
}

ACTIVITY_TYPES = [
    ("preventive_maintenance", 0.50, "Service"),
    ("routine_maintenance", 0.20, "Check"),
    ("inspection_maintenance", 0.15, "Inspect"),
    ("improvements_adjustments", 0.08, "Adjust"),
    ("corrective_maintenance", 0.04, "Repair"),
    ("", 0.03, "Review"),
]
FREQUENCY_WEIGHTS = {
    "one_time": 0.03,
    "daily": 0.02,
    "weekly": 0.18,
    "biweekly": 0.10,
    "monthly": 0.30,
    "bimonthly": 0.07,
    "quarterly": 0.12,
    "every_four_months": 0.03,
    "semiannual": 0.07,
    "annual": 0.06,
    "biennial": 0.02,
}
FAILURE_CATEGORIES = ["physical_damage", "programming_configuration", "component_replacement"]
FAILURE_DESCRIPTIONS = [
    "Unit not starting",
    "Abnormal noise detected",
    "Overheating alarm",
    "Water leak",
    "Controller fault code",
    "Excessive vibration",
    "Tripped breaker",
    "Sensor reading out of range",
]

METER_TYPES = {"electricity": ("city_power_co", 450.0), "water": ("metro_water", 35.0), "gas": ("national_gas", 60.0)}

TICKET_TYPOLOGIES = {
    "Maintenance Request": ["Air conditioning", "Lighting", "Plumbing", "Elevators"],
    "Cleaning": ["Common areas", "Restrooms", "Waste collection"],
    "Security": ["Access control", "Incident report", "Lost and found"],
    "Billing": ["Invoice dispute", "Payment confirmation"],
    "Information Request": ["Building rules", "Event booking"],
}
TICKET_CATEGORIES = [("request", 0.55), ("complaint", 0.20), ("claim", 0.15), ("suggestion", 0.10)]
TICKET_STATUSES = [("closed", 0.70), ("in_progress", 0.10), ("open", 0.08), ("new", 0.07), ("suspended", 0.05)]
TICKET_COMMENTS = [
    "Tenant reports the issue started this morning",
    "Please attend as soon as possible",
    "Recurring issue",
    "Reported by front desk",
    None,
    None,
]

BUDGET_ITEMS = {
    "operating_expenses": [
        "Preventive maintenance contracts",
        "Spare parts",
        "Cleaning services",
        "Security services",
        "Utilities",
    ],
    "income": ["Service fees", "Parking income", "Space rental"],
    "non_operating_expenses": ["Insurance", "Bank fees"],
    "capex": ["Equipment replacement", "Building upgrades"],
}
SUBITEMS = ["Labor", "Materials", "Third-party services"]

SEASONALITY = [1.00, 0.95, 1.00, 1.02, 1.05, 1.10, 1.15, 1.12, 1.05, 1.00, 0.98, 1.08]


# --------------------------------------------------------------------------------------
# Generator
# --------------------------------------------------------------------------------------


@dataclass
class _Unit:
    id: int
    name: str
    client_id: int
    city_id: int
    currency_factor: float
    vat_rate: float
    team: list[int] = field(default_factory=list)
    modules: list[tuple[int, str]] = field(default_factory=list)
    spaces: list[int] = field(default_factory=list)


class FacilityOpsGenerator:
    """Builds every table of the FacilityOps source system in memory."""

    def __init__(self, seed: int = 42, scale: float = 1.0, as_of: date | None = None) -> None:
        if scale <= 0:
            raise ValueError("scale must be positive")
        self.rng = random.Random(seed)
        self.scale = scale
        self.as_of = as_of or date.today()
        self.now = datetime.combine(self.as_of, time(6, 0))
        self.tables: dict[str, list[dict]] = defaultdict(list)

    # ---- helpers ---------------------------------------------------------------------

    def _count(self, base: int, minimum: int = 1) -> int:
        return max(minimum, round(base * self.scale))

    def _add(self, table: str, **row: object) -> int:
        row_id = len(self.tables[table]) + 1
        self.tables[table].append({"id": row_id, **row})
        return row_id

    def _weighted(self, options: list[tuple]) -> tuple:
        return self.rng.choices(options, weights=[option[1] for option in options])[0]

    def _noisy(self, value: str, probability: float = 0.15) -> str:
        """Randomly pad or re-case a string, like hand-typed operational data."""
        roll = self.rng.random()
        if roll < probability / 3:
            return f"  {value} "
        if roll < probability * 2 / 3:
            return value.upper()
        if roll < probability:
            return value.lower()
        return value

    def _timestamp_between(self, start: datetime, end: datetime) -> datetime:
        seconds = max(int((end - start).total_seconds()), 1)
        return start + timedelta(seconds=self.rng.randrange(seconds))

    def _days_ago(self, max_days: int, min_days: int = 0) -> datetime:
        return self.now - timedelta(days=self.rng.uniform(min_days, max_days))

    # ---- public API ------------------------------------------------------------------

    def generate(self) -> dict[str, list[tuple]]:
        """Return ``{table: rows}`` where each row is a tuple ordered like ``SCHEMAS``."""
        self._geography()
        self._users()
        self._clients_and_contracts()
        units = self._units()
        self._modules_spaces_elements(units)
        self._maintenance()
        self._meters(units)
        self._tickets(units)
        self._budgets(units)
        return {table: self._as_tuples(table) for table in SCHEMAS}

    def _as_tuples(self, table: str) -> list[tuple]:
        columns = column_names(table)
        rows = self.tables.get(table, [])
        for row in rows[:1]:
            if set(row) != set(columns):
                raise ValueError(f"{table}: generated columns {sorted(row)} != contract {sorted(columns)}")
        return [tuple(row[column] for column in columns) for row in rows]

    # ---- builders --------------------------------------------------------------------

    def _geography(self) -> None:
        currency_ids = {}
        self.currency_factor = {}
        for code, name, factor in CURRENCIES:
            currency_ids[code] = self._add("currencies", iso_code=code, name=name)
            self.currency_factor[code] = factor

        self.cities: list[tuple[int, int, float, float, float, float]] = []
        self.document_types: dict[int, list[int]] = {}
        for name, iso, currencies, vat, doc_types, cities in COUNTRIES:
            country_id = self._add(
                "countries",
                name=self._noisy(name),
                iso_code=iso.lower() if self.rng.random() < 0.2 else iso,
                created_at=self.now - timedelta(days=900),
            )
            for position, code in enumerate(currencies):
                self._add("country_currencies", country_id=country_id, currency_id=currency_ids[code], is_primary=position == 0)
            self.document_types[country_id] = [
                self._add("document_types", name=self._noisy(doc_type), country_id=country_id) for doc_type in doc_types
            ]
            for city, lat, lon in cities:
                city_id = self._add("cities", name=self._noisy(city), country_id=country_id)
                self.cities.append((city_id, country_id, lat, lon, self.currency_factor[currencies[0]], vat))

    def _users(self) -> None:
        self.users_by_role: dict[str, list[int]] = defaultdict(list)
        for _ in range(self._count(60, minimum=len(ROLES) * 2)):
            role = self._weighted(ROLES)[0]
            first, last = self.rng.choice(FIRST_NAMES), self.rng.choice(LAST_NAMES)
            created = self._days_ago(1000, 30)
            user_id = self._add(
                "users",
                first_name=self._noisy(first),
                last_name=self._noisy(last),
                email=self._noisy(f"{first}.{last}{self.rng.randint(1, 99)}@facilityops.example".lower(), 0.25),
                role=role,
                is_active=self.rng.random() < 0.9,
                is_staff=role in {"admin", "supervisor", "analyst"},
                is_supplier=role == "supplier",
                is_purchasing_leader=role in {"supervisor", "admin"} and self.rng.random() < 0.3,
                created_at=created,
                updated_at=self._timestamp_between(created, self.now),
            )
            self.users_by_role[role].append(user_id)
        # Guarantee every role has at least one user, whatever the scale.
        for role, _ in ROLES:
            if not self.users_by_role[role]:
                self.users_by_role[role].append(self.rng.randint(1, len(self.tables["users"])))

    def _clients_and_contracts(self) -> None:
        self.contracts: list[tuple[int, int]] = []
        for _ in range(self._count(12, minimum=3)):
            city_id, country_id, *_ = self.rng.choice(self.cities)
            name = f"{self.rng.choice(COMPANY_WORDS)} {self.rng.choice(COMPANY_NOUNS)}"
            created = self._days_ago(1100, 400)
            client_id = self._add(
                "clients",
                name=f"{name} {self.rng.choice(COMPANY_SUFFIXES)}",
                commercial_name=self._noisy(name),
                document_type_id=self.rng.choice(self.document_types[country_id]),
                document_number=self._noisy(f"{self.rng.randint(10_000_000, 99_999_999)}-{self.rng.randint(0, 9)}"),
                email=self._noisy(f"contact@{name.replace(' ', '').lower()}.example", 0.3),
                phone_number=f"+1 555 {self.rng.randint(1000000, 9999999)}",
                industry=self._noisy(self.rng.choice(INDUSTRIES), 0.4),
                city_id=city_id,
                created_at=created,
                updated_at=self._timestamp_between(created, self.now),
            )
            for _ in range(self.rng.randint(1, 2)):
                start = (self.now - timedelta(days=self.rng.randint(200, 1000))).date()
                contract_id = self._add(
                    "contracts",
                    client_id=client_id,
                    name=f"{name} - Facility Services {start.year}",
                    start_date=start,
                    end_date=start + timedelta(days=365 * 3) if self.rng.random() < 0.7 else None,
                    created_at=datetime.combine(start, time(9, 0)),
                )
                self.contracts.append((contract_id, client_id))

    def _units(self) -> list[_Unit]:
        units = []
        managers = self.users_by_role["unit_manager"]
        field_staff = self.users_by_role["technician"] + self.users_by_role["supervisor"]
        for index in range(self._count(25, minimum=3)):
            unit_type, noun = self.rng.choice(list(UNIT_TYPES.items()))
            city_id, _, lat, lon, factor, vat = self.rng.choice(self.cities)
            contract_id, client_id = self.rng.choice(self.contracts)
            name = f"{UNIT_PREFIXES[index % len(UNIT_PREFIXES)]} {noun}" + (
                f" {index // len(UNIT_PREFIXES) + 1}" if index >= len(UNIT_PREFIXES) else ""
            )
            constructed = round(self.rng.uniform(4_000, 80_000), 1)
            has_geo = self.rng.random() > 0.1
            created = self._days_ago(900, 300)
            unit_id = self._add(
                "units",
                name=self._noisy(name),
                unit_type=unit_type,
                contract_id=contract_id,
                manager_id=self.rng.choice(managers),
                city_id=city_id,
                legal_name=f"{name} Owners Association",
                document_number=f"{self.rng.randint(100_000_000, 999_999_999)}",
                phone=f"+1 555 {self.rng.randint(1000000, 9999999)}",
                email=self._noisy(f"admin@{name.replace(' ', '').lower()}.example", 0.3),
                address_line1=f"{self.rng.randint(1, 9999)} {self.rng.choice(STREETS)}",
                address_line2=self.rng.choice(["", "", f"Suite {self.rng.randint(100, 999)}", "  "]),
                year_built=self.rng.choice([0, self.rng.randint(1970, 2022), self.rng.randint(1970, 2022)]),
                constructed_area=constructed,
                rentable_area=round(constructed * self.rng.uniform(0.6, 0.85), 1) if self.rng.random() > 0.1 else 0.0,
                common_area=round(constructed * self.rng.uniform(0.1, 0.25), 1),
                lat=round(lat + self.rng.uniform(-0.08, 0.08), 6) if has_geo else 0.0,
                lon=round(lon + self.rng.uniform(-0.08, 0.08), 6) if has_geo else 0.0,
                is_active=self.rng.random() < 0.92,
                created_at=created,
                updated_at=self._timestamp_between(created, self.now),
            )
            unit = _Unit(unit_id, name, client_id, city_id, factor, vat)
            for user_id in self.rng.sample(field_staff, k=min(len(field_staff), self.rng.randint(2, 5))):
                self._add("unit_team_members", unit_id=unit_id, user_id=user_id, created_at=self._days_ago(300, 1))
                unit.team.append(user_id)
            units.append(unit)
        return units

    def _modules_spaces_elements(self, units: list[_Unit]) -> None:
        module_template_ids, space_template_ids, subsystem_ids = {}, {}, {}
        for module, (spaces, system, subsystems) in MODULES.items():
            module_template_ids[module] = self._add("module_templates", name=module)
            space_template_ids[module] = [
                self._add("space_templates", name=space, module_template_id=module_template_ids[module]) for space in spaces
            ]
            system_id = self._add("systems", name=system)
            subsystem_ids[module] = [
                (self._add("subsystems", name=name, system_id=system_id), equipment, brands)
                for name, equipment, brands in subsystems
            ]

        self.elements: list[tuple[int, _Unit]] = []
        for unit in units:
            for position, module in enumerate(self.rng.sample(list(MODULES), k=self.rng.randint(3, len(MODULES)))):
                unit_module_id = self._add(
                    "unit_modules",
                    unit_id=unit.id,
                    module_template_id=module_template_ids[module],
                    cost_center_id="" if self.rng.random() < 0.08 else f"CC-{unit.id:03d}-{position + 1:02d}",
                    cost_center_name=self._noisy(f"{module} - {unit.name}"),
                )
                unit.modules.append((unit_module_id, module))
                templates = space_template_ids[module]
                for space_template_id in self.rng.sample(templates, k=self.rng.randint(1, len(templates))):
                    space_id = self._add(
                        "spaces",
                        unit_module_id=unit_module_id,
                        space_template_id=space_template_id,
                        created_at=self._days_ago(800, 300),
                    )
                    unit.spaces.append(space_id)
                    for _ in range(self.rng.randint(1, 2)):
                        self._element(unit, unit_module_id, space_id, self.rng.choice(subsystem_ids[module]))

    def _element(self, unit: _Unit, unit_module_id: int, space_id: int, subsystem: tuple[int, str, list[str]]) -> None:
        subsystem_id, equipment, brands = subsystem
        brand = self.rng.choice(brands)
        installed = (self.now - timedelta(days=self.rng.randint(120, 3650))).date()
        received = installed - timedelta(days=self.rng.randint(5, 60))
        created = datetime.combine(received, time(10, 0))
        element_id = self._add(
            "elements",
            unit_module_id=unit_module_id,
            space_id=space_id,
            subsystem_id=subsystem_id,
            name=self._noisy(f"{equipment} {self.rng.randint(1, 12):02d}"),
            serial=self._noisy(f"{brand[:3].upper()}-{self.rng.randint(100000, 999999)}", 0.2),
            quantity=self.rng.choice([1, 1, 1, 2, 4]),
            installer_name=self._noisy(f"{self.rng.choice(COMPANY_WORDS)} Technical Services"),
            capacity_description=self.rng.choice(["", "High capacity", "Standard", f"{self.rng.randint(5, 500)} kW"]),
            brand=self._noisy(brand),
            manufacturer=self._noisy(brand),
            priority=self.rng.choice(["critical", "high", "medium", "medium", "low", "HIGH", " low"]),
            is_active=self.rng.random() < 0.93,
            availability_days=self.rng.choices(["mon_fri", "mon_sat", "mon_sun", None], weights=[45, 20, 33, 2])[0],
            availability_schedule=self.rng.choices(
                ["08-00_18-00", "06-00_22-00", "00-00_00-00", "22-00_06-00", None], weights=[40, 25, 23, 10, 2]
            )[0],
            receipt_date=received,
            installation_date=installed,
            manufacturing_date=received - timedelta(days=self.rng.randint(30, 400)),
            created_at=created,
            updated_at=self._timestamp_between(created, self.now),
        )
        self.elements.append((element_id, unit))

    def _maintenance(self) -> None:
        activity_types = [(name, weight, verb) for name, weight, verb in ACTIVITY_TYPES]
        frequencies = list(FREQUENCY_WEIGHTS.items())
        window_start = self.as_of - timedelta(days=400)
        window_end = self.as_of + timedelta(days=14)
        element_rows = {row["id"]: row for row in self.tables["elements"]}

        plans_by_element: dict[int, int] = {}
        for element_id, unit in self.elements:
            if self.rng.random() > 0.65:
                continue
            activity_type, _, verb = self._weighted(activity_types)
            frequency = self._weighted(frequencies)[0]
            start = self.as_of - timedelta(days=self.rng.randint(-10, 540))
            end = start + timedelta(days=self.rng.randint(90, 400)) if self.rng.random() < 0.1 else None
            owner = self.rng.choice(unit.team)
            equipment = element_rows[element_id]["name"].strip().title()
            plan_id = self._add(
                "activity_plans",
                activity_name=f"{verb} {equipment}",
                activity_type=activity_type,
                frequency=frequency,
                start_date=start,
                end_date=end,
                unit_id=unit.id if self.rng.random() < 0.9 else None,
                element_id=element_id,
                owner_id=owner,
                created_at=datetime.combine(start, time(8, 0)) - timedelta(days=3),
            )
            plans_by_element[element_id] = plan_id
            for due in occurrences(frequency, start, window_start, window_end, end):
                self._scheduled_activity(plan_id, f"{verb} {equipment}", owner, due)

        for element_id, unit in self.elements:
            if self.rng.random() > 0.30:
                continue
            for _ in range(self.rng.randint(1, 2)):
                self._failure(element_id, plans_by_element.get(element_id), unit)

    def _scheduled_activity(self, plan_id: int, name: str, owner: int, due: date) -> None:
        is_past = due < self.as_of
        if self.rng.random() > (0.9 if is_past else 0.8):
            return  # never materialised in the app -> shows up as "Overdue" / "Pending"
        if self.rng.random() < 0.04:
            due += timedelta(days=self.rng.randint(1, 3))  # rescheduled by hand
        scheduled_at = datetime.combine(due, time(self.rng.randint(7, 15), 0))
        started = completed = closed = None
        if scheduled_at < self.now:
            outcome = self.rng.random()
            if outcome < 0.75:
                started = scheduled_at + timedelta(minutes=self.rng.randint(0, 240))
                completed = started + timedelta(minutes=self.rng.randint(20, 300))
            elif outcome < 0.90:
                started = scheduled_at + timedelta(days=self.rng.randint(1, 15))
                completed = started + timedelta(hours=self.rng.randint(1, 30))
            if started and started > self.now:
                started = completed = None  # the late start hasn't happened yet
            completed = min(completed, self.now) if completed else None
            closed = completed + timedelta(hours=self.rng.randint(0, 6)) if completed and self.rng.random() < 0.8 else None
            closed = min(closed, self.now) if closed else None
        self._add(
            "scheduled_activities",
            activity_plan_id=plan_id,
            activity_name=self._noisy(name, 0.1),
            description=self.rng.choice([None, "Follow manufacturer checklist", "Coordinate with building security", ""]),
            owner_id=owner,
            scheduled_at=scheduled_at,
            created_at=scheduled_at - timedelta(days=self.rng.randint(3, 20)),
            started_at=started,
            completed_at=completed,
            closed_at=closed,
            archived_at=scheduled_at if self.rng.random() < 0.02 else None,
        )

    def _failure(self, element_id: int, plan_id: int | None, unit: _Unit) -> None:
        is_open = self.rng.random() < 0.15
        # open failures are recent; old ones have been repaired long ago
        start = self._days_ago(30) if is_open else self._days_ago(365)
        duration = timedelta(hours=min(self.rng.lognormvariate(2.5, 1.2), 24 * 25))
        end = None if is_open or start + duration >= self.now else start + duration
        corrective = self.rng.random() < 0.6
        self._add(
            "element_failures",
            element_id=element_id,
            activity_plan_id=plan_id,
            category=self.rng.choice(FAILURE_CATEGORIES),
            description=self._noisy(self.rng.choice(FAILURE_DESCRIPTIONS)),
            needs_corrective_maintenance=corrective,
            start_date=start,
            end_date=end,
            delivery_date=start + timedelta(days=self.rng.randint(2, 15)) if corrective and self.rng.random() < 0.5 else None,
            corrective_description=self.rng.choice(["", "Replace damaged component", "Recalibrate controller"])
            if corrective
            else "",
            closing_description=self.rng.choice(["Tested and working", "Back in service", ""]) if end else "",
            created_at=start + timedelta(minutes=self.rng.randint(5, 120)),
        )

    def _meters(self, units: list[_Unit]) -> None:
        reading_days = 180
        recorders = self.users_by_role["technician"]
        for unit in units:
            for meter_type in self.rng.sample(list(METER_TYPES), k=self.rng.randint(1, len(METER_TYPES))):
                provider, base_daily = METER_TYPES[meter_type]
                meter_id = self._add(
                    "meters",
                    unit_id=unit.id,
                    client_id=unit.client_id,
                    meter_number=f"{meter_type[:2].upper()}-{self.rng.randint(1_000_000, 9_999_999)}",
                    meter_type=meter_type,
                    provider=provider,
                    created_at=self.now - timedelta(days=reading_days + 30),
                )
                reading = self.rng.uniform(1_000, 50_000)
                daily = base_daily * self.rng.uniform(0.5, 2.5)
                for offset in range(reading_days, 0, -1):
                    day = self.as_of - timedelta(days=offset)
                    if self.rng.random() < 0.05:
                        continue  # missed reading
                    weekend = 0.6 if day.weekday() >= 5 else 1.0
                    reading += daily * weekend * self.rng.uniform(0.8, 1.2)
                    if self.rng.random() < 0.003:
                        reading = self.rng.uniform(0, 100)  # meter replaced / reset
                    self._add(
                        "meter_readings",
                        meter_id=meter_id,
                        reading=round(reading, 2),
                        reading_datetime=datetime.combine(day, time(self.rng.randint(7, 10), self.rng.randint(0, 59))),
                        observations=self.rng.choices([None, "Estimated reading", "Meter obstructed"], weights=[95, 4, 1])[0],
                        recorded_by_id=self.rng.choice(recorders),
                    )

    def _tickets(self, units: list[_Unit]) -> None:
        subtypologies = []
        for typology, children in TICKET_TYPOLOGIES.items():
            typology_id = self._add("ticket_typologies", name=typology)
            subtypologies += [
                (typology_id, self._add("ticket_subtypologies", typology_id=typology_id, name=child)) for child in children
            ]

        for _ in range(self._count(600, minimum=20)):
            unit = self.rng.choice(units)
            typology_id, subtypology_id = self.rng.choice(subtypologies)
            status = self._weighted(TICKET_STATUSES)[0]
            created = self._days_ago(365)
            first_response = None
            if status != "new":
                first_response = min(created + timedelta(hours=self.rng.lognormvariate(2.3, 0.9)), self.now)
            closed = None
            if status == "closed":
                closed = min(first_response + timedelta(hours=self.rng.lognormvariate(3.2, 1.0)), self.now)
            space_id = self.rng.choice(unit.spaces) if self.rng.random() < 0.6 else None
            ticket_id = len(self.tables["tickets"]) + 1
            self._add(
                "tickets",
                slug=f"TCK-{ticket_id:06d}",
                category=self._weighted(TICKET_CATEGORIES)[0],
                status=status,
                comment=self.rng.choice(TICKET_COMMENTS),
                client_id=unit.client_id,
                unit_id=unit.id if space_id is None or self.rng.random() < 0.7 else None,
                space_id=space_id,
                typology_id=typology_id,
                subtypology_id=subtypology_id,
                sla_response_hours=self.rng.choice([24, 24, 12, None]),
                sla_resolution_hours=self.rng.choice([48, 72, 24, None]),
                created_at=created,
                first_response_at=first_response,
                closed_at=closed,
                updated_at=closed or first_response or created,
            )

    def _budgets(self, units: list[_Unit]) -> None:
        for unit in units:
            for year in (self.as_of.year - 1, self.as_of.year):
                is_current = year == self.as_of.year
                status = "draft" if is_current and self.rng.random() < 0.1 else "approved"
                budget_id = self._add(
                    "budgets",
                    unit_id=unit.id,
                    year=year,
                    status=status,
                    created_at=datetime(year - 1, 11, self.rng.randint(1, 28), 9, 0),
                )
                closed_months = self.as_of.month - 1 if is_current else 12
                for order, (unit_module_id, module) in enumerate(unit.modules, start=1):
                    self._chapter(
                        budget_id, unit, unit_module_id, f"Maintenance {module}", "operating_expenses", order, closed_months
                    )
                first_module = unit.modules[0][0]
                self._chapter(budget_id, unit, first_module, "Operating Income", "income", 90, closed_months)
                self._chapter(
                    budget_id, unit, first_module, "Financial and Insurance", "non_operating_expenses", 91, closed_months
                )
                if self.rng.random() < 0.4:
                    self._chapter(budget_id, unit, first_module, "Capital Projects", "capex", 92, closed_months, taxed=False)

    def _chapter(
        self,
        budget_id: int,
        unit: _Unit,
        unit_module_id: int,
        name: str,
        chapter_type: str,
        order: int,
        closed_months: int,
        taxed: bool = True,
    ) -> None:
        chapter = {
            "budget_id": budget_id,
            "unit_module_id": unit_module_id,
            "name": self._noisy(name),
            "chapter_type": chapter_type,
            "sort_order": order,
        }
        planned_tax = [0.0] * 12
        spent_tax = [0.0] * 12
        items = []
        for item_order, item_name in enumerate(
            self.rng.sample(BUDGET_ITEMS[chapter_type], k=min(3, len(BUDGET_ITEMS[chapter_type]))), 1
        ):
            base = self.rng.uniform(800, 9_000) * unit.currency_factor * (4 if chapter_type == "income" else 1)
            item = {"name": self._noisy(item_name), "sort_order": item_order}
            for index, month in enumerate(MONTHS):
                planned = round(base * SEASONALITY[index], 2)
                spent = round(planned * self.rng.uniform(0.7, 1.25), 2) if index < closed_months else 0.0
                item[f"planned_{month}"], item[f"spent_{month}"] = planned, spent
                planned_tax[index] += planned * unit.vat_rate
                spent_tax[index] += spent * unit.vat_rate
            items.append(item)

        for index, month in enumerate(MONTHS):
            chapter[f"planned_tax_{month}"] = round(planned_tax[index], 2) if taxed else 0.0
            chapter[f"spent_tax_{month}"] = round(spent_tax[index], 2) if taxed else 0.0
        chapter_id = self._add("budget_chapters", **chapter)

        for item in items:
            item_id = self._add("budget_items", budget_chapter_id=chapter_id, **item)
            for subitem in self.rng.sample(SUBITEMS, k=self.rng.randint(1, len(SUBITEMS))):
                subitem_id = self._add("budget_subitems", budget_item_id=item_id, name=self._noisy(subitem))
                for _ in range(self.rng.randint(1, 2)):
                    self._add(
                        "budget_concepts",
                        budget_subitem_id=subitem_id,
                        name=self._noisy(f"{subitem} - {item['name'].strip()}"),
                        accounting_code=""
                        if self.rng.random() < 0.05
                        else f"{self.rng.randint(4100, 5999)}-{self.rng.randint(1, 99):02d}",
                    )


def generate(seed: int = 42, scale: float = 1.0, as_of: date | None = None) -> dict[str, list[tuple]]:
    """Generate every FacilityOps table. See :class:`FacilityOpsGenerator`."""
    return FacilityOpsGenerator(seed=seed, scale=scale, as_of=as_of).generate()


__all__ = ["FREQUENCIES", "FacilityOpsGenerator", "generate"]
