"""Create or upgrade the Assignment 2 DuckDB database.

The Assignment 1 database is treated as an immutable input.  On the first run (or
with --reset), it is copied to data/db/ev_chargers.duckdb and the Assignment 2 DDL
is applied to that copy.  Later runs only reapply the idempotent DDL, preserving
events already received by the MQTT subscriber.

Run:
    python -m evstream.db             # create if missing; otherwise apply schema
    python -m evstream.db --reset     # replace A2 DB with a fresh A1 copy
"""

import argparse
import shutil
from pathlib import Path

import duckdb

from evstream import config


REQUIRED_A1_TABLES = {
    "charger",
    "charging_site",
    "operator",
    "sa4_region",
    "charger_power_rating",
    "connector_type",
    "charger_connector",
}


def _require_file(path: Path, description: str) -> None:
    """Raise a useful error before DuckDB reports a less specific failure."""
    if not path.is_file():
        raise FileNotFoundError(f"{description} not found: {path}")


def _table_names(con: duckdb.DuckDBPyConnection) -> set[str]:
    """Return user table names in the current database."""
    return {
        row[0]
        for row in con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
            """
        ).fetchall()
    }


def _validate_a1_database(path: Path) -> None:
    """Confirm that the source is the expected Assignment 1 database."""
    con = duckdb.connect(str(path), read_only=True)
    try:
        tables = _table_names(con)
    finally:
        con.close()

    missing = REQUIRED_A1_TABLES - tables
    if missing:
        raise RuntimeError(
            "Assignment 1 database is missing required tables: "
            + ", ".join(sorted(missing))
        )


def _apply_a2_schema(path: Path) -> tuple[int, int]:
    """Apply the A2 DDL and return (charger count, total table count)."""
    ddl = config.SCHEMA_A2_SQL.read_text(encoding="utf-8")
    con = duckdb.connect(str(path))
    try:
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute(ddl)
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        charger_count = con.execute("SELECT count(*) FROM charger").fetchone()[0]
        table_count = len(_table_names(con))
        return charger_count, table_count
    finally:
        con.close()


def initialise_database(reset: bool = False) -> Path:
    """Create/upgrade the A2 database and return its path.

    A reset is built in a temporary sibling file first.  The existing A2 database
    is replaced only after the A1 copy has passed validation and the complete A2
    schema has been applied successfully.
    """
    _require_file(config.A1_DB, "Assignment 1 database")
    _require_file(config.SCHEMA_A2_SQL, "Assignment 2 schema")
    _validate_a1_database(config.A1_DB)

    config.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    create_fresh = reset or not config.DB_PATH.exists()

    if create_fresh:
        building = config.DB_PATH.with_name(config.DB_PATH.name + ".building")
        if building.exists():
            building.unlink()
        try:
            shutil.copy2(config.A1_DB, building)
            charger_count, table_count = _apply_a2_schema(building)
            building.replace(config.DB_PATH)
        finally:
            if building.exists():
                building.unlink()
        action = "Reset" if reset else "Created"
    else:
        # Reapplying an idempotent schema adds new objects without deleting events.
        charger_count, table_count = _apply_a2_schema(config.DB_PATH)
        action = "Updated"

    print(f"{action} {config.DB_PATH.relative_to(config.PROJECT_ROOT)}")
    print(f"  {charger_count:,} Assignment 1 chargers; {table_count} tables")
    return config.DB_PATH


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--reset",
        action="store_true",
        help="discard the A2 working database and rebuild it from the A1 copy",
    )
    args = parser.parse_args()
    initialise_database(reset=args.reset)


if __name__ == "__main__":
    main()
