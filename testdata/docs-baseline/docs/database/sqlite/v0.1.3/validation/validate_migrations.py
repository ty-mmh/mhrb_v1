from __future__ import annotations

import argparse
import json
import pathlib
import sqlite3
import sys


BASE = pathlib.Path(__file__).resolve().parents[1]
MIGRATIONS = BASE / "migrations"
REPORT = BASE / "reports" / "migration_smoke_report.json"
AGGREGATE = BASE / "reports" / "validation_report.json"


def split_parts(text: str) -> tuple[str, str]:
    up_marker = "-- +goose Up"
    down_marker = "-- +goose Down"
    if text.count(up_marker) != 1 or text.count(down_marker) != 1:
        raise ValueError("each migration must contain exactly one Goose Up and Down marker")
    up = text.split(up_marker, 1)[1].split(down_marker, 1)[0]
    down = text.split(down_marker, 1)[1]
    return up, down


def connect() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


def migration_files() -> list[pathlib.Path]:
    files = sorted(MIGRATIONS.glob("*.sql"))
    if len(files) != 13:
        raise ValueError(f"expected 13 migrations, found {len(files)}")
    for sequence, path in enumerate(files, 1):
        if not path.name.startswith(f"{sequence:05d}_"):
            raise ValueError(f"migration sequence is not contiguous at {path.name}")
    return files


def apply_up(connection: sqlite3.Connection, files: list[pathlib.Path]) -> None:
    for path in files:
        up, _ = split_parts(path.read_text(encoding="utf-8"))
        connection.executescript("BEGIN;\n" + up + "\nCOMMIT;")


def apply_down(connection: sqlite3.Connection, files: list[pathlib.Path]) -> None:
    for path in reversed(files):
        _, down = split_parts(path.read_text(encoding="utf-8"))
        connection.executescript("BEGIN;\n" + down + "\nCOMMIT;")


def summary(connection: sqlite3.Connection, head: int) -> dict:
    application_tables = connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()
    table_names = {row[0] for row in application_tables}
    table_list = connection.execute("PRAGMA table_list").fetchall()
    strict = {row[1]: row[5] for row in table_list if row[1] in table_names}
    without_rowid = {row[1]: row[4] for row in table_list if row[1] in table_names}
    quick = connection.execute("PRAGMA quick_check").fetchone()[0]
    foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
    return {
        "head": head,
        "application_tables": len(application_tables),
        "named_indexes": connection.execute("SELECT count(*) FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'").fetchone()[0],
        "triggers": connection.execute("SELECT count(*) FROM sqlite_master WHERE type='trigger'").fetchone()[0],
        "all_application_tables_strict": all(value == 1 for value in strict.values()),
        "strict_tables": sum(strict.values()),
        "without_rowid_tables": sum(without_rowid.values()),
        "quick_check": quick,
        "foreign_key_violations": len(foreign_keys),
    }


def generated_document() -> tuple[dict, dict]:
    files = migration_files()
    v10 = connect()
    v11 = connect()
    v12 = connect()
    v13 = connect()
    down = connect()
    try:
        apply_up(v10, files[:10])
        apply_up(v11, files[:11])
        apply_up(v12, files[:12])
        apply_up(v13, files)
        apply_up(down, files[:10])
        apply_down(down, files[:10])
        v10_summary = summary(v10, 10)
        v11_summary = summary(v11, 11)
        v12_summary = summary(v12, 12)
        v13_summary = summary(v13, 13)
        down_remaining_tables = down.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchone()[0]
    finally:
        v10.close()
        v11.close()
        v12.close()
        v13.close()
        down.close()

    smoke = {
        "current_head": 13,
        "reversible_head": 10,
        "migration_files": [path.name for path in files],
        "up_migrations_ok": True,
        "down_migrations_ok": down_remaining_tables == 0,
        "down_remaining_application_tables": down_remaining_tables,
        "forward_only_down_rejected": [path.name for path in files if "migration-policy=forward-only" in path.read_text(encoding="utf-8")],
        "v10": v10_summary,
        "v11": v11_summary,
        "v12": v12_summary,
        "v13": v13_summary,
    }
    aggregate = {
        "current_schema": "v13",
        "current_schema_head": 13,
        "reversible_head": 10,
        "schema_heads": {"v10": v10_summary, "v11": v11_summary, "v12": v12_summary, "v13": v13_summary},
        "semantic_reports": {
            "v10": "reports/validation_report.semantic_v10.json",
            "v11": "reports/validation_report.semantic_v11.json",
            "v12": "reports/validation_report.semantic_v12.json",
            "v13": "reports/validation_report.semantic_v13.json",
        },
    }
    return smoke, aggregate


def write_or_check(path: pathlib.Path, document: dict, mode: str) -> bool:
    body = json.dumps(document, ensure_ascii=False, indent=2) + "\n"
    if mode == "write":
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8", newline="\n")
        return True
    if not path.exists() or path.read_text(encoding="utf-8") != body:
        print(f"{path}: generated content differs", file=sys.stderr)
        return False
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--write", action="store_true")
    group.add_argument("--check", action="store_true")
    args = parser.parse_args()
    try:
        smoke, aggregate = generated_document()
        mode = "write" if args.write else "check"
        ok = write_or_check(REPORT, smoke, mode)
        ok = write_or_check(AGGREGATE, aggregate, mode) and ok
        for head in ("v10", "v11", "v12", "v13"):
            value = smoke[head]
            if value["quick_check"] != "ok" or value["foreign_key_violations"] != 0 or not value["all_application_tables_strict"]:
                print(f"{head}: SQLite health check failed: {value}", file=sys.stderr)
                ok = False
        return int(not ok)
    except (OSError, ValueError, sqlite3.Error) as exc:
        print(f"validate_migrations: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
