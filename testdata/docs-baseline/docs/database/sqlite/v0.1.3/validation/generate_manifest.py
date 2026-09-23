from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import sys
import tempfile


REPOSITORY = pathlib.Path(__file__).resolve().parents[5]
BUNDLE = REPOSITORY / "docs" / "database" / "sqlite" / "v0.1.3"
MANIFEST = BUNDLE / "source-manifest.json"

FILES = [
    "docs/README.md",
    "docs/assurance/mahoroba_constraint_enforcement_matrix_v0.1.1.csv",
    "docs/assurance/mahoroba_constraint_enforcement_matrix_v0.1.1.md",
    "docs/assurance/mahoroba_constraint_enforcement_matrix_v0.1.1_coverage.json",
    "docs/assurance/mahoroba_resident_scope_reference_inventory_v0.1.csv",
    "docs/assurance/mahoroba_resident_scope_reference_inventory_v0.1.md",
    "docs/database/sqlite/v0.1.3/DESIGN.md",
    "docs/database/sqlite/v0.1.3/README.md",
    "docs/database/sqlite/v0.1.3/schema_v10_v0.1.3.sql",
    "docs/database/sqlite/v0.1.3/schema_v11_v0.1.3.sql",
    "docs/database/sqlite/v0.1.3/schema_v12_v0.1.3.sql",
    "docs/database/sqlite/v0.1.3/schema_v13_v0.1.3.sql",
    *[f"docs/database/sqlite/v0.1.3/migrations/{path.name}" for path in sorted((BUNDLE / "migrations").glob("*.sql"))],
    "docs/database/sqlite/v0.1.3/reports/migration_smoke_report.json",
    "docs/database/sqlite/v0.1.3/reports/resident_scope_audit_report.json",
    "docs/database/sqlite/v0.1.3/reports/validation_report.json",
    "docs/database/sqlite/v0.1.3/reports/validation_report.semantic_v10.json",
    "docs/database/sqlite/v0.1.3/reports/validation_report.semantic_v11.json",
    "docs/database/sqlite/v0.1.3/reports/validation_report.semantic_v12.json",
    "docs/database/sqlite/v0.1.3/reports/validation_report.semantic_v13.json",
    "docs/database/sqlite/v0.1.3/validation/README.md",
    "docs/database/sqlite/v0.1.3/validation/generate_manifest.py",
    "docs/database/sqlite/v0.1.3/validation/generate_schemas.py",
    "docs/database/sqlite/v0.1.3/validation/validate_migrations.py",
    "docs/database/sqlite/v0.1.3/validation/validate_semantics.py",
]


def sha256(path: pathlib.Path) -> tuple[int, str]:
    body = path.read_bytes()
    return len(body), hashlib.sha256(body).hexdigest()


def schema_summary(path: pathlib.Path) -> dict[str, int]:
    import sqlite3

    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(path.read_text(encoding="utf-8"))
        return {
            "application_tables": connection.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchone()[0],
            "named_indexes": connection.execute("SELECT count(*) FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'").fetchone()[0],
            "triggers": connection.execute("SELECT count(*) FROM sqlite_master WHERE type='trigger'").fetchone()[0],
        }
    finally:
        connection.close()


def expected_document() -> dict:
    entries = []
    for relative in FILES:
        path = REPOSITORY / pathlib.Path(*relative.split("/"))
        if not path.is_file():
            raise FileNotFoundError(relative)
        size, digest = sha256(path)
        entries.append({"path": relative, "bytes": size, "sha256": digest})
    v10 = REPOSITORY / pathlib.Path(*"docs/database/sqlite/v0.1.3/schema_v10_v0.1.3.sql".split("/"))
    v11 = REPOSITORY / pathlib.Path(*"docs/database/sqlite/v0.1.3/schema_v11_v0.1.3.sql".split("/"))
    v12 = REPOSITORY / pathlib.Path(*"docs/database/sqlite/v0.1.3/schema_v12_v0.1.3.sql".split("/"))
    v13 = REPOSITORY / pathlib.Path(*"docs/database/sqlite/v0.1.3/schema_v13_v0.1.3.sql".split("/"))
    return {
        "artifact": "mahoroba_sqlite_ddl_migration",
        "version": "v0.1.3",
        "current_schema_head": 13,
        "parents": {
            "concept": "v0.2.14",
            "data_model": "v0.2.14",
            "runtime": "v0.1.2",
            "matrix": "v0.1.1",
            "resident_scope_inventory": "v0.1",
        },
        "schema_heads": {
            "v10": {
                "artifact": "docs/database/sqlite/v0.1.3/schema_v10_v0.1.3.sql",
                "head": 10,
                "bytes": sha256(v10)[0],
                "sha256": sha256(v10)[1],
                "migration_prefix": [1, 10],
                **schema_summary(v10),
            },
            "v11": {
                "artifact": "docs/database/sqlite/v0.1.3/schema_v11_v0.1.3.sql",
                "head": 11,
                "bytes": sha256(v11)[0],
                "sha256": sha256(v11)[1],
                "migration_prefix": [1, 11],
                **schema_summary(v11),
            },
            "v12": {
                "artifact": "docs/database/sqlite/v0.1.3/schema_v12_v0.1.3.sql",
                "head": 12,
                "bytes": sha256(v12)[0],
                "sha256": sha256(v12)[1],
                "migration_prefix": [1, 12],
                **schema_summary(v12),
            },
            "v13": {
                "artifact": "docs/database/sqlite/v0.1.3/schema_v13_v0.1.3.sql",
                "head": 13,
                "bytes": sha256(v13)[0],
                "sha256": sha256(v13)[1],
                "migration_prefix": [1, 13],
                **schema_summary(v13),
            },
        },
        "files": entries,
    }


def atomic_write(document: dict) -> None:
    body = (json.dumps(document, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    fd, temporary = tempfile.mkstemp(prefix=f".{MANIFEST.name}.", dir=MANIFEST.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, MANIFEST)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def check_tree(document: dict) -> list[str]:
    bundle_prefix = "docs/database/sqlite/v0.1.3/"
    allowed = {
        path[len(bundle_prefix):].replace("/", os.sep)
        for path in FILES
        if path.startswith(bundle_prefix)
    } | {"source-manifest.json"}
    failures = []
    for path in BUNDLE.rglob("*"):
        if path.is_file():
            relative = str(path.relative_to(BUNDLE))
            if relative not in allowed:
                failures.append(f"unexpected bundle file: {relative}")
    actual_paths = {entry["path"] for entry in document["files"]}
    if actual_paths != set(FILES):
        failures.append("manifest file set differs from the closed allowlist")
    if len(actual_paths) != len(document["files"]):
        failures.append("manifest contains duplicate paths")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--write", action="store_true")
    group.add_argument("--check", action="store_true")
    args = parser.parse_args()
    try:
        document = expected_document()
        if args.write:
            atomic_write(document)
            return 0
        failures = check_tree(document)
        if MANIFEST.exists():
            actual = json.loads(MANIFEST.read_text(encoding="utf-8"))
            if actual != document:
                failures.append("source-manifest.json differs from generated content")
        else:
            failures.append("source-manifest.json is missing")
        for failure in failures:
            print(f"generate_manifest: {failure}", file=sys.stderr)
        return int(bool(failures))
    except (OSError, UnicodeError, ValueError, FileNotFoundError, json.JSONDecodeError) as exc:
        print(f"generate_manifest: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
