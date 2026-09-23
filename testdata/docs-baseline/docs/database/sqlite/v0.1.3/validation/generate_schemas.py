from __future__ import annotations

import argparse
import hashlib
import os
import pathlib
import sys
import tempfile


BASE = pathlib.Path(__file__).resolve().parents[1]
MIGRATIONS = BASE / "migrations"
ARTIFACTS = {
    "v10": BASE / "schema_v10_v0.1.3.sql",
    "v11": BASE / "schema_v11_v0.1.3.sql",
    "v12": BASE / "schema_v12_v0.1.3.sql",
    "v13": BASE / "schema_v13_v0.1.3.sql",
}
EXPECTED_V10_SHA256 = "9e5ab0322f8049816678554bd3529d4a7b33f43397b25f683a07b16de0ba2671"
EXPECTED_V11_SHA256 = "d7b1fe5c0e4560dc83d4bdcac7b0b6a57ca0866df05e438937cc8986678f373e"
EXPECTED_V12_SHA256 = "d1947ff92c92927255d1a06a8872f4a636faa62ec319724b7b0f90de920351bc"


def migration_up(path: pathlib.Path) -> bytes:
    raw = path.read_bytes()
    if b"\r" in raw:
        raise ValueError(f"{path.name}: CRLF is not permitted")
    text = raw.decode("utf-8")
    if text.count("-- +goose Up") != 1 or text.count("-- +goose Down") != 1:
        raise ValueError(f"{path.name}: expected exactly one Goose Up and Down marker")
    up_marker = "-- +goose Up"
    down_marker = "-- +goose Down"
    up = text.split(up_marker, 1)[1].split(down_marker, 1)[0]
    up = up.rstrip("\n") + "\n\n"
    return f"-- {path.name}".encode("utf-8") + up.encode("utf-8")


def generated(head: str) -> bytes:
    if head not in ("v10", "v11", "v12", "v13"):
        raise ValueError(f"unknown schema head: {head}")
    files = sorted(MIGRATIONS.glob("*.sql"))
    if len(files) != 13:
        raise ValueError(f"expected 13 migrations, found {len(files)}")
    for expected, path in enumerate(files, 1):
        if not path.name.startswith(f"{expected:05d}_"):
            raise ValueError(f"migration sequence is not contiguous at {path.name}")
    limit = {"v10": 10, "v11": 11, "v12": 12, "v13": 13}[head]
    body = b"".join(migration_up(path) for path in files[:limit]).rstrip(b"\n") + b"\n"
    return body


def atomic_write(path: pathlib.Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def check_or_write(mode: str) -> int:
    failures = 0
    for head, path in ARTIFACTS.items():
        expected = generated(head)
        expected_hash = hashlib.sha256(expected).hexdigest()
        if mode == "write":
            atomic_write(path, expected)
            continue
        actual = path.read_bytes() if path.exists() else None
        actual_hash = hashlib.sha256(actual).hexdigest() if actual is not None else "<missing>"
        if actual != expected:
            print(f"{path}: generated SHA-256 {expected_hash}, actual {actual_hash}", file=sys.stderr)
            failures += 1
    if mode == "check":
        v10 = ARTIFACTS["v10"].read_bytes()
        actual_v10 = hashlib.sha256(v10).hexdigest()
        if actual_v10 != EXPECTED_V10_SHA256:
            print(f"{ARTIFACTS['v10']}: v10 baseline SHA-256 {actual_v10}, want {EXPECTED_V10_SHA256}", file=sys.stderr)
            failures += 1
        v11 = ARTIFACTS["v11"].read_bytes()
        actual_v11 = hashlib.sha256(v11).hexdigest()
        if actual_v11 != EXPECTED_V11_SHA256:
            print(f"{ARTIFACTS['v11']}: v11 baseline SHA-256 {actual_v11}, want {EXPECTED_V11_SHA256}", file=sys.stderr)
            failures += 1
        v12 = ARTIFACTS["v12"].read_bytes()
        actual_v12 = hashlib.sha256(v12).hexdigest()
        if actual_v12 != EXPECTED_V12_SHA256:
            print(f"{ARTIFACTS['v12']}: v12 baseline SHA-256 {actual_v12}, want {EXPECTED_V12_SHA256}", file=sys.stderr)
            failures += 1
    return failures


def main() -> int:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--write", action="store_true")
    group.add_argument("--check", action="store_true")
    args = parser.parse_args()
    try:
        return check_or_write("write" if args.write else "check")
    except (OSError, UnicodeError, ValueError) as exc:
        print(f"generate_schemas: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
