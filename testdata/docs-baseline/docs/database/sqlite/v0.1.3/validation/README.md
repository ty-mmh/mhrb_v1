# Validation

```bash
python validation/generate_schemas.py --check
python validation/validate_migrations.py --check
python validation/validate_semantics.py --head all --check
python validation/generate_manifest.py --check
```

- `generate_schemas.py`: migrationからv10／v11／v12／v13 artifactを決定的に生成・照合する。
- `validate_migrations.py`: Goose形式、Up／Down方針、STRICT／WITHOUT ROWID、全schema headを検証する。
- `validate_semantics.py`: 各headの既存制約、v12のassertion identity／integrity envelope制約、v13のCOVR-02進捗索引を実行検証する。
- `generate_manifest.py`: closed bundle allowlist、hash、schema summaryを生成・照合する。

全scriptはPython標準`sqlite3`だけを使用する。Go runtime側のmigration／schema contract testも独立して通す。
