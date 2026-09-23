# Mahoroba SQLite DDL / Migration v0.1.3

このbundleはSQLite physical design v0.1.2をbyte不変の親とし、M7 Slice 1向けschema reconciliationをmigration `00012`、COVR-02の物理SQL進捗索引をmigration `00013`で追加するsuccessorである。migration `00001`〜`00012`のsource byteは既存bundleから不変とし、current schema headは13とする。

## Contents

- `DESIGN.md`: physical design v0.1.3とv12／v13 delta。
- `migrations/`: Goose SQL migration 1〜13。
- `schema_v10_v0.1.3.sql`: migration 1〜10のUp連結（genuine v10 fixture）。
- `schema_v11_v0.1.3.sql`: migration 1〜11のUp連結（immutable Slice 0 head）。
- `schema_v12_v0.1.3.sql`: migration 1〜12のUp連結（immutable pre-COVR-02 head）。
- `schema_v13_v0.1.3.sql`: migration 1〜13のUp連結（current schema）。
- `reports/`: head-qualified migration／semantic validation結果。
- `validation/`: schema生成、migration／semantic検証、closed manifest生成script。

## v0.1.3 changes

- `integrity_findings`へnullableなM7 envelope 5列とresident内fingerprint partial unique indexを追加する。
- `integrity_check` pipelineの新規findingは5列を全て必須とし、legacy pipeline rowは変更しない。
- generation outcomeのlatest read用covering indexを追加する。
- claim validity／view-scope assertionを`claim_id + canonical_commit_id`で一意化する。
- v11 upgradeの重複assertionは履歴を自動修復せずstatic repair要求でfail closedする。
- re-extractionのcommit内run keyset用に`generation_runs(canonical_commit_id, resident_id, purpose, generation_run_id)`索引を追加する。

## Apply order

1. runtime loaderがembedded migration全fileのbytes／SHA-256／順序を検証する。
2. v11以前のassertion identity重複をpreflightし、重複時はmutation前に停止する。
3. Gooseを固定table名`schema_migrations`でhead 13まで適用する。
4. exact schema allowlist／fingerprint、`foreign_key_check`、`quick_check`を検証する。

`00011`〜`00013`はforward-onlyであり、Canonical履歴の自動repairやDown migrationは行わない。
