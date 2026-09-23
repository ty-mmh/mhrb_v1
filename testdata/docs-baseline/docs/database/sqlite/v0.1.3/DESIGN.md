# まほろば SQLite DDL・Migration設計 v0.1.3

**Status:** physical design / embedded migration implemented / resident-scope audited / validated
**Supersedes:** まほろば SQLite DDL・Migration設計 v0.1.2
**Database:** SQLite 3.37.0+（`STRICT` table必須）
**Migration tool:** goose v3 / embedded sequential SQL migrations
**Parent specifications:**

- まほろば 概念設計 v0.2.14（pre-DDL freeze）
- まほろば データモデル概念設計 v0.2.14（pre-DDL freeze）
- まほろば Runtime実行モデル v0.1.2

**Companion artifacts:**

- `migrations/00001_...00013_...sql`
- `schema_v10_v0.1.3.sql`
- `schema_v11_v0.1.3.sql`
- `schema_v12_v0.1.3.sql`
- `schema_v13_v0.1.3.sql`
- `reports/validation_report.json`
- `mahoroba_resident_scope_reference_inventory_v0.1.md / .csv`
- `mahoroba_constraint_enforcement_matrix_v0.1.1.md / .csv`

---

## 0. この文書の位置づけ

本書は、まほろばの静的設計とRuntime実行モデルを、初期SQLite実装で実行可能な物理DDLとmigration運用へ落とす。

扱うもの:

- SQLite物理型・nullable・主キー・外部キー・CHECK制約
- `STRICT` / `WITHOUT ROWID`の適用方針
- Canonical / Write-once / Projection / Operational tableの物理配置
- 索引
- append-only / erasure-only trigger
- goose migrationの分割、適用、rollback、forward migration方針
- 接続PRAGMA、writer / reader接続の役割
- bootstrapとschema migrationの分離
- Constraint Enforcement Matrix
- DDL・hash・erasure・migrationの検証条件

扱わないもの:

- GoのRepository実装
- Canonical Writerのcommand型とSQL文生成
- memory policyの具体値
- Projection計算アルゴリズム
- UI / API設計
- PostgreSQL DDL
- resident branch物理コピー
- FTS / vector / ClickHouseの初期導入

> **本書のDDLは、まほろばの意味規則をSQLite単体で可能な限り強制する。ただし、複数行・複数table・policy・hash計算へまたがる不変条件はCanonical Writer / Projection Coordinatorへ残す。**

---

## 1. 成果物

### 1.1 Migration files

```text
migrations/
├─ 00001_canonical_core.sql
├─ 00002_resident_revisions_and_lifecycle.sql
├─ 00003_content.sql
├─ 00004_versions_generation_recall.sql
├─ 00005_events.sql
├─ 00006_memory.sql
├─ 00007_projections.sql
├─ 00008_operational.sql
├─ 00009_indexes.sql
├─ 00010_immutability_triggers.sql
├─ 00011_m7_slice0.sql
├─ 00012_m7_integrity_foundation.sql
└─ 00013_covr02_progress_indexes.sql
```

各fileは`-- +goose Up` / `-- +goose Down`を持つ。migrations 1〜10、1〜11、1〜12、1〜13のUp全体をそれぞれhead-qualified schema artifactとして同梱する。v10／v11／v12 artifactとmigration `00001`〜`00012`は既存bundleからbyte不変であり、current schema headは13である。

### 1.2 Table count

初期baseline:

```text
Application tables                  38
Goose-managed schema_migrations      1
--------------------------------------
Initial mandatory total             39
```

`activity_sessions`は初期baselineではmaterializeしない。必要になった時点でProjection migrationとして追加する。

### 1.3 実行検証

本書付属SQLは、ローカル検証環境で次を通過した。

```text
SQLite version                     3.46.1
PRAGMA quick_check                 ok
Foreign key violations             0
Application tables                 39
Named indexes                      59
Indexes incl. autoindexes         103
Triggers                           82
All application tables STRICT      True
WITHOUT ROWID tables               5
Resident-scope reference families  38
Resident-scope DDL triggers        21
Negative/positive semantic tests   29 / 29 passed
Append-only UPDATE blocked         True
Content present -> erased update   True
Migrations 1-10 Down smoke         ok
Migrations 11-13 Down              forward-only rejected
```

この検証は物理DDLの構文・基本制約に加え、stage一意性、Event Hash一意性、resident-scope参照の拒否と、意図的なcross-principal参照の許可を確認する。Runtime不変条件全体の証明ではない。

---

## 2. 本物理設計で固定するSQLite判断

| ID | 判断 |
|---|---|
| SQL-D1 | 初期databaseはSQLite 3.37.0以上を要求し、アプリケーションtableをすべて`STRICT`にする |
| SQL-D2 | 単一ULID主キーのentity tableは通常のrowid tableとする |
| SQL-D3 | 複合主キーだけで識別される対応・Projection metadata tableは`WITHOUT ROWID`を選択できる |
| SQL-D4 | SQLite / PostgreSQL共通論理型を優先し、SQLite固有の意味をCanonical schemaへ持ち込まない |
| SQL-D5 | Canonical tableはDB triggerでUPDATE / DELETEを拒否する |
| SQL-D6 | `content_objects`だけは`present -> erased`の制御されたUPDATEを許し、それ以外を拒否する |
| SQL-D7 | `blobs`はUPDATEを拒否するが、到達不能確認後の物理GCとしてDELETEを許す |
| SQL-D8 | Projection tableからCanonical tableへの参照は論理参照とし、物理FKを張らない |
| SQL-D9 | Operational watermark dependencyはProjection Store内のwatermarkへだけ物理FKを持つ |
| SQL-D10 | `schema_migrations`はGoose管理とし、アプリケーションmigrationからCREATEしない |
| SQL-D11 | 初期baselineへFTS5 / vector index / `activity_sessions` materializationを入れない |
| SQL-D12 | bootstrap用Canonical dataをschema migrationへ混ぜない |
| SQL-D13 | release済みdatabaseはforward-only migrationを原則とし、Downは開発・検証専用とする |
| SQL-D14 | privacy上のbest-effortとしてwrite connectionで`secure_delete=ON`を使うが、Canonical erasure completionとは分離する |
| SQL-D15 | claim stageは単調であるため、同一claimの各`to_stage`への到達をUNIQUEで一度に制限する。statusは非単調なので同型制約を置かない |
| SQL-D16 | resident所有データの単純なcross-table参照はSQLite triggerでfail closedに検査し、principal参加・branch lineage等の意図的cross-scopeは許可する |
| SQL-D17 | Event HashはCanonical Store全体で`algorithm + domain + digest`を一意とし、multi-user isolationとは別の実装異常検出軸として扱う |

---

## 3. SQLite接続プロファイル

### 3.1 Writer connection

Canonical Writerは専用`*sql.DB`または専用connection poolを持ち、初期実装では次を要求する。

```text
MaxOpenConns = 1
MaxIdleConns = 1
transaction start = IMMEDIATE
```

接続時PRAGMA:

```sql
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA busy_timeout = 5000;
PRAGMA trusted_schema = OFF;
PRAGMA secure_delete = ON;
```

`modernc.org/sqlite`の概念DSN例:

```text
file:/var/lib/mahoroba/mahoroba.db
  ?_pragma=foreign_keys(1)
  &_pragma=journal_mode(WAL)
  &_pragma=synchronous(FULL)
  &_pragma=busy_timeout(5000)
  &_pragma=trusted_schema(OFF)
  &_pragma=secure_delete(ON)
  &_txlock=immediate
  &_dqs=0
```

実装ではURL encodingとdriverの固定versionに合わせて組み立てる。接続後に各PRAGMAを読み返し、未知PRAGMAの黙殺や設定不一致を検出する。

### 3.2 Reader connection

Readerはwriterと別の`*sql.DB`を使用してよい。

```sql
PRAGMA foreign_keys = ON;
PRAGMA trusted_schema = OFF;
PRAGMA query_only = ON;
PRAGMA busy_timeout = 5000;
```

ReaderはCanonical mutationを実行しない。WALによりwriter commit中でも既存snapshotを読めるが、長時間reader transactionを保持してcheckpointを妨げない。

### 3.3 New database initialization

`auto_vacuum=INCREMENTAL`はtable作成前に有効化する必要がある。そのためGooseがmigration tableを作る前に、空databaseへ一度だけ実行する。

```text
1. database fileをopen
2. sqlite_schemaにuser tableが無いことを確認
3. PRAGMA auto_vacuum = INCREMENTAL
4. Goose migration table名をschema_migrationsへ設定
5. goose.Up(...)
6. PRAGMA foreign_key_check
7. PRAGMA quick_check
8. PRAGMA optimize
```

既存databaseで`auto_vacuum=NONE`から変更する操作は通常migrationとして自動実行しない。変更には`VACUUM`を伴うため、明示的maintenanceとbackupを要求する。

### 3.4 Local filesystem requirement

初期SQLite profileはローカルfilesystem上のdatabaseを前提とする。network filesystem上のWAL共有運用は対象外。

---

## 4. 論理型からSQLite型への変換

| 論理型 | SQLite | 物理制約 |
|---|---|---|
| `ID` | `TEXT` | uppercase 26-char ULID CHECK、zero ULID不使用 |
| `Instant` | `INTEGER` | Unix epoch microseconds |
| `Timezone` | `TEXT` | non-empty。IANA validationはGo側 |
| `Seq` / `CommitSeq` | `INTEGER` | 正数、UNIQUE、単調性はWriter |
| `Ordinal` / `Count` | `INTEGER` | `>= 0`または用途別下限 |
| `Bool` | `INTEGER` | `IN (0, 1)` |
| `RawText` | `TEXT` | SQLiteはbyte-for-byte Unicode normalizationを強制しない。Go側で無加工保存 |
| `Symbol` / `VersionKey` | `TEXT` | CHECK値域またはnon-empty |
| `CanonicalJSON` | `TEXT` | `json_valid()`、JCS・整数mappingはGo側 |
| `Hash` / `Commitment` | `BLOB` | `length(...) = 32` |
| `Blob` | `BLOB` | 任意byte列 |
| `Ratio` | `INTEGER` | `0..1_000_000` |
| `Weight` | `INTEGER` | `>=0`、`1.0 = 1_000_000` |
| `Score` | `REAL` | Projectionだけで使用 |
| `Duration` | `INTEGER` | microseconds |
| `Money` | `INTEGER` | micro-unit、`>=0` |

### 4.1 ULID CHECK

SQLite CHECKは次を検証する。

- 長さ26
- uppercase
- 先頭文字`0..7`
- Crockford Base32許可文字のみ

完全なULID parser、overflow、zero ULID拒否はGo側でも再検証する。

### 4.2 CanonicalJSON CHECKの限界

SQLiteでは`json_valid()`までを強制する。次はGoのCanonical Serializerが担う。

- RFC 8785 JCS
- `mahoroba-jcs-v1`
- duplicate key禁止
- Unicode非正規化
- exact integerの厳密なdecimal string mapping
- `Score`の直接混入禁止

---

## 5. Table option方針

### 5.1 `STRICT`

38のアプリケーションtableをすべて`STRICT`にする。

利点:

- 型違反を早期に拒否
- SQLiteの柔軟typingによる意図しない値の混入を抑制
- `PRAGMA quick_check`でSTRICT型違反も検出可能

Gooseが管理する`schema_migrations`はGooseのtable定義へ従い、アプリケーションSTRICT保証の対象外とする。

### 5.2 `WITHOUT ROWID`

初期DDLで`WITHOUT ROWID`を使うtable:

```text
blobs
resident_current_revision
content_references
projection_watermarks
projection_watermark_dependencies
```

選定基準:

- 複合主キーが行identityそのもの
- 別のsingle-column ULID identityを持たない
- 主キーによる検索が中心

その他は通常rowid tableとする。ULID TEXT主キーを持つtableを一律`WITHOUT ROWID`にはしない。初期規模では書込・索引特性を実測してから変更する。

---

## 6. Migration構成

| Migration | 主なtable / object |
|---|---|
| `00001_canonical_core` | `canonical_commits`, `principals`, `residents` |
| `00002_resident_revisions_and_lifecycle` | revision / approval / activation / resident status |
| `00003_content` | `blobs`, `content_objects`, `content_erasure_events` |
| `00004_versions_generation_recall` | pipeline, sessionization policy, recall, generation |
| `00005_events` | `events` |
| `00006_memory` | claims, evidence, stage/status, relations, findings, usages |
| `00007_projections` | current state, claim state, content refs, watermarks |
| `00008_operational` | `runtime_config`, analytics/search outbox |
| `00009_indexes` | 初期索引 |
| `00010_immutability_triggers` | append-only / write-once trigger |
| `00011_m7_slice0` | claim identity erasure pairingとCanonical entity/commit identity |
| `00012_m7_integrity_foundation` | M7 finding envelope、assertion identity、outcome latest index |
| `00013_covr02_progress_indexes` | COVR-02 re-extractionのcommit内run keyset index |

依存関係のため、順序を変更しない。

### 6.1 v12 successor contract

`00012`はlegacy findingをrewriteせず、`integrity_findings`へ次のnullable列を末尾追加する。

```text
finding_fingerprint BLOB(32)
rule_code
target_kind
target_id
target_field
```

`integrity_check` pipelineが追加するrowだけはtriggerで全5列を必須とする。`(resident_id, finding_fingerprint)`はnon-NULL rowで一意とし、legacy pipelineのNULL rowは維持できる。

claim validity／view-scope assertionはそれぞれ`(claim_id, canonical_commit_id)`で一意とする。runtime migrationはv11以前の重複を最初のmutation前に検出し、自動でwinnerを選ばずstatic repair要求として停止する。DDLのunique indexもruntime preflightを迂回した適用をfail closedにする。

generation outcomeのlogical latestはrowidやrecorded timeへ依存せず、`idx_generation_run_outcomes_latest_v12(generation_run_id, attempt_no DESC, state, outcome_id)`を利用できる。旧indexは既存query互換のため残す。

### 6.2 v13 COVR-02 progress contract

`00013`はCanonical rowを変更せず、`idx_generation_runs_commit_resident_purpose_run(canonical_commit_id, resident_id, purpose, generation_run_id)`だけを追加する。re-extractionは`idx_canonical_commits_resident_seq`でresidentのcommitをfixed ceiling以下にpageし、対象commit内だけをこの索引で`generation_run_id` keyset走査する。これにより全run join／sortを避け、commit pageとrun pageの双方を物理的にboundedにする。

---

## 7. Table catalog

### 7.1 Canonical `[C]`

```text
canonical_commits
principals
residents
resident_revisions
resident_revision_approvals
resident_revision_activations
resident_status_transitions
content_erasure_events
pipeline_versions
sessionization_policy_versions
recall_runs
generation_runs
generation_run_inputs
generation_run_outcomes
events
claims
claim_evidence
claim_stage_transitions
claim_stage_transition_dependencies
claim_relations
integrity_findings
claim_status_transitions
claim_validity_assertions
claim_view_scope_assertions
claim_usages
```

`canonical_commits`を除くすべての[C] tableは`canonical_commit_id NOT NULL`を持つ。

### 7.2 Write-once `[W]`

```text
blobs
content_objects
```

- `blobs`: resident-scoped content-addressed store
- `content_objects`: commitment、salt、erasure policy/state

### 7.3 Projection `[P]`

```text
resident_current_revision
resident_current_status
claim_states
claim_view_scope_current
runtime_states
content_references
```

Canonicalへの物理FKを持たない。Projection rebuild中に一時的に空またはstaleになり得る。

### 7.4 Operational `[O]`

```text
projection_watermarks
projection_watermark_dependencies
runtime_config
analytics_outbox
search_outbox
schema_migrations   -- Goose managed
```

`activity_sessions`は初期baselineに含めず、必要時にProjection migrationとして追加する。

---

## 8. 主キー・外部キーの原則

### 8.1 Primary key

- Canonical entity: ULID `TEXT PRIMARY KEY`
- association / ordered input: composite PK
- singleton config: `config_id = 1`

### 8.2 Foreign key

すべてのFK検査を`PRAGMA foreign_keys=ON`で有効化する。

循環bootstrapを扱うFKには`DEFERRABLE INITIALLY DEFERRED`を使用する。代表例:

```text
canonical_commits.resident_id -> residents.resident_id
residents.canonical_commit_id  -> canonical_commits.canonical_commit_id
residents.principal_id         -> principals.principal_id
```

一つのCanonical UoW内でcommit、principal、residentを挿入し、transaction commit時に整合する。

### 8.3 Delete action

Canonical FKへ`ON DELETE CASCADE`を使わない。Canonical row自体を削除しないためである。

Projection Store内の従属metadataだけ、`projection_watermark_dependencies -> projection_watermarks`に`ON DELETE CASCADE`を許可する。

---

## 9. 値域・CHECK制約

DDLで固定する代表値域:

```text
event_type         user_message / resident_message / self_talk / outbound_initiative
ingress            local_ui / resident_runtime
trust_level        trusted / untrusted
revision_class     principles / persona / memory_policy
resident status    draft / active / archived / erased
claim kind         direct / other / meta / NULL
claim stage        floating / sediment / settled
claim status       active / invalidated / superseded / quarantined
polarity           support / contradict
grade              stated / observed / inferred
derivation         extracted / inherited
view_scope          resident_ui / admin_only
usage_type          candidate / selected / prompt_included / explicitly_referenced
erasure_policy      independent / resident_only
erasure_state       present / erased
decision_kind       human / automatic
```

初期event mappingはCHECKでさらに制約する。

```text
user_message
  -> ingress=local_ui
  -> actor human
  -> trust=trusted

resident_message / self_talk / outbound_initiative
  -> ingress=resident_runtime
  -> actor resident
  -> trust=trusted
```

actor principal kindとの一致は複数table参照になるためWriterが検査する。DDLはevent_type / ingress / trustの組を制約する。

---

## 10. Immutability trigger

### 10.1 Canonical append-only

すべての[C] tableへ、UPDATE / DELETEを拒否するBEFORE triggerを作成する。

```sql
CREATE TRIGGER trg_events_no_update
BEFORE UPDATE ON events
BEGIN
  SELECT RAISE(ABORT, 'events is append-only');
END;
```

SQLiteの権限モデルだけではtable単位のUPDATE禁止を十分表現できないため、triggerをdefense-in-depthとして使う。

### 10.2 `content_objects`

許可する唯一の意味変更:

```text
present -> erased
blob_hash       non-NULL -> NULL
commitment_salt non-NULL -> NULL
```

次は不変:

- content identity
- owner resident
- content class
- commitment
- hash algorithm / domain
- canonicalization version
- erasure policy
- created time

DELETEは常に拒否する。

### 10.3 `blobs`

UPDATEは拒否する。DELETEはRuntimeの安全条件を通過した物理GCだけが使用する。

### 10.4 Resident-scope参照trigger

resident独立性について、immutable parent rowを主キーで参照しresident IDの等値を判定できる参照は、`NOT EXISTS`型の`BEFORE INSERT` triggerでfail closedに検査する。

v0.1.2では21 triggerを追加し、次のfamilyを覆う。

```text
resident definition / revision / approval / activation / lifecycle
content erasure
recall / generation envelope / input content / outcome
event ledger
claim / evidence / stage / stage dependency / relation
integrity finding / status / validity / view scope / usage
```

親rowが未挿入の場合も、residentが不一致の場合も拒否する。`generation_run_inputs.source_type / source_id`のpolymorphic provenance、Projection／Operationalのlogical reference、principalのactor／subject／perspectiveはRuntime側または意図的cross-scopeとして扱う。

全38参照familyの正本は`mahoroba_resident_scope_reference_inventory_v0.1.md`とする。

---

## 11. 索引設計

### 11.1 Event / commit

```text
canonical_commits(resident_id, commit_seq)
events(resident_id, event_type, seq)
events(resident_id, recorded_at, seq)
events(actor_principal_id, recorded_at)
```

`UNIQUE(resident_id, seq)`とevent hash chain検査を併用する。

防御的なStore-wide一意性として次を持つ。

```text
UNIQUE(event_hash_algorithm, event_hash_domain, event_hash)
```

これは同一発話の重複排除ではなく、event ID等のhash input取りこぼしを検出する網である。複数user／複数residentでもStore全体で維持する。

### 11.2 Generation / mandatory work rediscovery

```text
generation_runs(resident_id, purpose, requested_at)
generation_run_inputs(generation_run_id, ordinal)
generation_run_outcomes(generation_run_id, attempt_no, recorded_at)
```

Partial unique index:

```text
1 attemptにつきrunning outcomeは最大1
1 attemptにつきterminal outcomeは最大1
```

obligation lookupは`UNIQUE(resident_id, idempotency_key)`で行う。

### 11.3 Memory

```text
claims(owner_resident_id, recorded_at)
claims(subject_principal_id, kind)
claim_evidence(claim_id, polarity)
claim_evidence(event_id)
claim_evidence(source_evidence_id)
claim_stage_transitions(claim_id, recorded_at)
claim_stage_transition_dependencies(dependency_claim_id)
claim_relations(from_claim_id, relation_type)
claim_relations(to_claim_id, relation_type)
claim_status_transitions(claim_id, recorded_at)
claim_usages(claim_id, usage_type, recorded_at)
claim_usages(recall_run_id, usage_type)
```

`dependency_claim_id`索引は、meta claim変更からdirect claim再評価候補を逆引きするため必須。

stageは単調であるため、同じclaimが同じstageへ二度到達することを次で拒否する。

```text
UNIQUE(claim_id, to_stage)
```

`claim_status_transitions`には同型の一意性を置かない。statusは`quarantined -> active -> quarantined`のように再訪可能である。

### 11.4 Projection / outbox

```text
claim_states(resident_id, status, stage)
claim_states(resident_id, salience DESC)
content_references(owner_resident_id, blob_hash_algorithm, blob_hash)
projection_watermarks(source_commit_seq)
analytics_outbox(status, available_at)
search_outbox(status, available_at)
```

実装開始後に`EXPLAIN QUERY PLAN`でmandatory rediscovery、Recall、Projection catch-up、GC queryを確認する。

---

## 12. Constraint Enforcement Matrix

不変条件と実装責任の正本は、独立成果物 **まほろば Constraint Enforcement Matrix v0.1.1** とする。

Matrixは、静的`I-1〜I-106`、Runtime`RT-I-1〜RT-I-31`、採用済みU系防御を、次へ対応づける。

```text
Invariant ID
Primary Enforcer
Secondary / defense-in-depth
DDL object / Runtime component
Positive / negative Test Family
実装状態
実装Phase
```

本DDL設計は、そのうちSQLiteが担う制約を物理objectへ落とす。v0.1.2で追加・更新した主要行は次。

| ID / family | DB object | Runtime側 | Test |
|---|---|---|---|
| U-1 / stage到達一意性 | `uq_claim_stage_transitions_stage` | current stage / gate再検査 | duplicate stage拒否 |
| I-14 / resident scope | resident-scope trigger 21本 | commit scope、polymorphic source、Projection scope | cross-resident fixture拒否 |
| U-4 / Event Hash一意性 | `uq_events_event_hash` | hash再計算 | duplicate hash拒否 |
| RT-I-8 / attempt outcome一意性 | partial UNIQUE 2本 | — | duplicate running/terminal拒否 |
| I-104 / ledger time | — | Canonical Writer | monotonic/restart property |

DBで表現可能でも、複数tableの意味・policy解決・hash計算・permissionに依存する規則はRuntimeへ委譲する。その場合もMatrixに責任主体とnegative testを必ず持たせ、`N/A`を含め未割当を許さない。

resident-scopeの詳細な参照棚卸しは、Matrixと対になる`Resident-Scope Reference Inventory v0.1`を正本とする。

---

## 13. Canonical WriterとDDLの接続

一つのCanonical UoWの概念手順:

```text
BEGIN IMMEDIATE

1. next commit_seqを決定
2. ledger_timeを一度だけ決定
3. canonical_commitsを挿入
4. purpose別Canonical rowを挿入
5. eventがあればresident seqとhashを確定
6. outbox行を必要に応じて挿入
7. FK / invariant再検査

COMMIT
```

同一UoWに属するすべてのtransaction time列へ同じ`ledger_time`を書く。

`canonical_commits.committed_at`から起動時の`last_ledger_time`を復元できるよう、commit内で二度目のledger timeを採番しない。

---

## 14. Projection transaction

Projection更新では、Projection本体、watermark、watermark dependenciesを同じtransactionで更新する。

```text
BEGIN

- required full build/rebuild/update成分を実行
- Projection本体を更新
- dependency集合を置換・検査
- 実際に完了したcommit catch-up / as-of re-evaluationに応じてcursorを進める

COMMIT
```

Canonical tableへの物理FKを持たないため、Projection update前に対象Canonical IDの存在を論理検証する。

`projection_watermark_dependencies`は親watermark削除時にのみ`ON DELETE CASCADE`で消える。

---

## 15. ErasureとSQLite物理削除

### 15.1 Canonical erasure completion

Canonical上の消去完了は、`content_erasure_events`の追記と`content_objects`の`present -> erased`成立で定義する。

物理blob byteの回収は後続Operational処理であり、Canonical commit成功条件へ含めない。

### 15.2 Blob GC

実行条件:

- `content_references`が構築済み
- watermarkがCanonical Headへ到達
- projection versionが現行
- rebuild中でない
- 対象blobへの参照が0

`blobs` DELETE後、maintenance policyに応じて次をbest-effortで行える。

```sql
PRAGMA wal_checkpoint(TRUNCATE);
PRAGMA incremental_vacuum;
```

`secure_delete=ON`を使用するが、WAL、filesystem、snapshot、backup、exportからの物理消滅を保証しない。これは既存の「管理境界外データは消去不能」という原則を物理層へ適用したもの。

### 15.3 FTS導入時の注意

SQLiteのsecure deleteはFTS shadow tableの痕跡を完全には保証しない。初期baselineへFTSを入れない理由の一つとする。FTSを追加する場合は、search indexを再構築可能な別Projection／別DBとして扱うことを優先する。

---

## 16. Goose migration policy

### 16.1 Versioning

初期公開前はsequential migrationを使用する。

```text
00001
00002
...
```

Gooseのmigration table名を`schema_migrations`へ設定する。

### 16.2 Transaction

全13 migrationはGoose標準のtransaction内で実行する。`NO TRANSACTION`を使わない。v10のclaims rebuildに限りruntimeが同一transactionの外側でforeign-key enforcementを一時停止し、完了後に再検査する。

### 16.3 Up / Down

- 開発DB・CI: Up / Down smokeを許可
- resident実データを持つrelease DB: forward-only
- Downはデータ消失を伴い得るため製品UI/APIから公開しない

### 16.4 SQLite schema変更

SQLiteで直接ALTERできない変更は次の手順を使う。

```text
1. backup
2. CREATE new_table
3. INSERT INTO new_table SELECT ...
4. row count / FK / invariant検査
5. DROP old_table
6. ALTER TABLE new_table RENAME TO old_table
7. index / trigger再作成
8. quick_check / foreign_key_check
```

Canonical dataの意味変換が必要なら、SQL migrationだけで隠さず、versioned Go migrationまたは明示的maintenance commandを使う。

### 16.5 Migration startup gate

Runtime起動時:

```text
1. exclusive startup phase
2. migration status確認
3. pending migrationを適用
4. foreign_key_check
5. quick_check
6. schema version確認
7. Projection compatibility確認
8. Canonical Writer / HTTP開始
```

migration失敗時はRuntimeをfail closedで停止する。

---

## 17. Bootstrap data

schema migrationはCanonical dataを挿入しない。

初回bootstrapはRuntimeのCanonical Writerを使い、最低限次をCanonical Commitとして作る。

```text
- system principal
- owner human principal
- initial pipeline version
- initial Sessionization Policy version
- blank resident principal / resident
- principles revision + approval + activation
- persona revision + activation
- memory policy revision + activation
- resident NULL -> draft -> active lifecycle
- runtime_config current selection
```

`runtime_config`はOperationalなのでCanonical Commitに所属しない。bootstrapのCanonical rowと同じtransactionへ混ぜず、Canonical bootstrap成功後に設定する。

しろseedの内容は別artifactであり、本DDLへ埋め込まない。

---

## 18. Golden test vectors

実装前に、最低限次の固定vectorを作る。

| Vector | 検証 |
|---|---|
| ULID | uppercase / invalid char / overflow / zero拒否 |
| Canonical JSON | key順序、整数decimal string、Unicode、duplicate key |
| RawText | CRLF、末尾空白、NFC/NFDが無加工で保持される |
| Blob Hash | logical bytesに対するSHA-256 |
| Commitment | salt + content class + bytes + domain |
| Event Hash | envelope JCS + domain + prev hash |
| Hash chain | seq 1..Nの再計算 |
| Erasure | salt/blob ref破棄後もcommitment/event hashを保持 |
| Fixed point | Ratio / Weight境界 |
| Runtime UoW | 1 commit = 1 ledger time |

vectorはJSONまたはYAML fixtureとしてRepositoryへ置き、Go実装と将来の別実装で共有する。

---

## 19. DDL validation suite

### 19.1 Migration tests

- empty databaseへの全Up
- 全Down（開発専用）
- Up → Down → Up
- migration途中failure時のrollback
-既存versionから次versionへのupgrade

### 19.2 Schema tests

```sql
PRAGMA quick_check;
PRAGMA foreign_key_check;
```

加えて:

- 全application tableがSTRICT
- 期待table / index / trigger集合が一致
- Canonical table UPDATE / DELETEが失敗
- content objectの許可外UPDATEが失敗
- present → erasedだけ成功
- invalid enum / ULID / JSON / digest lengthが失敗
- generation outcome partial unique indexが効く
- 同一claim / to_stageの二重挿入が失敗する
- 同一algorithm / domain / event_hashの二重event挿入が失敗する
- resident-scope trigger対象21 familyのcross-resident insertが失敗する
- `generation_run_inputs.source_type / source_id`のcross-resident provenanceをWriterが拒否する
- principalを介する意図的cross-scope（subject / perspective等）は成功する
- Matrixの全Invariant IDがPrimary EnforcerとTest Familyを持つ

### 19.3 Query-plan tests

代表queryに`EXPLAIN QUERY PLAN`を行う。

- resident event replay
- mandatory dialogue rediscovery
- mandatory memory extraction rediscovery
- memory re-extraction commit／run keyset
- generation latest outcome
- dependency claim逆引き
- Projection catch-up
- content reference GC scan
- outbox pending scan

full table scanが意図されていないqueryはCIで検出する。

---

## 20. 既知の境界・保留

- `activity_sessions`は非materializedが初期既定
- FTS / vectorは別migration
- branch物理コピーは無効
- Projection別SQLite化は未採用
- PostgreSQL adapter DDLは別文書
- trust=`untrusted`を生成する初期ingressはない
- `observed` evidenceは予約値
- relationship modelはない
- schema migrationはpolicy値を決めない
- `secure_delete`は物理消去の保証ではない
- Canonical JSONのJCS適合性はSQLite CHECKだけでは証明できない
- 複数tableに跨る不変条件は、DDL triggerで閉じたものを除きWriter側が最終責任を持つ
- Event HashのStore-wide一意性はmulti-user／multi-resident isolationを意味しない。user×residentの閲覧・Recall隔離は将来のConversation／permission設計で扱う
- branch lineageの`parent_resident_id`、principalのactor／subject／perspectiveは意図的cross-scopeであり、resident-scope triggerの対象外

---

## 21. 実装順序

### Phase SQL-1: Database opening

- writer / reader connection profile
- new database initialization
- PRAGMA verify
- Goose embed / custom migration table

### Phase SQL-2: Migration application

- migrationsをembed
- empty DB Up
- schema validation
- resident-scope trigger / stage UNIQUE / Event Hash UNIQUEのnegative tests
- Matrix coverage検査
- failure時fail closed

### Phase SQL-3: CanonicalStore skeleton

- ID / time / JSON / digest codec
- Canonical Commit
- single ledger time
- principal / resident bootstrap

### Phase SQL-4: Content / Event spine

- BlobStore staging
- content object / commitment
- event hash chain
- append-only verification

### Phase SQL-5: Generation / Memory

- generation envelope / outcome
- claims / evidence / transitions
- mandatory work rediscovery query

### Phase SQL-6: Projection / Erasure

- Projection Store
- watermark / dependencies
- rebuild tests
- erasure / GC safety

---

## 22. Acceptance criteria

本設計を実装へ渡せる条件:

1. 13 migrationが空DBへ適用できる
2. 38 application tableとGoose migration tableが作成される
3. 全application tableがSTRICT
4. FK / quick checkがclean
5. Canonical UPDATE / DELETEが拒否される
6. content erasure遷移だけが許可される
7. migration 1〜10のUp / Down smokeと、11〜13のforward-only拒否が通る
8. 初期bootstrapを一つのWriter flowで実行できる
9. event hash test vectorが一致する
10. mandatory rediscovery queryが索引を使う
11. Projection watermark更新が本体と同一transactionになる
12. GCがstale Projectionでは拒否される
13. 同一claimの同一stage二重到達がDBで拒否される
14. resident-scope参照のDDL対象familyがすべてcross-resident fixtureを拒否する
15. generation inputのpolymorphic source scopeをCanonical Writerが拒否する
16. Event HashのStore-wide重複がDBで拒否される
17. Constraint Enforcement MatrixとResident-Scope Inventoryのcoverage検査がclean
18. v11 populated upgradeでlegacy findingがNULL envelopeのまま保持される
19. assertion identity重複を含むv11 upgradeがschema／rowを変更せずfail closedになる
20. M7 integrity writer guard、fingerprint partial unique、assertion unique、latest outcome indexがschema allowlistとsemantic testで検証される
21. v12 populated upgradeがCanonical rowを変更せずhead 13へ到達し、COVR-02 progress indexがexact schema allowlist／fingerprint／query-plan testで検証される

---

## 23. 参考資料

- SQLite STRICT Tables
- SQLite PRAGMA documentation
- SQLite WAL documentation
- SQLite WITHOUT ROWID documentation
- modernc.org/sqlite package documentation
- pressly/goose v3 documentation

本書のSQLite固有判断は上記公式資料を参照している。resident-scope制約とnegative testは、DDL v0.1監査およびResident-Scope Reference Inventory v0.1を根拠とする。Go依存versionは実装開始時に`go.mod`へ固定し、そのversionのdriver挙動をCIで検証する。
