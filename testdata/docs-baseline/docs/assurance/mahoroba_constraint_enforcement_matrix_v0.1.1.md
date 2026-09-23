# まほろば Constraint Enforcement Matrix v0.1.1

**Status:** implementation assurance / test specification / pre-implementation  
**Parent specifications:** まほろば 概念設計 v0.2.14 / データモデル概念設計 v0.2.14 / Runtime実行モデル v0.1.2  
**Physical baseline:** SQLite DDL・Migration設計 v0.1.2  
**Resident-scope companion:** Resident-Scope Reference Inventory v0.1  
**Supersedes:** Constraint Enforcement Matrix v0.1

---

## 0. 位置づけ

本書は、静的不変条件 `I-1〜I-106`、Runtime不変条件 `RT-I-1〜RT-I-31`、採用済みU系防御を、強制主体・実装object・検証testへ対応づける横断正本である。resident所有データの全参照familyは、別紙 `Resident-Scope Reference Inventory v0.1` を正本とする。

### 0.1 v0.1.1での同期

- U-1のstage一意性索引をDDL v0.1.2で実装・negative test済み
- U-2a〜cを含むresident-scope triggerを21 familyへ拡張し、DDL v0.1.2で実測済み
- U-2dのpolymorphic generation input scopeはRuntime未実装のまま明示
- U-4のStore-wide Event Hash UNIQUEをDDL v0.1.2で実装・negative test済み
- Matrix coverageとResident-Scope Inventoryの責任境界を同期

## 1. Coverage summary

- Total rows: **144**
- Static invariants: **106**
- Runtime invariants: **31**
- Missing IDs: **0**
- Duplicate IDs: **0**
- Unassigned primary/test/status: **0**

| Status | Rows |
|---|---:|
| `DDL-IMPLEMENTED` | 23 |
| `DEFERRED` | 2 |
| `PLANNED-RUNTIME` | 2 |
| `RUNTIME-SPEC` | 88 |
| `SPECIFIED` | 17 |
| `THIS-DOCUMENT` | 1 |
| `VERIFIED` | 6 |
| `VERIFIED-DDL-0.1.2` | 5 |

## 2. Master Matrix

| ID | Rule | Primary | Secondary | Object / component | Test ID | Level | Status | Phase | Note |
|---|---|---|---|---|---|---|---|---|---|
| I-1 | Canonicalエンベロープは追記のみ。UPDATE / DELETEを行わない | SQLite DDL | CanonicalStore API | trg_*_no_update / trg_*_no_delete | INV-I-1 | SQL negative | VERIFIED | SQL-2 | 現行監査でappend-only UPDATE拒否を実測 |
| I-2 | contentはwrite-once。後から許される変化は明示的消去のみ | SQLite DDL | Erasure Coordinator | trg_blobs_no_update / trg_content_objects_erasure_only | INV-I-2 | SQL negative + integration | VERIFIED | SQL-2/Runtime-7 |  |
| I-3 | eventは`prev_event_hash / event_hash`による連鎖を持つ | Canonical Writer | verify command / U-4 unique defense | events prev/hash columns | INV-I-3 | golden + integration | SPECIFIED | Runtime-1 |  |
| I-4 | `seq`はresident内で一意・単調増加・再利用しない。欠番は許容 | SQLite DDL | Canonical Writer | UNIQUE(events.resident_id, events.seq) | INV-I-4 | SQL negative + writer property | DDL-IMPLEMENTED | SQL-2/Runtime-1 |  |
| I-5 | content消去は`content_erasure_events`の追記を伴う | Erasure Coordinator | SQLite DDL | content_erasure_events + content update tx | INV-I-5 | integration | SPECIFIED | Runtime-7 |  |
| I-6 | content消去時、commitmentを保持しsaltを破棄する | SQLite DDL + Erasure Coordinator | — | trg_content_objects_erasure_only | INV-I-6 | SQL negative + integration | VERIFIED | SQL-2/Runtime-7 |  |
| I-7 | claimはstatement・kind・主体関係・temporal kindを変更しない | SQLite DDL | Canonical Writer | trg_claims_no_update/no_delete | INV-I-7 | SQL negative | DDL-IMPLEMENTED | SQL-2 |  |
| I-8 | すべてのclaimは最低一件のevidenceを持つ | Canonical Writer | — | CreateClaim UoW | INV-I-8 | writer negative | RUNTIME-SPEC | Runtime-5 |  |
| I-9 | すべてのclaim_evidenceはderivationを問わず、実在するeventをNOT NULLで直接参照する | SQLite DDL | Canonical Writer | claim_evidence.event_id NOT NULL FK | INV-I-9 | SQL negative | DDL-IMPLEMENTED | SQL-2 |  |
| I-10 | すべてのstage変化は`claim_stage_transitions`を伴う | Canonical Writer | SQLite DDL (verified defense) | claim_stage_transitions + uq_claim_stage_transitions_stage | INV-I-10 | SQL negative + writer unit | RUNTIME-SPEC | SQL-2 | uq_claim_stage_transitions_stage verified; transition existence/order remains Writer responsibility |
| I-11 | すべてのstatus変化は`claim_status_transitions`を伴う | Canonical Writer | SQLite append-only | claim_status_transitions | INV-I-11 | writer integration | RUNTIME-SPEC | Runtime-5 |  |
| I-12 | settledへの遷移はmemory policyが適格とするsupport evidenceを必要とする | Canonical Writer | — | settled gate evaluator | INV-I-12 | writer negative | RUNTIME-SPEC | Runtime-5 |  |
| I-13 | 非信頼evidenceのみで構成されるclaimはsettledに到達しない | Canonical Writer | — | trust gate | INV-I-13 | writer negative | RUNTIME-SPEC | Runtime-5 | 初期ingressではuntrusted生成なし |
| I-14 | residentをまたぐclaim / evidenceの生きた参照を行わない | SQLite DDL (21 verified triggers) + Canonical Writer | — | Resident-Scope Inventory v0.1 + source_type-specific scope validator | INV-I-14 | SQL negative + writer negative | PLANNED-RUNTIME | SQL-2/Runtime-3 | DDL-scoped references verified; polymorphic generation input and Projection/Operational scope remain Runtime |
| I-15 | Recall / claim usage自体をevidenceにしない | Canonical Writer | — | memory extraction validator | INV-I-15 | writer negative | RUNTIME-SPEC | Runtime-5 |  |
| I-16 | policy依存判断は使用したmemory policy revisionを記録する | Canonical Writer | SQLite NOT NULL where applicable | memory_policy_revision_id | INV-I-16 | writer negative | SPECIFIED | Runtime-5 |  |
| I-17 | revisionは`principles / persona / memory_policy`のclassを持つ | SQLite DDL | Canonical Writer | resident_revisions.revision_class CHECK | INV-I-17 | SQL negative | DDL-IMPLEMENTED | SQL-2 |  |
| I-18 | principles activationはhuman principalのapprovalを必要とする | Canonical Writer / Permission | SQLite FK | approval + activation UoW | INV-I-18 | permission integration | RUNTIME-SPEC | Runtime-2 |  |
| I-19 | Runtime / resident自身はprinciplesを自動活性化しない | Permission / Canonical Writer | — | activation command authorization | INV-I-19 | permission negative | RUNTIME-SPEC | Runtime-2 |  |
| I-20 | personaの小さなrevisionは自動活性化可能 | Persona Revision Runtime | Canonical Writer | persona activation flow | INV-I-20 | integration | RUNTIME-SPEC | Runtime-5 |  |
| I-21 | memory policyは管理操作によって明示的に変更する | Admin / Permission | Canonical Writer | memory policy command | INV-I-21 | permission negative | RUNTIME-SPEC | Runtime-2 |  |
| I-22 | resident lifecycleは追記型transitionで表現する | SQLite DDL | Canonical Writer | resident_status_transitions append-only | INV-I-22 | SQL negative + replay | DDL-IMPLEMENTED | SQL-2 |  |
| I-23 | draft residentはgeneration runを開始できない | Canonical Writer | — | PrepareGeneration validation | INV-I-23 | writer negative | RUNTIME-SPEC | Runtime-2/3 |  |
| I-24 | ProjectionはCanonicalから破棄・再構築できる | Projection Coordinator | CI | rebuild command | INV-I-24 | projection golden | RUNTIME-SPEC | Runtime-4 |  |
| I-25 | Projectionは`Canonical + projection_version + expected versioned dependency set + explicit as_of`が同じなら決定的である | Projection Coordinator | Golden tests | projection evaluator | INV-I-25 | property/golden | RUNTIME-SPEC | Runtime-4 |  |
| I-26 | CanonicalからProjectionを参照しない | Schema / Architecture review | CI schema audit | FK direction audit | INV-I-26 | schema audit | SPECIFIED | SQL-2 |  |
| I-27 | Projection更新失敗はCanonical commitを無効にしない | Runtime Coordinator | — | post-commit projection notification | INV-I-27 | failure integration | RUNTIME-SPEC | Runtime-4 |  |
| I-28 | Activity Session全量を暗黙の既定入力として常時投入しない | Context Assembler | — | live context selector | INV-I-28 | context unit | RUNTIME-SPEC | Runtime-3 |  |
| I-29 | すべてのgeneration inputは`source_type / inclusion_mode`を持つ | SQLite DDL | Context Assembler | generation_run_inputs CHECK | INV-I-29 | SQL negative + context unit | DDL-IMPLEMENTED | SQL-2/Runtime-3 |  |
| I-30 | 最終入力集合と順序を`generation_run_inputs`へ完全記録する | Context Assembler + Canonical Writer | SQLite UNIQUE | generation_run_inputs(run, ordinal) | INV-I-30 | integration | SPECIFIED | Runtime-3 |  |
| I-31 | Live Context / Backfillへのevent包含はclaim usageを生成しない | Context Assembler | Canonical Writer | claim_usages policy | INV-I-31 | context negative | RUNTIME-SPEC | Runtime-3 |  |
| I-32 | 入力包含それ自体はevidence・confidence・stageを変えない | Canonical Writer | — | claim/evidence mutation API | INV-I-32 | writer negative | RUNTIME-SPEC | Runtime-3/5 |  |
| I-33 | Live Contextから外れたeventを削除・忘却扱いしない | Context Assembler / Store | — | event retention behavior | INV-I-33 | integration | RUNTIME-SPEC | Runtime-3 |  |
| I-34 | self-talkを対話生成のLive Contextへ自動投入しない | Context Assembler | — | self_talk exclusion rule | INV-I-34 | context negative | RUNTIME-SPEC | Runtime-3 |  |
| I-35 | memory_recallで実入力になったclaimは`prompt_included` usageと整合する | Canonical Writer / Context Assembler | Writer scope validator | claim_usages + generation_run_inputs | INV-I-35 | integration | RUNTIME-SPEC | Runtime-3 |  |
| I-36 | Active ThreadはCanonicalではなくRuntime Projectionである | Projection Coordinator | Schema audit | runtime_states [P] | INV-I-36 | schema + projection test | SPECIFIED | Runtime-4 |  |
| I-37 | Live Contextは原則として現在のActivity Session内のeventから構成する | Context Assembler | — | activity session selector | INV-I-37 | context unit | RUNTIME-SPEC | Runtime-3 |  |
| I-38 | claim、初期evidence、初期`NULL→floating` transitionを同一transactionでcommitする | Canonical Writer | — | CreateClaim transaction | INV-I-38 | integration/crash | RUNTIME-SPEC | Runtime-5 |  |
| I-39 | `resident_message / outbound_initiative`をclaim evidence sourceにしない | Canonical Writer | — | evidence source validator | INV-I-39 | writer negative | RUNTIME-SPEC | Runtime-5 |  |
| I-40 | settledには、非self_talk source eventを直接参照するsupport evidence、またはmemory policyが定義する外部整合条件のいずれかを必要とする。後者の依存先claimは、それ自体が外部由来support evidenceを持たなければならない | Canonical Writer | — | settled gate evaluator | INV-I-40 | writer negative | RUNTIME-SPEC | Runtime-5 |  |
| I-41 | direct claimのsettledはactiveなmeta claimとの整合を必要とし、そのmeta claimはperspective本人の`user_message`を直接参照するsupport evidenceを持つ | Canonical Writer | Index support | direct/meta alignment evaluator | INV-I-41 | writer negative | RUNTIME-SPEC | Runtime-5 |  |
| I-42 | `kind in {other, NULL}`のsettledは、event_type=user_messageかつactor=subjectの直接source eventを持つsupport evidenceを必要とする | Canonical Writer | — | other/null gate | INV-I-42 | writer negative | RUNTIME-SPEC | Runtime-5 |  |
| I-43 | generation runは不変とし、結果をoutcomeへ追記する | SQLite DDL | Generation Runtime | append-only triggers on runs/outcomes | INV-I-43 | SQL negative | DDL-IMPLEMENTED | SQL-2/Runtime-3 |  |
| I-44 | event生成purposeの成功runは、ちょうど一件のeventを生成する | SQLite DDL + Canonical Writer | — | events.generation_run_id UNIQUE | INV-I-44 | SQL negative + integration | DDL-IMPLEMENTED | SQL-2/Runtime-3 |  |
| I-45 | generation inputsと`prompt_included` usageを外部呼び出し前にcommitする | Runtime Coordinator | Canonical Writer | PrepareGeneration UoW | INV-I-45 | provider-spy integration | RUNTIME-SPEC | Runtime-3 |  |
| I-46 | resident分岐はquiescent stateでのみ実行する | Runtime Coordinator | — | quiescent checker | INV-I-46 | integration | DEFERRED | deferred branch |  |
| I-47 | claim usageは、そのusage記録時点で有効なmemory policy revisionを持つ | Canonical Writer | SQLite FK/NOT NULL | claim_usages.memory_policy_revision_id | INV-I-47 | writer negative | SPECIFIED | Runtime-3/5 |  |
| I-48 | Canonicalな時刻判断はinstantとtimezoneの組を失わない | SQLite DDL + Canonical Writer | — | instant/timezone paired columns | INV-I-48 | SQL negative + writer unit | DDL-IMPLEMENTED | SQL-2 |  |
| I-49 | automatic status transitionは許可されたreason classに限定する | SQLite DDL + Canonical Writer | — | claim_status_transitions reason CHECK | INV-I-49 | SQL negative | DDL-IMPLEMENTED | SQL-2/Runtime-5 |  |
| I-50 | automatic decisionはtrigger、pipeline、gate metricsを必須で持ち、memory policy依存時だけ使用したmemory policy revisionを必須で持つ | SQLite DDL + Canonical Writer | — | decision/trigger CHECK | INV-I-50 | SQL negative + writer negative | DDL-IMPLEMENTED | SQL-2/Runtime-5 |  |
| I-51 | human decisionはactor principalを持つ | SQLite DDL | Canonical Writer | human actor CHECK/FK | INV-I-51 | SQL negative | DDL-IMPLEMENTED | SQL-2 |  |
| I-52 | quarantinedからactiveへの復帰はhuman decisionのみ許可する | SQLite DDL + Canonical Writer | — | status transition checks | INV-I-52 | SQL negative | DDL-IMPLEMENTED | SQL-2/Runtime-5 |  |
| I-53 | 生成由来eventは、同一Canonical Commitに所属するsucceeded outcomeとともに着地する | Canonical Writer | SQLite FKs | Success Landing UoW | INV-I-53 | crash/integration | RUNTIME-SPEC | Runtime-3 |  |
| I-54 | Canonical entity IDはuppercase ULIDで保存し、zero ULIDを未設定値に使わない | SQLite DDL | Go strict parser | ULID CHECK | INV-I-54 | SQL negative + property | DDL-IMPLEMENTED | SQL-2 |  |
| I-55 | ULID timestampをCanonical順序・意味時間の根拠にしない | Architecture / Code review | — | ID API | INV-I-55 | unit/static | SPECIFIED | Runtime-1 |  |
| I-56 | Canonical JSONはrecord作成時のcanonicalization versionで再serialization可能であり、Canonical論理整数は厳密なdecimal string mappingを使う | Serialization library | Golden vectors | mahoroba-jcs-v1 | INV-I-56 | golden/property | SPECIFIED | Runtime-1 |  |
| I-57 | event hashは物理DB rowではなくCanonical event envelope bytesから計算する | Canonical Writer | SQLite DDL (verified uniqueness defense) | event hash function + uq_events_event_hash | INV-I-57 | golden + SQL negative | RUNTIME-SPEC | SQL-2/Runtime-1 | Event Hash computation remains Writer/golden-test responsibility; uq_events_event_hash verified in DDL v0.1.2 |
| I-58 | content commitmentはcontentごとのsaltを用い、消去時にsaltを破棄してcommitmentを保持する | Content service | SQLite DDL | commitment algorithm | INV-I-58 | golden + erasure integration | SPECIFIED | Runtime-1/7 |  |
| I-59 | content erasure auditへcommitment saltやunsalted blob hashを複製して残さない | Schema / Erasure Coordinator | CI schema audit | erasure audit columns | INV-I-59 | schema + integration | SPECIFIED | SQL-2/Runtime-7 |  |
| I-60 | Activity SessionはCanonical entityではなく、user_message列・resident lifecycle・Canonical Sessionization Policy・as_ofから再構築する | Projection Coordinator | Sessionization Policy | activity session evaluator | INV-I-60 | projection golden | RUNTIME-SPEC | Runtime-4 |  |
| I-61 | `resident_message / self_talk / outbound_initiative`はActivity Sessionを開始・延長しない | Sessionization evaluator | — | event type filter | INV-I-61 | projection unit | RUNTIME-SPEC | Runtime-4 |  |
| I-62 | Activity Session close event・Canonical ID・event参照を持たない | Schema / Projection | — | no session canonical entity | INV-I-62 | schema audit | SPECIFIED | SQL-2 |  |
| I-63 | 初期Canonical eventはConversationの存在を要求しない | Schema / Context | — | events without conversation FK | INV-I-63 | schema audit + dialogue integration | SPECIFIED | SQL-2/Runtime-3 |  |
| I-64 | `canonical_commits`自身を除くすべての[C] rowは`canonical_commit_id NOT NULL`で所属Canonical Commitを持つ | SQLite DDL | Canonical Writer | all [C] rows canonical_commit_id NOT NULL FK | INV-I-64 | schema + SQL negative | DDL-IMPLEMENTED | SQL-2 |  |
| I-65 | 1 Canonical Unit of Workで追加される[C] rowは同一Canonical Commitへ所属する | Canonical Writer | — | UoW command validator | INV-I-65 | integration | RUNTIME-SPEC | Runtime-1 |  |
| I-66 | `commit_seq`はstore全体で単調増加・再利用なし・欠番可であり、resident意味順序へ使わない | SQLite DDL + Canonical Writer | — | canonical_commits.commit_seq UNIQUE/CHECK | INV-I-66 | SQL negative + property | DDL-IMPLEMENTED | SQL-2/Runtime-1 |  |
| I-67 | automatic status transitionはtyped direct Canonical triggerをちょうど一件持つ | SQLite DDL | Canonical Writer | typed trigger nullable CHECK | INV-I-67 | SQL negative | DDL-IMPLEMENTED | SQL-2 |  |
| I-68 | `explicit_supersession / explicit_correction / structural_quarantine`は固定されたtrigger kindと遷移先以外をautomaticで許可しない | SQLite DDL + Canonical Writer | — | reason→trigger/status CHECK | INV-I-68 | SQL negative | DDL-IMPLEMENTED | SQL-2/Runtime-5 |  |
| I-69 | structural quarantineは上流eventを直接triggerにせず、Canonical `integrity_finding`をtriggerにする | SQLite DDL + Canonical Writer | — | structural_quarantine CHECK | INV-I-69 | SQL negative | DDL-IMPLEMENTED | SQL-2/Runtime-5 |  |
| I-70 | `erasure_policy`は`independent / resident_only`の二値で、content作成後に変更しない | SQLite DDL | — | erasure_policy CHECK + trigger | INV-I-70 | SQL negative | VERIFIED | SQL-2 |  |
| I-71 | `erasure_state`は`present → erased`の一方向で、`erased → present`を許可しない | SQLite DDL | Erasure Coordinator | erasure-only trigger | INV-I-71 | SQL negative | VERIFIED | SQL-2/Runtime-7 |  |
| I-72 | `resident_only` contentの単独Content Eraseを拒否する | Erasure Coordinator / Permission | SQLite value check | authorization + policy check | INV-I-72 | permission negative | RUNTIME-SPEC | Runtime-7 |  |
| I-73 | erasure policyとErasure Impact Analysisを分離し、branch済み別residentへ消去を自動伝播しない | Erasure Coordinator | — | Impact Analysis | INV-I-73 | integration | RUNTIME-SPEC | Runtime-7 |  |
| I-74 | watermark rowなしをProjection未構築とし、sentinel source positionを使わない | Projection Coordinator | SQLite PK | watermark row existence | INV-I-74 | projection unit | RUNTIME-SPEC | Runtime-4 |  |
| I-75 | watermark cursorには`source_commit_seq`を使い、event `seq`、ULID、timestamp、DB固有positionを使わない | Projection Coordinator | Schema | source_commit_seq | INV-I-75 | unit/static | RUNTIME-SPEC | Runtime-4 |  |
| I-76 | Projection本体更新とwatermark進行は同一Projection transactionでcommitする | Projection Coordinator | SQLite transaction | projection + watermark transaction | INV-I-76 | crash/integration | RUNTIME-SPEC | Runtime-4 |  |
| I-77 | status transitionが一件も存在しない新規claimは`active`としてProjectionし、`NULL → active` transitionを作らない | Projection evaluator | — | claim status replay | INV-I-77 | projection golden | RUNTIME-SPEC | Runtime-4 |  |
| I-78 | claim生成時には初期`claim_view_scope_assertion`を同一Canonical Unit of Workで必ず作る | Canonical Writer | — | CreateClaim UoW | INV-I-78 | crash/integration | RUNTIME-SPEC | Runtime-5 |  |
| I-79 | content erasure eventは消去scope・actor・reasonを持ち、派生消去では元erasure eventへ任意に遡れる | SQLite DDL + Erasure Coordinator | — | content_erasure_events required columns/FKs | INV-I-79 | SQL negative + integration | DDL-IMPLEMENTED | SQL-2/Runtime-7 |  |
| I-80 | `projection_watermarks`は`[O]`としてProjection Store側に保存し、Canonicalから参照しない | Schema / Projection | CI FK audit | projection_watermarks [O] | INV-I-80 | schema audit | SPECIFIED | SQL-2 |  |
| I-81 | automatic semantic correction / supersessionはclaim kindごとに許可されたprincipalのeventをevidenceが直接参照する場合だけ成立する | Canonical Writer | — | kind-specific semantic transition validator | INV-I-81 | writer negative | RUNTIME-SPEC | Runtime-5 |  |
| I-82 | Activity Session再構築に使うSessionization Policy definitionはCanonicalに保存し、過去versionを変更・削除しない | SQLite DDL | Canonical Writer | sessionization versions append-only triggers | INV-I-82 | SQL negative | DDL-IMPLEMENTED | SQL-2 |  |
| I-83 | `canonical_commit_id / commit_seq / canonical_commits.resident_id`をevent hash、content commitment、claim identityの入力へ含めない | Serialization / Golden tests | CI static audit | hash/identity input spec | INV-I-83 | golden/static | SPECIFIED | Runtime-1 |  |
| I-84 | `Score`をCanonicalJSONへ直接含めず、Canonical判定材料はfixed-point / integerへ量子化する | Canonical JSON validator | Canonical Writer | fixed-point validation | INV-I-84 | unit/property | SPECIFIED | Runtime-1/5 |  |
| I-85 | resident lifecycleは定義済み遷移行列に限定し、`archived → active`を禁止し、`erased`を終端とする | Canonical Writer | SQLite value CHECK | resident lifecycle matrix | INV-I-85 | writer negative | RUNTIME-SPEC | Runtime-2 |  |
| I-86 | 初期4 event typeの`actor_principal_id`はNOT NULLとし、未定義ingress / trust mappingを持つeventをcommitしない | SQLite DDL + Canonical Writer | — | events.actor NOT NULL + event mapping CHECK | INV-I-86 | SQL negative | DDL-IMPLEMENTED | SQL-2 |  |
| I-87 | structural quarantineのtrigger findingは`claim_id NOT NULL`で、transition対象claimと一致する | Canonical Writer | SQLite FK | finding/transition validator | INV-I-87 | writer negative | RUNTIME-SPEC | Runtime-5/7 |  |
| I-88 | blob GCは`content_references`のwatermarkがCanonical Headへ到達し、projection versionが現行と一致する場合だけ実行できる | Erasure Coordinator | Projection Coordinator | GC precondition | INV-I-88 | integration | RUNTIME-SPEC | Runtime-7 |  |
| I-89 | bootstrapおよびCanonical rowを投入するmigrationはCanonical Commitを発行する | Bootstrap / Migration runner | Canonical Writer | bootstrap data UoW | INV-I-89 | integration | RUNTIME-SPEC | Runtime-2 |  |
| I-90 | Recall candidateであることだけではsalienceを上げない | Projection / Memory evaluator | — | salience contribution rules | INV-I-90 | unit | RUNTIME-SPEC | Runtime-5 |  |
| I-91 | stage dependencyに使ったmeta claimの事後status変化は過去stageを無効化せず、integrity finding / automatic quarantineを生成しない | Scheduler / Reassessment | — | dependency change detector | INV-I-91 | integration | RUNTIME-SPEC | Runtime-6 |  |
| I-92 | Projection watermarkは、そのProjection全体の再構築結果を決めるversioned Canonical dependencyの完全な集合を`projection_watermark_dependencies`として保持し、不一致をfull rebuild条件とする | Projection Coordinator | — | projection_watermark_dependencies | INV-I-92 | projection negative | RUNTIME-SPEC | Runtime-4 |  |
| I-93 | settled direct claimはautomatic `explicit_correction`でinvalidatedせず、external alignmentを満たしてsettledへ到達したreplacement direct claimによる`explicit_supersession`だけをautomaticで許可する | Canonical Writer | — | settled direct status validator | INV-I-93 | writer negative | RUNTIME-SPEC | Runtime-5 |  |
| I-94 | Recallのcandidate / selected / prompt_included等の実結果をCanonicalに記録するが、同一条件から同一選択を再計算できることは保証しない | Recall Runtime | Canonical Writer | recall_runs/claim_usages | INV-I-94 | integration | RUNTIME-SPEC | Runtime-3 |  |
| I-95 | meta claimのsettledは、perspective principal本人の`user_message`を直接参照するsupport evidenceを必要とする | Canonical Writer | — | meta settled gate | INV-I-95 | writer negative | RUNTIME-SPEC | Runtime-5 |  |
| I-96 | settled direct claimのautomatic supersessionは、replacementのsettled到達stage transition、`supersedes` relation、old claimのstatus transitionが同一Canonical Commitで成立する場合に限る。replacementが先行commitですでにsettledの場合はhuman decisionとする | Canonical Writer | — | replacement atomic UoW | INV-I-96 | crash/integration | RUNTIME-SPEC | Runtime-5 |  |
| I-97 | 各Projectionは期待するversioned dependency集合を宣言し、保存済み集合との完全一致を検査する | Projection Coordinator | CI registry audit | expected dependency registry | INV-I-97 | unit | RUNTIME-SPEC | Runtime-4 |  |
| I-98 | `claim_states`はpointwise / aggregateの二段規則で再生し、`runtime_states`はpolicy依存項目を永続化しないため、初期watermark dependencyを持たない | Projection Coordinator | — | claim_states/runtime_states rebuild rules | INV-I-98 | projection golden | RUNTIME-SPEC | Runtime-4 |  |
| I-99 | desired Sessionization Policyが`runtime_config`から失われた場合、既存watermark dependencyからのみ復旧し、両方なければfail closedとする | Recovery Manager | Runtime config | sessionization policy recovery | INV-I-99 | recovery integration | RUNTIME-SPEC | Runtime-4 |  |
| I-100 | `projection_version`はProjection計算コードの版であり、Canonical definitionを持たない | Projection Coordinator / Build metadata | — | projection_version | INV-I-100 | unit/static | RUNTIME-SPEC | Runtime-4 |  |
| I-101 | `claim_states`の集約・大域計算は、明示的`as_of`時点で有効だったmemory policy revisionを適用する。外部の現在選択を参照しない | Projection Coordinator | — | claim_states aggregate evaluator | INV-I-101 | projection golden | RUNTIME-SPEC | Runtime-4 |  |
| I-102 | `claim_states`は、保存済み`source_commit_seq`より後のCanonical Commitに対象residentのmemory policy revision activationが存在する場合、policy definitionの意味差分や実時計を解析せずfull rebuildする | Projection Coordinator | Canonical commit scan | claim_states rebuild trigger | INV-I-102 | projection integration | RUNTIME-SPEC | Runtime-4 |  |
| I-103 | 同一`projection_name / resident_id`の永続Projectionでは`as_of`を単調に進め、過去`as_of`評価の結果をProjection本体またはwatermarkへ書き戻さない | Projection Coordinator | — | watermark as_of guard | INV-I-103 | projection negative | RUNTIME-SPEC | Runtime-4 |  |
| I-104 | Canonicalへ記録するtransaction time（`recorded_at / committed_at / created_at`等）はsingle writer下の共通台帳時計から採番し、単調非減少とする。event timeおよびvalid timeには単調性を要求しない | Canonical Writer | — | ledger clock | INV-I-104 | property/restart | RUNTIME-SPEC | Runtime-1 |  |
| I-105 | resident revision activationはrevision classを問わず非遡及とし、activation instantを`resident_revision_activations.recorded_at`とする。過去へ遡るeffective timeを持たない | Canonical Writer | SQLite schema lacks effective_at | revision activation | INV-I-105 | writer negative | SPECIFIED | Runtime-2 |  |
| I-106 | 初期4 event typeはcontent objectを必ず一件参照し、`events.content_id`をNOT NULLとする。content消去後も参照を保持し、消去を`content_id`のNULL化で表現しない | SQLite DDL | Erasure Coordinator | events.content_id NOT NULL FK | INV-I-106 | SQL negative + erasure integration | DDL-IMPLEMENTED | SQL-2 |  |
| RT-I-1 | Canonical mutationは一つのCanonical Writerだけが実行する | Architecture / Canonical Writer | CI | single writer construction | INV-RTI-1 | concurrency integration | RUNTIME-SPEC | Runtime-1 |  |
| RT-I-2 | Canonical Writer内で外部network callを行わない | Architecture / Canonical Writer | CI static rule | writer API | INV-RTI-2 | unit/static | RUNTIME-SPEC | Runtime-1 |  |
| RT-I-3 | 一つのCanonical Unit of Workは一つのCanonical Commitに対応する | Canonical Writer | SQLite transaction | UoW abstraction | INV-RTI-3 | integration | RUNTIME-SPEC | Runtime-1 |  |
| RT-I-4 | 同一Unit of Workの全[C] rowは同じ`canonical_commit_id`を持つ | Canonical Writer | SQLite FK | UoW rows | INV-RTI-4 | integration | RUNTIME-SPEC | Runtime-1 |  |
| RT-I-5 | `commit_seq / event.seq / ledger_time`を同じsingle writer境界で採番する | Canonical Writer | SQLite unique constraints | sequence/clock allocator | INV-RTI-5 | property/concurrency | RUNTIME-SPEC | Runtime-1 |  |
| RT-I-6 | generation inputsと`prompt_included` usageをprovider呼び出し前にcommitする | Runtime Coordinator | Canonical Writer | PrepareGeneration | INV-RTI-6 | provider-spy integration | RUNTIME-SPEC | Runtime-3 |  |
| RT-I-7 | generation run envelopeをretryで変更しない。入力変更時は新runを作る | Generation Runtime | SQLite append-only | retry flow | INV-RTI-7 | integration | RUNTIME-SPEC | Runtime-3 |  |
| RT-I-8 | 一attemptは一つのrunning outcomeと最大一つのterminal outcomeを持つ | SQLite DDL | Generation Runtime | uq_generation_run_outcomes_running/terminal | INV-RTI-8 | SQL negative | VERIFIED | SQL-2/Runtime-3 | 監査で部分UNIQUEを実測 |
| RT-I-9 | event生成purposeの`succeeded` outcomeとeventは同一commitで成立する | Canonical Writer | — | event Success Landing | INV-RTI-9 | crash/integration | RUNTIME-SPEC | Runtime-3 |  |
| RT-I-10 | non-event purposeの`succeeded`はpurpose固有Canonical効果までlandingしたことを意味する | Purpose Landing Handler | Canonical Writer | purpose contract | INV-RTI-10 | integration | RUNTIME-SPEC | Runtime-3/5 |  |
| RT-I-11 | UI provisional outputをCanonical event・evidence・TTSの確定入力として扱わない | HTTP/SSE Runtime | Canonical Writer | provisional stream | INV-RTI-11 | integration | RUNTIME-SPEC | Runtime-3 |  |
| RT-I-12 | in-memory work queueの消失でmandatory workを永久に失わない | Derived Work Coordinator | Recovery Manager | rediscovery predicates | INV-RTI-12 | restart integration | RUNTIME-SPEC | Runtime-3 |  |
| RT-I-13 | mandatory obligation keyをpipeline / policy version変更だけで変えない | Derived Work Coordinator | — | obligation key builder | INV-RTI-13 | unit | RUNTIME-SPEC | Runtime-3 |  |
| RT-I-14 | mandatory workはsuccessまたはterminal failure / cancellationのいずれかへ終端させる | Derived Work Coordinator | Recovery Manager | mandatory state machine | INV-RTI-14 | integration | RUNTIME-SPEC | Runtime-3 |  |
| RT-I-15 | quiescent判定直前にCanonicalからmandatory workを再発見する | Quiescent Checker | Derived Work Coordinator | pre-quiescence scan | INV-RTI-15 | integration | RUNTIME-SPEC | Runtime-3/deferred branch |  |
| RT-I-16 | Projection更新失敗でCanonical Commitをrollbackしない | Runtime Coordinator | Projection Coordinator | post-commit notification | INV-RTI-16 | failure integration | RUNTIME-SPEC | Runtime-4 |  |
| RT-I-17 | Projection本体・watermark・dependency集合を同じProjection transactionで更新する | Projection Coordinator | SQLite transaction | projection tx | INV-RTI-17 | crash/integration | RUNTIME-SPEC | Runtime-4 |  |
| RT-I-18 | 永続Projectionの`as_of`を後退させない | Projection Coordinator | — | as_of guard | INV-RTI-18 | projection negative | RUNTIME-SPEC | Runtime-4 |  |
| RT-I-19 | stale / 未構築content_referencesを根拠にblob GCしない | Erasure Coordinator | Projection Coordinator | GC gate | INV-RTI-19 | integration | RUNTIME-SPEC | Runtime-7 |  |
| RT-I-20 | startup時に旧processのrunning attemptをcancelledへ終端する | Recovery Manager | Canonical Writer | startup interrupted-run handler | INV-RTI-20 | restart integration | RUNTIME-SPEC | Runtime-1/3 |  |
| RT-I-21 | quiescent判定はmandatory workとrunning attemptを対象とし、Projection遅延を含めない | Quiescent Checker | — | quiescence predicate | INV-RTI-21 | integration | RUNTIME-SPEC | deferred branch |  |
| RT-I-22 | self-talk eventだけでself-talk活動を無制限延命しない | Scheduler | Runtime policy | self-talk limiter | INV-RTI-22 | scheduler simulation | RUNTIME-SPEC | Runtime-6 |  |
| RT-I-23 | self-talk連続回数は新しい`user_message`でのみresetし、resident自身の生成eventではresetしない | Scheduler | Runtime policy | self-talk counter | INV-RTI-23 | scheduler unit | RUNTIME-SPEC | Runtime-6 |  |
| RT-I-24 | self-talk retention policyはcontentを直接削除せず、通常のErasure候補へ送る | Retention Scheduler | Erasure Coordinator | retention candidate flow | INV-RTI-24 | integration | RUNTIME-SPEC | Runtime-6/7 |  |
| RT-I-25 | Projection scheduling intervalの変更はProjection意味論を変えず、実行頻度だけを変える | Projection Scheduler | Projection Coordinator | interval config | INV-RTI-25 | configuration/property | RUNTIME-SPEC | Runtime-4 |  |
| RT-I-26 | Content Erase / Resident Eraseをsystem principalやresident principalが自動実行しない | Permission / Erasure Coordinator | — | authorization table | INV-RTI-26 | security negative | RUNTIME-SPEC | Runtime-7 |  |
| RT-I-27 | TTS失敗でtext eventのCanonical成功を取り消さない | TTS Adapter | Canonical event flow | TTS post-commit | INV-RTI-27 | failure integration | RUNTIME-SPEC | Runtime-3 |  |
| RT-I-28 | Branch Copy Specが未確定の間、branch featureを有効化しない | Feature Gate | Admin/API | branch disabled flag | INV-RTI-28 | API negative | DEFERRED | deferred branch |  |
| RT-I-29 | time-sensitive Projectionでcommit catch-upとas-of再評価の両方が必要な場合、同一Projection transactionで両成分を完了し、Projection本体が表さないcursorをwatermarkへ記録しない | Projection Coordinator | SQLite transaction | Update Plan | INV-RTI-29 | projection integration | RUNTIME-SPEC | Runtime-4 |  |
| RT-I-30 | 一つのCanonical Unit of Workでledger timeを一度だけ採番し、そのUoWに属する全transaction-time列へ同一値を書く | Canonical Writer | — | ledger time allocator | INV-RTI-30 | property/restart | RUNTIME-SPEC | Runtime-1 |  |
| RT-I-31 | 一回のgenerationで使用する`as_of / context_policy_version / memory_rendering_version`をAssembly開始時に固定し、同一Assembly中に読み直さず、その固定値をgeneration provenanceへ保存する | Context Assembler | Canonical Writer | Assembly Snapshot | INV-RTI-31 | concurrency/integration | RUNTIME-SPEC | Runtime-3 |  |
| U-1 | 同一claimは同じstageへ高々一度だけ到達する | SQLite DDL | Canonical Writer | uq_claim_stage_transitions_stage | INV-U-1 | SQL negative | VERIFIED-DDL-0.1.2 | SQL-2 complete | I-10の物理防御; DDL v0.1.2 negative test verified |
| U-2a | claim_evidenceはclaim/eventのresident scopeをまたがない | SQLite DDL | Canonical Writer | trg_claim_evidence_resident_scope | INV-U-2a | SQL negative | VERIFIED-DDL-0.1.2 | SQL-2 complete | I-14; DDL v0.1.2 negative test verified |
| U-2b | claim relationの両端は同一residentに属する | SQLite DDL | Canonical Writer | trg_claim_relations_resident_scope | INV-U-2b | SQL negative | VERIFIED-DDL-0.1.2 | SQL-2 complete | I-14; DDL v0.1.2 negative test verified |
| U-2c | stage dependency claimは対象claimと同一residentに属する | SQLite DDL | Canonical Writer | trg_claim_stage_dependency_resident_scope | INV-U-2c | SQL negative | VERIFIED-DDL-0.1.2 | SQL-2 complete | I-14; DDL v0.1.2 negative test verified |
| U-2d | generation inputのsource entityはgeneration runと同一resident scopeに属する | Context Assembler / Canonical Writer | — | source_type-specific scope validator | INV-U-2d | writer negative | PLANNED-RUNTIME | Runtime-3 | I-14/I-35 |
| U-3 | 全I/RT-Iへ強制責任者・テスト・状態を割り当てる | CI / Design governance | All components | this matrix | INV-U-3 | document/CI | THIS-DOCUMENT | pre-implementation | Matrix v0.1.1 + Resident-Scope Reference Inventory v0.1; coverage clean |
| U-4 | Canonical Store内でalgorithm/domain/event_hashを一意にする | SQLite DDL | Canonical Writer | uq_events_event_hash | INV-U-4 | SQL negative | VERIFIED-DDL-0.1.2 | SQL-2 complete | multi-userでもStore-wide。isolationとは別責務; DDL v0.1.2 negative test verified |

## 3. CI contract

1. `I-1〜I-106`と`RT-I-1〜RT-I-31`が全件一度以上存在する。
2. ID重複、Primary Enforcer未割当、Test ID未定義、Status未定義を拒否する。
3. DDL object名を持つ行はmigration内の実在を検査する。
4. `VERIFIED-DDL-0.1.2`行は対応negative testをvalidation suiteで実行する。
5. resident-scopeのDDL/Runtime/allowed/deferred分類はResident-Scope Inventoryと一致させる。
6. 新しい不変条件・参照familyを追加する変更は、同じ変更でMatrix・Inventory・test catalogを更新する。
