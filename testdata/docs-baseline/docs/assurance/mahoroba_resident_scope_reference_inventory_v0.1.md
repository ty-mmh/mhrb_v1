# まほろば Resident-Scope Reference Inventory v0.1

**Status:** exhaustive scope audit / DDL v0.1.2 basis
**Sources:** Data Model v0.2.14 / Runtime v0.1.2 / SQLite DDL v0.1 / Constraint Enforcement Matrix v0.1

## 0. 目的

resident独立性を破り得る参照を、Canonical・Projection・Operational・意図的cross-scopeの全域で棚卸しし、DB trigger、Canonical Writer、Projection Coordinator、Permissionの責任を固定する。

本棚卸しでは、**principal参照はresident越境と同一視しない**。subject / perspective / actor / targetは、他者との関係を表現するためにresident外を参照し得る。一方、claim・evidence・event・revision・generation・content等のresident所有物は、明示的な例外を除いて同一resident scopeへ閉じる。

## 1. 集計

- `DDL v0.1.2`: 20
- `DDL v0.1.2 + Runtime`: 1
- `Runtime`: 10
- `Writer`: 1
- `allowed`: 4
- `already enforced`: 1
- `deferred`: 1
- 総参照family: 38
- DDL v0.1.2で追加するresident-scope trigger: 21

## 2. 正本一覧

| ID | Area | Source | Required scope | Enforcement | Object / component | Disposition |
|---|---|---|---|---|---|---|
| RS-001 | Canonical commit | all resident-scoped [C] rows | derived resident scope must equal commit.resident_id | Canonical Writer | Canonical UoW scope validator | Writer |
| RS-002 | Resident definition | residents.description_content_id | content owner = residents.resident_id | SQLite trigger | trg_residents_content_scope | DDL v0.1.2 |
| RS-003 | Resident revision | resident_revisions content/parent/run/reason | all referenced resident-scoped entities = resident_revisions.resident_id; parent class also equal | SQLite trigger | trg_resident_revisions_resident_scope | DDL v0.1.2 |
| RS-004 | Revision approval | resident_revision_approvals.reason_content_id | reason content owner = approved revision resident | SQLite trigger | trg_resident_revision_approvals_resident_scope | DDL v0.1.2 |
| RS-005 | Revision activation | resident_revision_activations revision/approval/reason | revision resident = activation resident; approval is for same revision; reason content same resident | SQLite trigger | trg_resident_revision_activations_resident_scope | DDL v0.1.2 |
| RS-006 | Resident lifecycle | resident_status_transitions.reason_content_id | reason content owner = transition resident | SQLite trigger | trg_resident_status_transitions_resident_scope | DDL v0.1.2 |
| RS-007 | Blob/content | content_objects -> blobs | blob dedupe scope = content owner | SQLite composite FK | content_objects(owner_resident_id,algorithm,hash) -> blobs | already enforced |
| RS-008 | Erasure | content_erasure_events reason/source | reason and source erasure content owners = target content owner | SQLite trigger | trg_content_erasure_events_resident_scope | DDL v0.1.2 |
| RS-009 | Recall | recall_runs policy/query content | memory policy resident and query content owner = recall resident | SQLite trigger | trg_recall_runs_resident_scope | DDL v0.1.2 |
| RS-010 | Generation envelope | generation_runs revisions/recall | principles/persona/memory policy and recall run = generation resident | SQLite trigger | trg_generation_runs_resident_scope | DDL v0.1.2 |
| RS-011 | Generation input content | generation_run_inputs.content_id | rendered input content owner = generation resident | SQLite trigger | trg_generation_run_inputs_resident_scope | DDL v0.1.2 |
| RS-012 | Generation input provenance | generation_run_inputs.source_type/source_id | source entity resident = generation resident | Context Assembler + Canonical Writer | source_type-specific scope validator | Runtime |
| RS-013 | Generation outcome | generation_run_outcomes output/error content | output/error content owner = generation resident | SQLite trigger | trg_generation_run_outcomes_resident_scope | DDL v0.1.2 |
| RS-014 | Event ledger | events content/generation run | content owner and generation run resident = event resident | SQLite trigger | trg_events_resident_scope | DDL v0.1.2 |
| RS-015 | Claim envelope | claims statement/run | statement content owner and creator run resident = claim owner resident | SQLite trigger | trg_claims_resident_scope | DDL v0.1.2 |
| RS-016 | Evidence | claim_evidence claim/event/source/policy/run/reason | all resident-scoped provenance = claim owner resident | SQLite trigger | trg_claim_evidence_resident_scope | DDL v0.1.2 |
| RS-017 | Stage transition | claim_stage_transitions policy/run/reason | all resident-scoped references = target claim owner | SQLite trigger | trg_claim_stage_transitions_resident_scope | DDL v0.1.2 |
| RS-018 | Stage dependency | claim_stage_transition_dependencies | dependency claim owner = target claim owner | SQLite trigger | trg_claim_stage_dependency_resident_scope | DDL v0.1.2 |
| RS-019 | Claim relation | claim_relations endpoints/run/reason | both claims, optional run and reason content share resident | SQLite trigger | trg_claim_relations_resident_scope | DDL v0.1.2 |
| RS-020 | Integrity finding | integrity_findings claim/erasure/details | all referenced resident-scoped entities = finding.resident_id | SQLite trigger | trg_integrity_findings_resident_scope | DDL v0.1.2 |
| RS-021 | Status transition | claim_status_transitions typed triggers/policy/reason | all resident-scoped trigger and policy material = target claim owner | SQLite trigger + Writer | trg_claim_status_transitions_resident_scope + semantic validator | DDL v0.1.2 + Runtime |
| RS-022 | Validity assertion | claim_validity_assertions event/reason | evidence event and reason content = claim owner resident | SQLite trigger | trg_claim_validity_assertions_resident_scope | DDL v0.1.2 |
| RS-023 | View-scope assertion | claim_view_scope_assertions run/policy/reason | resident-scoped provenance = claim owner resident | SQLite trigger | trg_claim_view_scope_assertions_resident_scope | DDL v0.1.2 |
| RS-024 | Claim usage | claim_usages recall/run/detected/policy | all execution provenance = claim owner resident | SQLite trigger | trg_claim_usages_resident_scope | DDL v0.1.2 |
| RS-025 | Projection | resident_current_revision | revision and activation resident/class = projection resident/class | Projection Coordinator | projection rebuild validator | Runtime |
| RS-026 | Projection | resident_current_status | transition resident = projection resident | Projection Coordinator | projection rebuild validator | Runtime |
| RS-027 | Projection | claim_states | claim owner = projection resident | Projection Coordinator | projection rebuild validator | Runtime |
| RS-028 | Projection | claim_view_scope_current | claim and assertion resident = projection resident | Projection Coordinator | projection rebuild validator | Runtime |
| RS-029 | Projection | runtime_states | resident exists and seqs belong to resident ledger | Projection Coordinator | runtime-state evaluator | Runtime |
| RS-030 | Projection | content_references | content owner, logical referrer and dedupe scope = row resident | Projection Coordinator | content-reference builder | Runtime |
| RS-031 | Projection metadata | projection_watermark_dependencies memory_policy | memory policy revision belongs to watermark resident | Projection Coordinator | dependency resolver | Runtime |
| RS-032 | Operational | analytics/search outbox resident_id + source_commit_seq | resident_id must match commit scope when non-null | Outbox producer | outbox scope validator | Runtime |
| RS-033 | Operational | runtime_config.active_resident_id | selected resident exists; desired policy exists globally | Runtime Coordinator | config resolver | Runtime |
| RS-034 | Allowed cross-scope | claims.subject_principal_id / perspective_principal_id | may intentionally differ from owner resident | Allowed by design | none | allowed |
| RS-035 | Allowed cross-scope | events.actor_principal_id / target_principal_id | human/resident participants may differ from event resident identity | Allowed with event-type rules | Writer event actor/type validator | allowed |
| RS-036 | Allowed cross-scope | approval/activation/status/erasure actor principals | owner human/system actor may differ from resident principal | Permission layer | authorization validator | allowed |
| RS-037 | Allowed cross-scope | residents.parent_resident_id | parent and child are different residents by definition | Deferred Branch Copy Spec | branch validator | deferred |
| RS-038 | Global reference | pipeline/sessionization policy versions | no resident equality required | Allowed by design | version resolver | allowed |

## 3. DDL v0.1.2へ反映する境界

SQLite triggerへ落とすのは、immutable parent rowを主キーで引き、resident IDの等値を機械的に判定できる参照である。親rowが存在しない場合も`NOT EXISTS`でfail closedとする。

次はRuntimeへ残す。

- `generation_run_inputs.source_type / source_id`のpolymorphic scope解決
- Canonical Commit scopeと同一UoW所属
- Projection／Operationalのlogical reference
- event actor kind、automatic status transition、permission等の意味規則

## 4. 意図的に許可するcross-scope

- claimのsubject / perspective principal
- eventのactor / target principal
- owner human / system principalによる承認・管理操作
- branch lineageとしての`parent_resident_id`
- global pipeline / Sessionization Policy definition

これらをresident-scope triggerで拒否してはならない。multi-user化におけるprincipal分離と、resident所有データのscope分離は別責務である。

## 5. 完了条件

- DDL対象familyがnegative fixtureでcross-resident insertを拒否する
- Runtime対象familyがConstraint Enforcement Matrixのtest IDを持つ
- intentional cross-scope fixtureは拒否されない
- `PRAGMA foreign_key_check`と`quick_check`が成功する
