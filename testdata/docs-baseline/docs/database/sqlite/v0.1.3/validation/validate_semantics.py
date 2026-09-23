from __future__ import annotations
import argparse
import pathlib
import subprocess
import sqlite3
import hashlib
import json
import re
import sys
from dataclasses import dataclass

BASE = pathlib.Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument('--head', choices=('v10', 'v11', 'v12', 'v13', 'all'), required=True)
mode = parser.add_mutually_exclusive_group(required=True)
mode.add_argument('--write', action='store_true')
mode.add_argument('--check', action='store_true')
args = parser.parse_args()

if args.head == 'all':
    selected_mode = '--write' if args.write else '--check'
    results = [subprocess.run([sys.executable, str(pathlib.Path(__file__)), '--head', head, selected_mode]).returncode for head in ('v10', 'v11', 'v12', 'v13')]
    raise SystemExit(max(results, default=0))

SCHEMA = (BASE / f'schema_{args.head}_v0.1.3.sql').read_text(encoding='utf-8')
REPORT = BASE / 'reports' / f'validation_report.semantic_{args.head}.json'

class IDs:
    def __init__(self): self.n=1
    def new(self):
        v=f'{self.n:026d}'
        self.n+=1
        return v
ids=IDs()

def d(label:str)->bytes: return hashlib.sha256(label.encode()).digest()

con=sqlite3.connect(':memory:')
con.execute('PRAGMA foreign_keys=ON')
con.executescript(SCHEMA)
con.execute('BEGIN')

# Core IDs
C_G,C_A,C_B=ids.new(),ids.new(),ids.new()
P_SYS,P_HUMAN,P_RA,P_RB,P_C=ids.new(),ids.new(),ids.new(),ids.new(),ids.new()
R_A,R_B,R_C=ids.new(),ids.new(),ids.new()
T=1_700_000_000_000_000
TZ='Asia/Tokyo'

con.execute('INSERT INTO canonical_commits VALUES (?,?,?,?,?)',(C_G,1,None,T,TZ))
con.execute('INSERT INTO canonical_commits VALUES (?,?,?,?,?)',(C_A,2,R_A,T+1,TZ))
con.execute('INSERT INTO canonical_commits VALUES (?,?,?,?,?)',(C_B,3,R_B,T+2,TZ))
for pid,commit,kind,name in [(P_SYS,C_G,'system','system'),(P_HUMAN,C_G,'human','owner'),(P_RA,C_A,'resident','A'),(P_RB,C_B,'resident','B'),(P_C,C_G,'resident','C')]:
    con.execute('INSERT INTO principals VALUES (?,?,?,?,?,?)',(pid,commit,kind,name,T,TZ))
con.execute('INSERT INTO residents VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',(R_A,C_A,P_RA,'A',None,None,None,None,None,None,T,TZ))
con.execute('INSERT INTO residents VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',(R_B,C_B,P_RB,'B',None,None,None,None,None,None,T,TZ))
con.commit()

content_counter=0
def content(resident:str, cls:str, text:str, policy:str|None=None)->str:
    global content_counter
    content_counter+=1
    cid=ids.new(); h=d(f'blob:{resident}:{content_counter}:{text}'); salt=d(f'salt:{resident}:{content_counter}'); commitment=d(f'commit:{resident}:{content_counter}:{text}')
    if policy is None:
        policy='resident_only' if cls in ('principles_text','persona_text','memory_policy_text') else 'independent'
    con.execute('INSERT INTO blobs VALUES (?,?,?,?,?,?,?,?,?)',(resident,'sha256',h,text.encode(),len(text.encode()),'utf-8','none',T,TZ))
    con.execute('INSERT INTO content_objects VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(cid,resident,cls,h,'sha256',commitment,salt,'sha256','mahoroba:content-commitment:v1','mahoroba-jcs-v1','present',policy,T,TZ))
    return cid

# Resident-owned content
CA={}
CB={}
for key,cls in [('principles','principles_text'),('persona','persona_text'),('memory','memory_policy_text'),('event','event_payload'),('claim','claim_statement'),('reason','reason_text'),('input','generation_input'),('output','generation_output'),('error','error_detail'),('description','reason_text')]:
    CA[key]=content(R_A,cls,f'A-{key}')
    CB[key]=content(R_B,cls,f'B-{key}')

# Global version definitions
PV=ids.new(); SP=ids.new()
con.execute('INSERT INTO pipeline_versions VALUES (?,?,?,?,?,?,?)',(PV,C_G,'memory','v1','{}',T,TZ))
con.execute('INSERT INTO sessionization_policy_versions VALUES (?,?,?,?,?,?)',(SP,C_G,'v1','{}',T,TZ))

# Revisions
REV={}
for r,c,commit,prefix in [(R_A,CA,C_A,'A'),(R_B,CB,C_B,'B')]:
    for cls,key in [('principles','principles'),('persona','persona'),('memory_policy','memory')]:
        rid=ids.new(); REV[(prefix,cls)]=rid
        con.execute('INSERT INTO resident_revisions VALUES (?,?,?,?,?,?,?,?,?,?)',(rid,commit,r,cls,c[key],None,None,None,T,TZ))

# Approval baseline
APP={}
for prefix,commit in [('A',C_A),('B',C_B)]:
    aid=ids.new(); APP[prefix]=aid
    con.execute('INSERT INTO resident_revision_approvals VALUES (?,?,?,?,?,?,?,?)',(aid,commit,REV[(prefix,'principles')],P_HUMAN,'approved',None,T,TZ))

# Recall and generation runs
RECALL={}; RUN={}
for prefix,r,commit,c in [('A',R_A,C_A,CA),('B',R_B,C_B,CB)]:
    rr=ids.new(); RECALL[prefix]=rr
    con.execute('INSERT INTO recall_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',(rr,commit,r,None,'{}',PV,REV[(prefix,'memory_policy')],T,TZ,'{}',T,TZ))
    gr=ids.new(); RUN[prefix]=gr
    con.execute('INSERT INTO generation_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(
        gr,commit,r,'memory_extraction',f'idem-{prefix}','test','model',None,'prompt-v1',PV,'context-v1',SP,'render-v1',
        REV[(prefix,'principles')],REV[(prefix,'persona')],REV[(prefix,'memory_policy')],rr,None,None,None,None,'{}',T,TZ,0,'{}',T,TZ))

# Events
EVENT={}
for prefix,r,commit,c,seq,actor,target in [('A',R_A,C_A,CA,1,P_HUMAN,P_RA),('B',R_B,C_B,CB,1,P_HUMAN,P_RB)]:
    eid=ids.new(); EVENT[prefix]=eid
    con.execute('INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(
        eid,commit,r,seq,'user_message','conversation',1,0,'local_ui','trusted',actor,target,None,T,TZ,T,TZ,c['event'],d(f'payload-{prefix}'),None,d(f'event-{prefix}'),'sha256','mahoroba:event-hash:v1','mahoroba-jcs-v1'))

# Claims
CLAIM={}
for prefix,r,commit,c in [('A',R_A,C_A,CA),('B',R_B,C_B,CB)]:
    cid=ids.new(); CLAIM[prefix]=cid
    con.execute('INSERT INTO claims VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(
        cid,commit,r,P_HUMAN,P_RA if prefix=='A' else P_RB,'other','stable',c['claim'],d(f'statement-{prefix}'),'sha256','norm-v1',RUN[prefix],T,TZ))

# Evidence and initial stage
EVID={}; STAGE={}
for prefix,commit in [('A',C_A),('B',C_B)]:
    ev=ids.new(); EVID[prefix]=ev
    con.execute('INSERT INTO claim_evidence VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(
        ev,commit,CLAIM[prefix],EVENT[prefix],'support','stated','trusted',1_000_000,'extracted',None,REV[(prefix,'memory_policy')],RUN[prefix],'explicit_statement',None,T,TZ))
    st=ids.new(); STAGE[prefix]=st
    con.execute('INSERT INTO claim_stage_transitions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(
        st,commit,CLAIM[prefix],None,'floating','{}',PV,REV[(prefix,'memory_policy')],RUN[prefix],'initial',None,T,TZ,T,TZ))

# Valid erasure and finding per resident
ERASE={}; FINDING={}
for prefix,commit,c,r in [('A',C_A,CA,R_A),('B',C_B,CB,R_B)]:
    ee=ids.new(); ERASE[prefix]=ee
    con.execute('INSERT INTO content_erasure_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',(ee,commit,c['event'],'content',P_HUMAN,'test',None,None,T,TZ,T,TZ))
    fi=ids.new(); FINDING[prefix]=fi
    con.execute('''INSERT INTO integrity_findings(
        integrity_finding_id, canonical_commit_id, resident_id, claim_id,
        finding_kind, source_content_erasure_event_id, pipeline_version_id,
        details_content_id, occurred_at, occurred_tz, recorded_at, recorded_tz
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',(fi,commit,r,CLAIM[prefix],'provenance_unresolvable',ee,PV,c['reason'],T,TZ,T,TZ))
con.commit()

# Helpers
results=[]
def expect_block(name, sql, params):
    con.execute('SAVEPOINT t')
    try:
        con.execute(sql,params)
        con.execute('RELEASE t')
        results.append((name,False,'unexpectedly allowed'))
    except sqlite3.IntegrityError as e:
        con.execute('ROLLBACK TO t'); con.execute('RELEASE t')
        results.append((name,True,str(e)))

def expect_allow(name, sql, params):
    con.execute('SAVEPOINT t')
    try:
        con.execute(sql,params)
        con.execute('ROLLBACK TO t'); con.execute('RELEASE t')
        results.append((name,True,'allowed'))
    except Exception as e:
        con.execute('ROLLBACK TO t'); con.execute('RELEASE t')
        results.append((name,False,str(e)))

# U-1 duplicate stage
expect_block('U1_duplicate_claim_stage', 'INSERT INTO claim_stage_transitions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
    (ids.new(),C_A,CLAIM['A'],None,'floating','{}',PV,REV[('A','memory_policy')],RUN['A'],'initial',None,T,TZ,T,TZ))
# U-4 duplicate event hash
expect_block('U4_duplicate_event_hash','INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
    (ids.new(),C_A,R_A,2,'user_message','conversation',1,0,'local_ui','trusted',P_HUMAN,P_RA,None,T,TZ,T,TZ,CA['event'],d('payload-A'),d('event-A'),d('event-A'),'sha256','mahoroba:event-hash:v1','mahoroba-jcs-v1'))

# Scope trigger tests, one per trigger family.
expect_block('RS_resident_description','INSERT INTO residents VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',(R_C,C_G,P_C,'C',CB['description'],None,None,None,None,None,T,TZ))
expect_block('RS_revision','INSERT INTO resident_revisions VALUES (?,?,?,?,?,?,?,?,?,?)',(ids.new(),C_A,R_A,'persona',CB['persona'],None,None,None,T,TZ))
expect_block('RS_revision_approval','INSERT INTO resident_revision_approvals VALUES (?,?,?,?,?,?,?,?)',(ids.new(),C_A,REV[('A','principles')],P_HUMAN,'approved',CB['reason'],T,TZ))
expect_block('RS_revision_activation','INSERT INTO resident_revision_activations VALUES (?,?,?,?,?,?,?,?,?,?)',(ids.new(),C_A,R_A,REV[('B','principles')],P_HUMAN,None,'activate',None,T,TZ))
expect_block('RS_resident_status','INSERT INTO resident_status_transitions VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',(ids.new(),C_A,R_A,None,'draft',P_HUMAN,'create',CB['reason'],T,TZ,T,TZ))
expect_block('RS_erasure','INSERT INTO content_erasure_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',(ids.new(),C_A,CA['event'],'content',P_HUMAN,'test',CB['reason'],None,T,TZ,T,TZ))
expect_block('RS_recall','INSERT INTO recall_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',(ids.new(),C_A,R_A,None,'{}',PV,REV[('B','memory_policy')],T,TZ,'{}',T,TZ))
expect_block('RS_generation_run','INSERT INTO generation_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(
    ids.new(),C_A,R_A,'dialogue','bad-gen','test','model',None,'prompt-v1',PV,'context-v1',SP,'render-v1',REV[('B','principles')],REV[('A','persona')],REV[('A','memory_policy')],None,None,None,None,None,'{}',T,TZ,0,'{}',T,TZ))
expect_block('RS_generation_input_content','INSERT INTO generation_run_inputs VALUES (?,?,?,?,?,?,?,?,?,?,?)',(ids.new(),C_A,RUN['A'],0,'user','event',EVENT['A'],'current_input',CB['input'],T,TZ))
expect_block('RS_generation_outcome_content','INSERT INTO generation_run_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(ids.new(),C_A,RUN['A'],0,'succeeded',CB['output'],1,1,1,0,None,None,T,TZ))
expect_block('RS_event','INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(
    ids.new(),C_A,R_A,2,'user_message','conversation',1,0,'local_ui','trusted',P_HUMAN,P_RA,None,T,TZ,T,TZ,CB['event'],d('payload-x'),d('event-A'),d('event-x'),'sha256','mahoroba:event-hash:v1','mahoroba-jcs-v1'))
expect_block('RS_claim','INSERT INTO claims VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(ids.new(),C_A,R_A,P_HUMAN,P_RA,'other','stable',CB['claim'],d('bad-claim'),'sha256','norm-v1',RUN['A'],T,TZ))
expect_block('RS_evidence','INSERT INTO claim_evidence VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(
    ids.new(),C_A,CLAIM['A'],EVENT['B'],'support','stated','trusted',1_000_000,'extracted',None,REV[('A','memory_policy')],RUN['A'],'x',None,T,TZ))
expect_block('RS_stage','INSERT INTO claim_stage_transitions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(
    ids.new(),C_A,CLAIM['A'],'floating','sediment','{}',PV,REV[('B','memory_policy')],RUN['A'],'x',None,T,TZ,T,TZ))
expect_block('RS_stage_dependency','INSERT INTO claim_stage_transition_dependencies VALUES (?,?,?,?,?)',(ids.new(),C_A,STAGE['A'],'meta_alignment',CLAIM['B']))
expect_block('RS_relation','INSERT INTO claim_relations VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',(ids.new(),C_A,CLAIM['A'],CLAIM['B'],'contradicts','x',None,None,T,TZ,T,TZ))
expect_block('RS_integrity','''INSERT INTO integrity_findings(
    integrity_finding_id, canonical_commit_id, resident_id, claim_id,
    finding_kind, source_content_erasure_event_id, pipeline_version_id,
    details_content_id, occurred_at, occurred_tz, recorded_at, recorded_tz
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',(ids.new(),C_A,R_A,CLAIM['B'],'provenance_unresolvable',None,PV,None,T,TZ,T,TZ))
expect_block('RS_status','INSERT INTO claim_status_transitions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(
    ids.new(),C_A,CLAIM['A'],'active','quarantined','automatic',None,'integrity_finding',None,None,None,FINDING['B'],PV,None,'{}','structural_quarantine',None,T,TZ,T,TZ))
expect_block('RS_validity','INSERT INTO claim_validity_assertions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(
    ids.new(),C_A,CLAIM['A'],'observed',None,None,None,None,EVENT['B'],1_000_000,P_HUMAN,'x',None,T,TZ))
expect_block('RS_view_scope','INSERT INTO claim_view_scope_assertions VALUES (?,?,?,?,?,?,?,?,?,?,?)',(
    ids.new(),C_A,CLAIM['A'],'resident_ui',None,RUN['B'],REV[('A','memory_policy')],'x',None,T,TZ))
expect_block('RS_usage','INSERT INTO claim_usages VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(
    ids.new(),C_A,CLAIM['A'],RECALL['B'],None,'candidate',0,REV[('A','memory_policy')],None,None,None,None,T,TZ))

# Intentional principal cross-scope should remain allowed: A claim about human owner and event targeted at resident principal.
# Existing baseline rows already demonstrate these; also test a new valid A claim with human subject.
valid_claim=ids.new(); valid_content=content(R_A,'claim_statement','A-valid-cross-principal')
expect_allow('ALLOW_claim_principal_cross_scope','INSERT INTO claims VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(
    valid_claim,C_A,R_A,P_HUMAN,P_RA,'other','stable',valid_content,d('valid-cross-principal'),'sha256','norm-v1',RUN['A'],T,TZ))

if args.head in ('v12', 'v13'):
    con.execute('''INSERT INTO claim_validity_assertions(
        validity_assertion_id, canonical_commit_id, claim_id, assertion_type,
        valid_from, valid_from_tz, valid_to, valid_to_tz, evidence_event_id,
        confidence, actor_principal_id, reason_code, reason_content_id,
        recorded_at, recorded_tz
    ) VALUES (?,?,?,'observed',NULL,NULL,NULL,NULL,?,1000000,?,'test',NULL,?,?)''',
        (ids.new(),C_A,CLAIM['A'],EVENT['A'],P_HUMAN,T,TZ))
    expect_block('M7_duplicate_validity_entity_commit','''INSERT INTO claim_validity_assertions(
        validity_assertion_id, canonical_commit_id, claim_id, assertion_type,
        valid_from, valid_from_tz, valid_to, valid_to_tz, evidence_event_id,
        confidence, actor_principal_id, reason_code, reason_content_id,
        recorded_at, recorded_tz
    ) VALUES (?,?,?,'observed',NULL,NULL,NULL,NULL,?,1000000,?,'test',NULL,?,?)''',
        (ids.new(),C_A,CLAIM['A'],EVENT['A'],P_HUMAN,T,TZ))
    con.execute('''INSERT INTO claim_view_scope_assertions(
        view_scope_assertion_id, canonical_commit_id, claim_id, view_scope,
        actor_principal_id, generation_run_id, memory_policy_revision_id,
        reason_code, reason_content_id, recorded_at, recorded_tz
    ) VALUES (?,?,?,'resident_ui',?,NULL,NULL,'test',NULL,?,?)''',
        (ids.new(),C_A,CLAIM['A'],P_HUMAN,T,TZ))
    expect_block('M7_duplicate_view_scope_entity_commit','''INSERT INTO claim_view_scope_assertions(
        view_scope_assertion_id, canonical_commit_id, claim_id, view_scope,
        actor_principal_id, generation_run_id, memory_policy_revision_id,
        reason_code, reason_content_id, recorded_at, recorded_tz
    ) VALUES (?,?,?,'resident_ui',?,NULL,NULL,'test',NULL,?,?)''',
        (ids.new(),C_A,CLAIM['A'],P_HUMAN,T,TZ))
    integrity_pipeline = ids.new()
    con.execute('INSERT INTO pipeline_versions VALUES (?,?,?,?,?,?,?)',
        (integrity_pipeline,C_G,'integrity_check','integrity-check-v1','{}',T,TZ))
    expect_block('M7_integrity_envelope_required','''INSERT INTO integrity_findings(
        integrity_finding_id, canonical_commit_id, resident_id, claim_id,
        finding_kind, source_content_erasure_event_id, pipeline_version_id,
        details_content_id, occurred_at, occurred_tz, recorded_at, recorded_tz
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',
        (ids.new(),C_A,R_A,CLAIM['A'],'canonical_invariant_violation',None,integrity_pipeline,None,T,TZ,T,TZ))
    expect_allow('M7_integrity_envelope_complete','''INSERT INTO integrity_findings(
        integrity_finding_id, canonical_commit_id, resident_id, claim_id,
        finding_kind, source_content_erasure_event_id, pipeline_version_id,
        details_content_id, occurred_at, occurred_tz, recorded_at, recorded_tz,
        finding_fingerprint, rule_code, target_kind, target_id, target_field
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
        (ids.new(),C_A,R_A,CLAIM['A'],'canonical_invariant_violation',None,integrity_pipeline,None,T,TZ,T,TZ,
         d('finding-fingerprint'),'claim_statement_pairing','claim',CLAIM['A'],'statement_hash'))

if args.head == 'v13':
    index_columns = [row[2] for row in con.execute(
        "PRAGMA index_info('idx_generation_runs_commit_resident_purpose_run')"
    ).fetchall()]
    expected_columns = ['canonical_commit_id', 'resident_id', 'purpose', 'generation_run_id']
    results.append((
        'COVR02_generation_run_progress_index',
        index_columns == expected_columns,
        'columns=' + ','.join(index_columns),
    ))

quick=con.execute('PRAGMA quick_check').fetchone()[0]
fk=con.execute('PRAGMA foreign_key_check').fetchall()
triggers=con.execute("SELECT count(*) FROM sqlite_master WHERE type='trigger'").fetchone()[0]
indexes=con.execute("SELECT count(*) FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'").fetchone()[0]
failed=[r for r in results if not r[1]]
report={
  'head': args.head,
  'schema_head': {'v10': 10, 'v11': 11, 'v12': 12, 'v13': 13}[args.head],
  'application_tables': con.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchone()[0],
  'named_indexes': indexes,
  'triggers': triggers,
  'quick_check': quick,
  'foreign_key_violations': len(fk),
  'negative_positive_tests': [{'name':n,'passed':ok,'detail':detail} for n,ok,detail in results],
  'tests_passed': sum(1 for _,ok,_ in results if ok),
  'tests_total': len(results),
  'failed_tests': [n for n,ok,_ in failed],
}
body = json.dumps(report, indent=2, ensure_ascii=False) + '\n'
if args.write:
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(body, encoding='utf-8', newline='\n')
elif not REPORT.exists() or REPORT.read_text(encoding='utf-8') != body:
    print(f'{REPORT}: generated content differs', file=sys.stderr)
    failed.append(('generated_report_is_current', False, 'report mismatch'))
print(json.dumps(report,indent=2,ensure_ascii=False))
if failed or quick!='ok' or fk:
    sys.exit(1)
