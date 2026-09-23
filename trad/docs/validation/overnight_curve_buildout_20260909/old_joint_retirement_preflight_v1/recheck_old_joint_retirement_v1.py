"""New coherent RO audit and exact retained-obligation comparison; never stops work."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys

sys.dont_write_bytecode=True
BASE=Path(__file__).resolve().parent
AUDITOR=BASE/'audit_old_joint_obligations_v1.py'
# Assigned after the bounded auditor review fixes; no runtime source is changed.
AUDITOR_SHA='d454833f16fd2c3135f6a23a6176233bfb446b22fdf0ebc539d9ff54e831b8e5'


def need(ok,reason):
    if not ok:raise ValueError(reason)


def sha(raw):return hashlib.sha256(raw).hexdigest()


def load_auditor():
    need(sha(AUDITOR.read_bytes())==AUDITOR_SHA,'auditor_source_changed')
    spec=importlib.util.spec_from_file_location('retirement_pinned_auditor',AUDITOR)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def inventory(report):
    need(report.get('status')=='no_retirement_obligation_blocker_observed' and report.get('all_registered_sources_unchanged') is True,'audit_blocked_or_changed')
    rows=report.get('ledgers');need(type(rows) is list and len(rows)==136,'exact_136_ledgers_required')
    result={}
    for row in rows:
        key=(row['study'],row['instrument']);need(key not in result,'duplicate_ledger')
        need(row.get('status')=='no_retirement_obligation_blocker_observed' and row.get('issues')==[],'ledger_blocked')
        for field in ('immutable_obligation_records_sha256','selected_quote_evidence_sha256'):
            need(type(row.get(field)) is str and len(row[field])==64,'missing_identity')
        result[key]={k:row[k] for k in ('row_counts','original_target_max_epoch','immutable_obligation_records_sha256','selected_quote_evidence_sha256')}
    need({k[0] for k in result}=={'v1','v2'} and all(sum(k[0]==s for k in result)==68 for s in ('v1','v2')),'study_inventory')
    return result


def compare(before,after):
    old=inventory(before);new=inventory(after)
    need(before['source_bindings']==after['source_bindings'],'registered_source_difference')
    need(set(old)==set(new),'ledger_identity_set_changed')
    changed=[{'study':k[0],'instrument':k[1],'changed_fields':[field for field in old[k] if old[k][field]!=new[k][field]]} for k in sorted(old) if old[k]!=new[k]]
    return dict(status='unchanged_terminal_obligations' if not changed else 'blocked_records_changed',
        compared_ledgers=136,changed_ledgers=changed,
        unchanged_original_obligation_records_and_selected_quotes=not changed,
        scope='Exact contracts/activation and original forecast/publication/consumption/entry/outcome/exclusion identity plus referenced quote evidence; unrelated growing quote rows excluded. No stop or settlement.')


def read_report(path,expected,auditor):
    raw=auditor.read(path,2*1024*1024);need(sha(raw)==expected,'report_hash')
    value=json.loads(raw);inventory(value)
    for row in value['ledgers']:
        detail=Path(row['path']);need(detail.parent==path.parent/'ledgers','detail_path_escape')
        body=auditor.read(detail,16*1024*1024);need(sha(body)==row['sha256'],'ledger_detail_hash')
        item=json.loads(body)
        need(item['immutable_obligation_records_sha256']==row['immutable_obligation_records_sha256'],'detail_obligation_identity')
        need(auditor.digest(dict(contract_sha256=item['contract_sha256'],activation=item['activation'],tables=item['immutable_records']))==item['immutable_obligation_records_sha256'],'detail_records_hash')
        need(auditor.selected_quote_identity(item['selected_quote_evidence'])==row['selected_quote_evidence_sha256']==item['selected_quote_evidence_sha256'],'detail_quote_identity')
    return value


def run(baseline,baseline_sha,output):
    auditor=load_auditor();own=Path(__file__).read_bytes()
    need(baseline.parent.parent==BASE and output.parent==BASE and not output.exists(),'bounded_external_paths')
    before=read_report(baseline,baseline_sha,auditor)
    auditor.run(output)
    target=output/'OLD_JOINT_RETIREMENT_OBLIGATIONS_20260909.json';actual_sha=sha(target.read_bytes())
    after=read_report(target,actual_sha,auditor);result=compare(before,after)
    need(sha(AUDITOR.read_bytes())==AUDITOR_SHA and Path(__file__).read_bytes()==own,'helper_changed')
    result.update(schema_version='old_joint_retirement_identity_recheck_v1_20260909',
        baseline={'path':str(baseline),'sha256':baseline_sha},current={'path':str(target),'sha256':actual_sha},
        auditor_sha256=AUDITOR_SHA,recheck_helper_sha256=sha(own),completed_epoch=auditor.time.time(),runtime_actions=False)
    path=output/'OLD_JOINT_RETIREMENT_IDENTITY_COMPARISON_20260909.json'
    raw=auditor.encoded(result)+b'\n'
    with path.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
    print(json.dumps({'path':str(path),'sha256':sha(raw),'status':result['status']}),flush=True)
    need(result['status']=='unchanged_terminal_obligations','retirement_identity_changed')
    return result


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--baseline',type=Path,required=True);p.add_argument('--baseline-sha256',required=True);p.add_argument('--output-directory',type=Path,required=True)
    a=p.parse_args();run(a.baseline.absolute(),a.baseline_sha256,a.output_directory.absolute())
