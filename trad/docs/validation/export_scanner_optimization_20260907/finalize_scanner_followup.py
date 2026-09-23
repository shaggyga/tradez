"""Create a separate unmapped scanner follow-up; preserve operational records."""
from pathlib import Path
from datetime import datetime, timezone
import argparse, hashlib, importlib.util, json, sys, xml.etree.ElementTree as ET

sys.dont_write_bytecode=True
OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
EVIDENCE=ROOT/'docs/validation/export_scanner_optimization_20260907'
REPORT=ROOT/'docs/FOREX_EXPORT_SCANNER_OPTIMIZATION_20260907.md'
RECEIPT=ROOT/'FOREX_EXPORT_SCANNER_OPTIMIZATION_VALIDATION_20260907.json'
RESUME=OUT.parent/'pair_news_review_20260907/resume_verified_export.py'
INTERRUPTION=OUT.parent/'pair_news_review_20260907/PUBLISHER_SCAN_INTERRUPTION.json'
SOURCE_SHA='c7e5fc81644c87d322b9f57c9e2a0d013e44f0c3d50b964dca268ee9aebf5d08'
TEST_SHA='a8bab2913da724ff29f8156c543f9e9ad5116f2d1595f3995293acbf6fb313d6'

def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def load(p): return json.loads(p.read_text(encoding='utf-8-sig'))
def binding(p): return {'path':p.relative_to(ROOT).as_posix(),'sha256':sha(p)}
def xml_counts(p):
    node=ET.fromstring(p.read_bytes())
    suites=[node] if node.tag=='testsuite' else list(node.iter('testsuite'))
    leaves=[s for s in suites if not list(s.findall('testsuite'))]
    return {key:sum(int(s.get(key,0)) for s in leaves) for key in ('tests','errors','failures','skipped')}
def passed_xml(p):
    result=xml_counts(p)
    if not result['tests'] or any(result[k] for k in ('errors','failures','skipped')):
        raise ValueError('required_test_xml_not_clean:'+str(p))
    return result

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--broader-tests',type=Path,required=True)
    parser.add_argument('--independent-review',type=Path,required=True)
    parser.add_argument('--root-review',type=Path,required=True)
    args=parser.parse_args()
    required=[OUT/'before_source/credential_audit.py',OUT/'scanner_tests.xml',
              OUT/'scanner_linear_tests.xml',OUT/'SCANNER_SYNTHETIC_PERFORMANCE_20260907.json',
              OUT/'SCANNER_IMPLEMENTATION_VALIDATION_20260907.json',
              OUT/'vault_integration_initial_collection_error.xml',
              args.broader_tests,args.independent_review,args.root_review]
    if any(not p.is_file() for p in required+[RESUME,INTERRUPTION]): raise ValueError('missing_required_evidence')
    if sha(ROOT/'tools/credential_audit.py')!=SOURCE_SHA or sha(ROOT/'test_credential_audit_linear_assignment.py')!=TEST_SHA:
        raise ValueError('frozen_scanner_source_or_test_changed')
    targeted=passed_xml(OUT/'scanner_linear_tests.xml')
    broader=passed_xml(args.broader_tests)
    if targeted['tests']!=64: raise ValueError('unexpected_targeted_case_count')
    initial=xml_counts(OUT/'scanner_tests.xml')
    if initial!={'tests':132,'errors':0,'failures':1,'skipped':0}: raise ValueError('initial_failed_evidence_changed')
    for p in [args.independent_review,args.root_review,OUT/'SCANNER_SYNTHETIC_PERFORMANCE_20260907.json',OUT/'SCANNER_IMPLEMENTATION_VALIDATION_20260907.json']:
        if load(p).get('status')!='passed': raise ValueError('required_review_not_passed:'+str(p))
    spec=importlib.util.spec_from_file_location('resume_preflight',RESUME)
    resume=importlib.util.module_from_spec(spec);spec.loader.exec_module(resume)
    sys.path[:0]=[str(ROOT),str(ROOT.parent)]
    import forex_model_vault_sync as records
    from tools import vault_worktree_snapshot as snapshot
    operational=resume.operational_bindings()
    resume.canonical_records(records)
    backup_sha=resume.backup_bindings()
    pointer=load(resume.VAULT/'source/WORKTREE_SOURCE_LATEST.json')
    if pointer['archive']!=resume.PRIOR_ARCHIVE or sha(resume.VAULT/'source'/resume.PRIOR_ARCHIVE)!=resume.PRIOR_ARCHIVE_SHA:
        raise ValueError('preceding_archive_changed')
    if REPORT.exists() or RECEIPT.exists() or EVIDENCE.exists(): raise ValueError('followup_already_exists')
    copies=[]
    for p in required:
        relative=p.relative_to(OUT) if p.is_relative_to(OUT) else Path(p.name)
        copies.append((p,EVIDENCE/relative))
    copies += [(RESUME,EVIDENCE/RESUME.name),(INTERRUPTION,EVIDENCE/INTERRUPTION.name),
               (Path(__file__),EVIDENCE/Path(__file__).name)]
    if len({p[1] for p in copies})!=len(copies): raise ValueError('duplicate_evidence_destination')
    for source,target in copies:
        target.parent.mkdir(parents=True,exist_ok=True)
        with target.open('xb') as handle: handle.write(source.read_bytes())
        if sha(source)!=sha(target): raise ValueError('evidence_copy_changed')
    report=f'''# Export credential scanner performance repair — September 7, 2026

The shared source-export credential scanner now handles long encoded evidence without repeatedly searching the same identifier run. The operational dashboard validation remains unchanged: receipt `{resume.RECEIPT_SHA}`, all 87 bound source/evidence files, and all 149 already-copied canonical vault records were verified before this separate follow-up was written. This report and its receipt are deliberately outside the canonical mapping.

## Cause and scope

The first operational publisher copied the 149 records and preserved the previous records, then spent more than 16 minutes in source credential scanning before ZIP staging. The new minute-gap source capture contains a 252,696-byte uninterrupted encoded identifier run. The old assignment expression retried an unbounded greedy key prefix at each byte, producing quadratic work on that run. At 18:58:26 UTC the verified publisher child was stopped; its matching launcher exited. No other process was targeted, no archive had been staged, and the prior source pointer was preserved. The interruption evidence retains the exact process identities and clocks.

Only the greedy API-key/token branch now requires the beginning of an identifier run, with the assertion after the optional opening quote. The other alternatives retain their existing suffix matches. Replacing that expression with the prior expression makes the scanner AST identical to its saved predecessor. No detection rule, path restriction, fixture exemption, reference exemption, or archive check was removed or relaxed.

The supported equivalence is the scanner's actual full-payload `finditer` call pattern. Explicit starting positions inside an identifier are outside this equivalence claim and are not used by the scanner. Match spans, captured values, and Python-reference classification were compared using constructed data; no private credential file was read for these tests.

## Validation

- Final targeted suite: {targeted['tests']} pytest cases passed, with no failures, errors, or skips.
- Root's broader impacted scanner/source-vault regression suite: {broader['tests']} pytest cases passed, with no failures, errors, or skips. These runs may overlap and are not added together.
- The first broader-suite invocation used the project subdirectory and failed test collection because the `trad` package was not importable from that working directory. Its original collection-error XML is preserved. Running from the Forex parent directory resolved this harness-only error; no product source changed.
- Independent generated comparisons: 50,736 full-payload span/capture cases and 1,728 Python-reference cases, with zero differences. These are generated comparisons, not additional pytest cases.
- The initial 132-case run is retained: 131 passed and one newly added test's own source failed the existing archive audit. The synthetic fixture's string construction was corrected; no scanner rule or exemption changed. The existing 68 credential/snapshot cases passed on the same final scanner source.
- A constructed 252,696-byte encoded run took approximately 0.056 seconds for the assignment scan and 0.077 seconds through all six audit rules on this machine. A credential-shaped alert placed after the run was still detected in approximately 0.063 seconds. The old expression was not rerun on that large input, so no measured speedup ratio is claimed.

The saved prior scanner, final implementation receipt, both targeted XML runs, independent review, synthetic timing result, broader regression XML, root review, interruption evidence, and exact source-only resume helper are retained under `docs/validation/export_scanner_optimization_20260907` and bound by the separate scanner receipt.

## Source-only export continuation

The resume helper checks the unchanged operational receipt and all 87 bindings, the source and target bytes of all 149 already-copied records, the original backup, the three preceding receipts, and the preceding archive `forex_worktree_source_885144f85e85de600e4470d8.zip` with SHA-256 `{resume.PRIOR_ARCHIVE_SHA}` before publication. It also checks its own exact source copy against this scanner receipt.

It then resumes only source-archive construction and the remaining extraction, source-hash, compilation, credential, backup, and local-record verification. It does not repeat canonical writes or recreate the backup. Successful completion is recorded separately in vault maintenance, including verified copies of this unmapped report and its receipt. This pre-export validation does not claim that the resumed archive has already succeeded; the maintenance export receipt records that outcome.

Only local OneDrive files are verified; cloud synchronization is not observed. Private databases and credentials remain outside the source archive. This export performance repair does not change the registered studies, runtime trading authorization, forecasts, or prediction-performance claims. Orders remain disabled.
'''
    REPORT.write_text(report,encoding='utf-8')
    paths=[ROOT/'tools/credential_audit.py',ROOT/'test_credential_audit_linear_assignment.py',REPORT]+[target for _,target in copies]
    for p in paths: snapshot.audit_payload(p.relative_to(ROOT).as_posix(),p.read_bytes(),set())
    value={'schema_version':'forex_export_scanner_optimization_validation_v1_20260907',
           'observed_utc':datetime.now(timezone.utc).isoformat(),'status':'passed',
           'scope':'Full-payload credential assignment scanning performance only; separate unmapped follow-up.',
           'report':binding(REPORT),'tests':{'targeted':targeted,'broader_impacted':broader,'counts_are_not_additive':True},
           'independent_generated_comparisons':{'span_capture':50736,'python_reference':1728,'differences':0,'are_pytest_cases':False},
           'initial_run_preserved':initial,'operational_receipt_unchanged':{'path':resume.RECEIPT,'sha256':resume.RECEIPT_SHA,'verified_bindings':87},
           'canonical_mapping_count_unchanged':149,'canonical_records_recopied':False,
           'prior_receipts_unchanged':resume.PRIOR,'prior_archive_preserved':{'archive':resume.PRIOR_ARCHIVE,'sha256':resume.PRIOR_ARCHIVE_SHA},
           'preserved_backup_index_sha256':backup_sha,'source_bindings':[binding(p) for p in paths],
           'rules_removed':0,'exemptions_added':0,'archive_resume_completed_at_validation':False,
           'research_only':True,'can_place_orders':False,'runtime_changes':False,'deletions':0,'git_commit_created':False,
           'limitations':['Parity covers existing full-payload calls, not arbitrary explicit starting positions.',
                         'Synthetic timings are a bounded local observation, not a full export benchmark.',
                         'Separate maintenance receipt is required to establish completed source export.',
                         'Local OneDrive verification does not observe cloud synchronization.']}
    payload=(json.dumps(value,indent=2)+'\n').encode('utf-8')
    snapshot.audit_payload(RECEIPT.name,payload,set())
    with RECEIPT.open('xb') as handle:handle.write(payload)
    resume.operational_bindings();resume.canonical_records(records)
    print(json.dumps({'status':'passed','receipt':str(RECEIPT),'sha256':sha(RECEIPT),
                     'binding_count':len(paths),'targeted_tests':targeted,'broader_tests':broader,
                     'operational_bindings_unchanged':87,'canonical_records_unchanged':149}))

if __name__=='__main__':main()
