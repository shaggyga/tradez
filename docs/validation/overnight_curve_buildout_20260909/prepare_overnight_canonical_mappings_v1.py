"""Add two documentation aliases; preserve every existing mapping and function."""
import ast
import hashlib
import json
from pathlib import Path
import time

BASE=Path(__file__).resolve().parent
ROOT=BASE.parent/'trad'
SOURCE=ROOT/'forex_model_vault_sync.py'


def sha(raw):return hashlib.sha256(raw).hexdigest()


def main():
    before=SOURCE.read_bytes();old=ast.parse(before)
    needle=b'CANONICAL_PROJECT_RECORDS = (\n'
    additions=(b'    (Path("trad/docs/FOREX_OVERNIGHT_CURVE_BUILDOUT_20260909.md"), "OVERNIGHT_CURVE_BUILDOUT_CURRENT.md"),\n'
               b'    (Path("trad/FOREX_OVERNIGHT_CURVE_BUILDOUT_VALIDATION_20260909.json"), "OVERNIGHT_CURVE_BUILDOUT_VALIDATION_CURRENT.json"),\n')
    if before.count(needle)!=1 or b'OVERNIGHT_CURVE_BUILDOUT_CURRENT.md' in before:
        raise ValueError('unexpected_mapping_before_state')
    after=before.replace(needle,needle+additions,1);new=ast.parse(after)
    old_node=next(n for n in old.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='CANONICAL_PROJECT_RECORDS' for t in n.targets))
    new_node=next(n for n in new.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='CANONICAL_PROJECT_RECORDS' for t in n.targets))
    if len(new_node.value.elts)!=len(old_node.value.elts)+2:raise ValueError('mapping_count_mismatch')
    if [ast.dump(n) for n in new_node.value.elts[2:]]!=[ast.dump(n) for n in old_node.value.elts]:
        raise ValueError('existing_mapping_changed')
    new_node.value=old_node.value
    if ast.dump(new)!=ast.dump(old):raise ValueError('unrelated_source_change')
    compile(after,str(SOURCE),'exec',dont_inherit=True)
    out=BASE/'canonical_mapping_preparation_v1';out.mkdir(exist_ok=False)
    with (out/'forex_model_vault_sync_before.py.txt').open('xb') as h:h.write(before)
    if SOURCE.read_bytes()!=before:raise ValueError('concurrent_source_change')
    SOURCE.write_bytes(after)
    if SOURCE.read_bytes()!=after:raise ValueError('mapping_readback_failed')
    value=dict(schema_version='overnight_canonical_mapping_preparation_v1_20260909',actual_epoch=time.time(),
        path=str(SOURCE),before_sha256=sha(before),after_sha256=sha(after),helper_sha256=sha(Path(__file__).read_bytes()),
        existing_mapping_count=len(old_node.value.elts),new_mapping_count=len(old_node.value.elts)+2,
        existing_mappings_and_all_other_ast_unchanged=True,source_compile_passed=True,
        records_published=False,vault_writes=False,runtime_actions=False,
        status='two_aliases_prepared_final_documents_and_publication_pending')
    target=out/'CANONICAL_MAPPING_PREPARATION_20260909.json'
    raw=(json.dumps(value,indent=2,sort_keys=True)+'\n').encode()
    with target.open('xb') as h:h.write(raw)
    print(json.dumps(dict(path=str(target),sha256=sha(raw),source_sha256=sha(after),mappings=value['new_mapping_count'])))


if __name__=='__main__':main()
