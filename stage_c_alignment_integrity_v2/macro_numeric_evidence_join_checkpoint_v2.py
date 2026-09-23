"""Portable checkpoint for the numeric-evidence join successor."""
import argparse
import hashlib
import json
import os
import subprocess
import sys
import zipfile
from pathlib import Path

ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import macro_numeric_evidence_join_operator_v2 as op
from portable_checkpoint_v2 import inspect_package,_plain_destination,safe_member,MANIFEST

SCHEMA="forex_macro_numeric_evidence_join_checkpoint.v2"
ALLOWLIST=tuple(sorted(set(op.SOURCES)|{"macro_numeric_evidence_join_checkpoint_v2.py","portable_checkpoint_v2.py","test_macro_numeric_evidence_join_v2.py"}))

def inventory(root): return {row["path"]:row["sha256"] for row in op.read(root/"COMPLETION_MANIFEST.json")["payloads"]}

def invoke(source,inputs,runs,action):
    recipe=source/"NUMERIC_EVIDENCE_JOIN_OPERATOR_RECIPE.json"
    result=subprocess.run([sys.executable,"-I","-B",str(source/"macro_numeric_evidence_join_operator_v2.py"),action,"--recipe",str(recipe),"--recipe-sha256",op.sha(recipe),"--inputs",str(inputs),"--runs-dir",str(runs)],capture_output=True,text=True,timeout=180)
    if result.returncode: raise ValueError("numeric_join_replay_failed:"+result.stdout+result.stderr)
    receipt=json.loads(result.stdout)
    if receipt["status"]!="completed_verified":raise ValueError("numeric_join_replay_incomplete")
    return receipt

def inspect(package,digest):
    data=inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    expected={"source/"+name for name in ALLOWLIST}|{"source/NUMERIC_EVIDENCE_JOIN_OPERATOR_RECIPE.json"}|{"inputs/"+name for name in json.loads(data["source/NUMERIC_EVIDENCE_JOIN_OPERATOR_RECIPE.json"])["inputs"]}|{MANIFEST,"EXPECTED_REPLAY.json"}
    if set(data)!=expected:raise ValueError("exact_numeric_join_checkpoint_inventory_required")
    return data

def export(package,inputs,runs):
    receipt=invoke(ROOT,inputs,runs,"verify")
    values={"source/"+name:(ROOT/name).read_bytes() for name in ALLOWLIST}
    values.update({"inputs/"+name:(inputs/name).read_bytes() for name in op.input_names(inputs)})
    values["source/NUMERIC_EVIDENCE_JOIN_OPERATOR_RECIPE.json"]=(ROOT/"NUMERIC_EVIDENCE_JOIN_OPERATOR_RECIPE.json").read_bytes()
    values["EXPECTED_REPLAY.json"]=op.encoded(inventory(Path(receipt["run_path"])))
    manifest={"schema_version":SCHEMA,"source_allowlist":list(ALLOWLIST),"members":[{"path":name,"bytes":len(value),"sha256":hashlib.sha256(value).hexdigest()} for name,value in sorted(values.items())]}
    with zipfile.ZipFile(package,"x",compression=zipfile.ZIP_DEFLATED) as archive:
        for name,value in sorted(values.items()):archive.writestr(name,value)
        archive.writestr(MANIFEST,op.encoded(manifest))
    digest=op.sha(package);inspect(package,digest);return {"status":"VERIFIED_EXPORT","sha256":digest,"members":len(values)+1}

def restore(package,digest,destination,run_tests=False):
    data=inspect(package,digest);_plain_destination(destination);destination.mkdir(parents=True,exist_ok=True)
    for name,value in data.items():
        path=destination.joinpath(*safe_member(name).parts);path.parent.mkdir(parents=True,exist_ok=True)
        with path.open("xb") as handle:handle.write(value)
    receipt=invoke(destination/"source",destination/"inputs",destination/"runs","run")
    actual=inventory(Path(receipt["run_path"]));
    if actual!=json.loads(data["EXPECTED_REPLAY.json"]):raise ValueError("numeric_join_relocated_payload_mismatch")
    if run_tests:
        env=dict(os.environ)
        env.pop("FOREX_NUMERIC_JOIN_EVIDENCE", None)
        result=subprocess.run([sys.executable,"-I","-B","-m","pytest","-q","-p","no:cacheprovider","--noconftest",str(destination/"source"/"test_macro_numeric_evidence_join_v2.py")],capture_output=True,text=True,env=env,timeout=240)
        (destination/"tests.log").write_text(result.stdout+result.stderr,encoding="utf-8")
        if result.returncode:raise ValueError("numeric_join_relocated_tests_failed:"+result.stdout+result.stderr)
    result={"status":"VERIFIED","checkpoint_sha256":digest,"payloads_identical":len(actual),"relocated_tests_passed":run_tests,"operator":receipt,"independent_review":False}
    (destination/"RESTORE_RECEIPT.json").write_bytes(op.encoded(result));return result

if __name__=="__main__":
    parser=argparse.ArgumentParser();sub=parser.add_subparsers(dest="action",required=True)
    export_parser=sub.add_parser("export");[export_parser.add_argument("--"+name,type=Path,required=True) for name in ("package","inputs","runs-dir")]
    restore_parser=sub.add_parser("restore");[restore_parser.add_argument("--"+name,type=Path,required=True) for name in ("package","destination")];restore_parser.add_argument("--sha256",required=True);restore_parser.add_argument("--run-tests",action="store_true")
    args=parser.parse_args();print(json.dumps(export(args.package,args.inputs,args.runs_dir) if args.action=="export" else restore(args.package,args.sha256,args.destination,args.run_tests)))
