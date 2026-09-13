"""Reuse the exact read-only scanner helper with a fresh receipt directory."""
import argparse
import hashlib
import importlib.util
from pathlib import Path
import sys

BASE=Path(__file__).resolve().parent
SOURCE=BASE/'preflight_source_export.py'
EXPECTED='5fafacaf5bc2c087719bb1d2ec6c4d00fbfc5c29c434c7045899c33bef508534'


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output-directory',type=Path,required=True)
    args=parser.parse_args();output=args.output_directory.absolute()
    if output.parent!=BASE or output.exists() or output.is_symlink():raise ValueError('fresh_direct_output_required')
    raw=SOURCE.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=EXPECTED:raise ValueError('original_scanner_helper_changed')
    sys.dont_write_bytecode=True
    spec=importlib.util.spec_from_file_location('retained_source_export_preflight_v1',SOURCE)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    # The accepted scanner and its original status semantics stay unchanged.
    output.mkdir();module.OUT=output;module.main()
    if SOURCE.read_bytes()!=raw:raise ValueError('original_helper_changed_during_scan')


if __name__=='__main__':main()
