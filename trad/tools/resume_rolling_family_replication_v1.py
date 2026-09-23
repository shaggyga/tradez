"""Complete only the pending family grid from an exactly bound failed V2 run.

The completed models, forecasts and diagnostics are copied byte-for-byte into a
fresh output. No original run, package dependency or frozen numerical source is
modified. A scoped sklearn compatibility shim preserves the all-NaN design.
"""
from __future__ import annotations
import argparse
import copy
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import numpy as np
from threadpoolctl import threadpool_limits
from tools import run_rolling_specialists_v2 as frozen

EXPECTED_CONTEXTS = {f'{g}_{h}m' for g in frozen.GROUPS for h in frozen.HORIZONS}
EXPECTED_VARIANTS = {'direct', 'mixture_raw', 'mixture_calibrated', 'direct_ridge', 'direct_context_hgb'}
SECTIONS = ('contexts', 'pair_priors', 'comparators', 'component_priors')


def referenced_files(value):
    result = {}
    def visit(node):
        if isinstance(node, dict):
            if 'path' in node and 'sha256' in node:
                name = node['path']; digest = node['sha256']
                if name in result and result[name] != digest:
                    raise ValueError('conflicting_artifact_reference')
                result[name] = digest
            for child in node.values():
                visit(child)
        elif isinstance(node, list):
            for child in node:
                visit(child)
    visit(value)
    return result


def validate_failed_report(report):
    if (report.get('schema') != frozen.SCHEMA or report.get('status') != 'failed'
            or report.get('failure') != {'type': 'ValueError', 'message': 'window shape cannot be larger than input array shape'}):
        raise ValueError('exact_empty_binning_failure_required')
    expected = {'completed_base_bundles': 20, 'completed_variants': 20,
                'completed_comparator_fits': 8, 'completed_family_refits': 0,
                'completed_family_mean_variants': 2}
    if any(report.get(k) != v for k, v in expected.items()):
        raise ValueError('complete_unchanged_main_grid_and_no_family_fit_required')
    if set(report['contexts']) != EXPECTED_CONTEXTS or len(report['comparators']) != 18:
        raise ValueError('complete_main_contexts_and_comparators_required')
    for ctx in report['contexts'].values():
        if (set(ctx['variants']) != EXPECTED_VARIANTS or len(ctx['oof_models']) != 4
                or set(ctx['meta_models']) != set(frozen.META_ARMS)):
            raise ValueError('complete_main_models_and_variants_required')
    if report.get('can_place_orders') is not False or report.get('models_promoted') != 0:
        raise ValueError('research_only_unpromoted_source_required')


def verify_reuse(source, report):
    source = Path(source).resolve()
    records = referenced_files({k: report[k] for k in SECTIONS})
    actual = set()
    for sub in frozen.SUBDIRS:
        folder = source / sub
        if folder.is_symlink():
            raise ValueError('ordinary_artifact_directory_required')
        for path in folder.iterdir():
            if path.is_symlink() or not path.is_file():
                raise ValueError('ordinary_artifact_file_required')
            actual.add(path.relative_to(source).as_posix())
    if actual != set(records):
        raise ValueError('all_reused_files_must_have_exact_existing_references')
    for name, digest in records.items():
        frozen.checked_path(source, name, digest)
    return records


def copy_reuse(source, output, records):
    if output.exists():
        raise ValueError('fresh_output_required')
    output.mkdir()
    for sub in frozen.SUBDIRS:
        (output / sub).mkdir()
    for name, digest in records.items():
        src = frozen.checked_path(source, name, digest)
        dst = output / name
        if not dst.resolve().is_relative_to(output.resolve()):
            raise ValueError('copy_destination_escape')
        with src.open('rb') as incoming, dst.open('xb') as outgoing:
            shutil.copyfileobj(incoming, outgoing, 4 * 1024**2)
        frozen.checked_path(output, name, digest)
        frozen.storage_check(output)


def run(args):
    from tools.rolling_empty_binning_compat_v1 import empty_binning_compat
    source, output = Path(args.failed_run).resolve(), Path(args.output).resolve()
    if (source == output or not source.is_relative_to(ROOT / 'data')
            or not output.is_relative_to(ROOT / 'data') or output.exists()):
        raise ValueError('separate_new_project_data_output_required')
    manifest = source / 'RESULTS.json'
    if frozen.file_sha(manifest) != args.failed_results_sha256:
        raise ValueError('exact_failed_results_digest_required')
    old = json.loads(manifest.read_bytes())
    validate_failed_report(old)
    if args.threads != old['threads']:
        raise ValueError('unchanged_thread_count_required')
    frozen.assert_pins(old['source_bindings'])
    if shutil.disk_usage(output.parent).free < 36 * 1024**3:
        raise ValueError('reserve_plus_output_budget_required')
    records = verify_reuse(source, old)
    pins = frozen.merge_source_bindings((old['source_bindings'],),
                ('tools/resume_rolling_family_replication_v1.py', 'tools/rolling_empty_binning_compat_v1.py'))
    inputs = Path(old['inputs_root']); comparison = Path(old['previous_comparison_root'])
    im = json.loads((inputs / 'SPECIALIST_INPUTS.json').read_bytes())
    if frozen.file_sha(inputs / 'SPECIALIST_INPUTS.json') != old['inputs_sha256']:
        raise ValueError('unchanged_inputs_required')
    previous = json.loads((comparison / 'RESULTS.json').read_bytes())
    if frozen.file_sha(comparison / 'RESULTS.json') != old['previous_comparison_sha256']:
        raise ValueError('unchanged_comparison_required')
    frozen.validate_replication_inputs(im, previous, args.threads)
    report = copy.deepcopy(old)
    report.pop('failure')
    report.update(status='running', source_bindings=pins, family_contexts={}, completed_family_mean_variants=0,
                  started_utc=datetime.now(timezone.utc).isoformat(),
                  recovery={'schema': 'rolling_family_only_recovery_v1', 'source_root': str(source),
                            'failed_results_sha256': args.failed_results_sha256,
                            'source_started_utc': old['started_utc'],
                            'reused_files': records, 'reused_file_count': len(records),
                            'main_grid_refitted': False, 'main_forecasts_rescored': False,
                            'family_design_changed': False,
                            'implementation_change': 'Scoped empty-effective-column bin-threshold guard; all 53 slots and uniformly NaN family masks retained; installed package files unchanged'})
    began = time.monotonic()
    copy_reuse(source, output, records)
    frozen.save(output / 'RESULTS.json', report)
    try:
        with threadpool_limits(limits=args.threads):
            data, loaded, pm, qm = frozen.load_specialist_inputs(inputs)
            if loaded != im:
                raise ValueError('input_manifest_changed_during_load')
            data['boundaries'] = pm['boundaries']
            assessment = np.flatnonzero(data['split'] != 0)
            if len(assessment) != report['assessment_rows']:
                raise ValueError('same_all_original_assessment_origins_required')
            prior, parameters = frozen.pair_prior(data, 60, assessment)
            if parameters != json.loads((output / report['pair_priors']['60']['path']).read_bytes()):
                raise ValueError('identical_existing_pair_prior_required')
            component_prior = frozen.components.train_baselines(data, 60)
            if component_prior != json.loads((output / report['component_priors']['60']['path']).read_bytes()):
                raise ValueError('identical_existing_component_prior_required')
            with empty_binning_compat() as compatibility:
                frozen.run_family_grid(data, assessment, prior, component_prior, output, report, pins, began)
            report['recovery']['compatibility'] = compatibility
            if compatibility['empty_effective_columns_handled'] <= 0:
                raise ValueError('empty_column_guard_not_exercised')
            frozen.assert_pins(pins)
            if frozen.file_sha(manifest) != args.failed_results_sha256:
                raise ValueError('failed_predecessor_changed')
            if frozen.file_sha(inputs / 'SPECIALIST_INPUTS.json') != old['inputs_sha256'] or frozen.file_sha(comparison / 'RESULTS.json') != old['previous_comparison_sha256']:
                raise ValueError('original_input_or_comparison_changed')
            if frozen.file_sha(Path(old['prior_specialist_results_path'])) != old['prior_specialist_results_sha256']:
                raise ValueError('prior_specialist_reference_changed')
            report['completion_input_recheck'] = frozen.recheck_input_artifacts(inputs, im, pm, qm)
            for name, digest in records.items():
                frozen.checked_path(source, name, digest)
                frozen.checked_path(output, name, digest)
            for section in (*SECTIONS, 'family_contexts'):
                frozen.check_recorded_artifacts(output, report[section])
            if report['completed_family_refits'] != 7 or report['completed_family_mean_variants'] != 16:
                raise ValueError('complete_fixed_family_grid_required')
            report['artifacts'] = {p.relative_to(output).as_posix(): {'sha256': frozen.file_sha(p), 'bytes': p.stat().st_size}
                                   for sub in frozen.SUBDIRS for p in sorted((output / sub).iterdir())}
            report.update(status='complete', completed_utc=datetime.now(timezone.utc).isoformat(),
                          output_bytes=frozen.storage_check(output), elapsed_seconds=round(time.monotonic()-began, 3))
            frozen.save(output / 'RESULTS.json', report)
    except BaseException as exc:
        report.update(status='failed', failure={'type': type(exc).__name__, 'message': str(exc)})
        frozen.save(output / 'RESULTS.json', report)
        raise
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--failed-run', type=Path, required=True)
    parser.add_argument('--failed-results-sha256', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--threads', type=int, default=2)
    result = run(parser.parse_args())
    print(json.dumps({k: result[k] for k in ('status', 'completed_family_refits', 'elapsed_seconds')}))
