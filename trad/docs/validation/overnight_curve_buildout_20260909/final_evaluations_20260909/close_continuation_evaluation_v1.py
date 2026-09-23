"""Document accepted evaluator/presenter outputs; no scoring or broker/runtime access."""
from pathlib import Path
from datetime import datetime, timezone
from collections import Counter
import argparse
import hashlib
import json
import math
import os
import time

BASE = Path(__file__).resolve().parent.parent
HERE = Path(__file__).resolve().parent
KINDS = {
    'pilot': {
        'evaluator': 'prospective_pilot/scalability_v3/evaluate_prospective_pilot_v3.py',
        'report': 'prospective_pilot/scalability_v3/actual_evaluation_003/PROSPECTIVE_PILOT_EVALUATION.json',
        'compact': 'prospective_pilot/complete_results_final_003/COMPLETE_PILOT_HORIZON_RESULTS_20260909.json',
        'presenter': 'prospective_pilot/summarize_retained_pilot_v1.py',
        'source_sha': 'bdab15904cfe82c5a7db12a66055fefe186f333fb0d50c4fff63e13bff04fee3',
    },
    'risk': {
        'evaluator': 'richer_inputs/prospective_risk_v1/evaluate_retained_risk_distributions_v1.py',
        'report': 'richer_inputs/prospective_risk_v1/actual_evaluation_005/RETAINED_RISK_DISTRIBUTION_EVALUATION.json',
        'compact': 'richer_inputs/prospective_risk_v1/complete_results_final_005/COMPLETE_RETAINED_RISK_RESULTS_20260909.json',
        'presenter': 'richer_inputs/prospective_risk_v1/summarize_retained_risk_v1.py',
        'source_sha': '8353d83d38ade86c0376b5aa57b95c17df25f37c3d7e1256c8b9b77ebfee6448',
    },
}


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def read(path):
    path = Path(path).resolve()
    if BASE not in path.parents or path.stat().st_size > 64 * 1024 * 1024:
        raise ValueError('bounded_base_document_required')
    raw = path.read_bytes()
    return json.loads(raw), dict(path=str(path), sha256=sha(raw), bytes=len(raw))


def file_ref(path):
    raw = Path(path).read_bytes()
    return dict(path=str(Path(path).resolve()), sha256=sha(raw), bytes=len(raw))


def write_new(path, value):
    raw = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + '\n').encode()
    with Path(path).open('xb') as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    if Path(path).read_bytes() != raw:
        raise ValueError('output_readback')
    return file_ref(path)


def utc(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat()


def epoch(value):
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        raise ValueError('finite_positive_epoch_required')
    return float(value)


def range_of(values):
    values = [epoch(value) for value in values]
    return dict(minimum_epoch=min(values), maximum_epoch=max(values),
                minimum_utc=utc(min(values)), maximum_utc=utc(max(values))) if values else None


def check_prepared_pins():
    preparation, ref = read(HERE / 'FINAL_EVALUATION_PREPARATION_20260909.json')
    for expected in preparation['tools']:
        if file_ref(expected['path']) != expected:
            raise ValueError('prepared_tool_changed')
    for registry in preparation['registries']:
        if file_ref(registry['registry']['path']) != registry['registry']:
            raise ValueError('prepared_registry_changed')
        for expected in registry['source_bindings']:
            if file_ref(expected['path']) != expected:
                raise ValueError('registered_source_changed')
    return preparation, ref


def close(kind):
    own = file_ref(__file__)
    preparation, preparation_ref = check_prepared_pins()
    settings = KINDS[kind]
    report, original_ref = read(BASE / settings['report'])
    compact, compact_ref = read(BASE / settings['compact'])
    if report['issues'] or compact.get('independent_sample_size') is not None:
        raise ValueError('successful_unpooled_original_evidence_required')
    if report['research_only'] is not True or report['can_place_orders'] is not False:
        raise ValueError('research_authority')
    linked = compact['source_report'] if kind == 'pilot' else compact['original_report']
    if linked['sha256'] != original_ref['sha256'] or linked['bytes'] != original_ref['bytes']:
        raise ValueError('compact_original_binding')
    if kind == 'pilot':
        if report['status'] != 'completed' or report['evaluator_source_sha256'] != settings['source_sha']:
            raise ValueError('accepted_pilot_source_or_status')
        if len(compact['complete_original_groups']) != 156 or report['source_closure_unchanged'] is not True:
            raise ValueError('pilot_all_cells_or_closure')
        sources, sources_ref = read((BASE / settings['report']).parent / 'source_captures.json')
        manifest, manifest_ref = read((BASE / settings['report']).parent / 'manifest.json')
        evidence = {row['path']: row for row in report['evidence_files']}
        for name, reference in [('source_captures.json', sources_ref), ('manifest.json', manifest_ref)]:
            if any(evidence[name][k] != reference[k] for k in ('sha256', 'bytes')):
                raise ValueError('original_auxiliary_file_binding')
        start, end = report['started_epoch'], report['completed_epoch']
        metrics = dict(verified_chain_status_counts=report['curve_chain_status_counts'],
            source_capture_status_counts=report['source_capture_status_counts'],
            view_dispositions=report['view_dispositions'], all_cell_count=156,
            cell_descriptions=compact['cell_descriptions'],
            cycle_directories_observed=report['cycle_directories_observed'],
            h4_original_groups=[g for g in compact['complete_original_groups'] if g['horizon_sec'] == 14400])
        clocks = dict(original_source_read_completed_range=range_of(
            r['original_read_completed_epoch'] for r in sources if r['status'] == 'verified'),
            immutable_file_observation_range=range_of(r['observed_epoch'] for r in manifest),
            source_cutoff_semantics='The accepted evaluator enumerates cycle directories once. The maximum original source receipt time bounds the retained source inventory; later curve score clocks and audit completion do not extend it. Individual immutable files are observed separately, not atomically.')
    else:
        if report['status'] != 'passed' or report['helper_sha256'] != settings['source_sha']:
            raise ValueError('accepted_risk_source_or_status')
        if len(compact['all_216_original_groups']) != 216 or len(compact['all_108_original_paired_method_cells']) != 108:
            raise ValueError('risk_all_cells')
        start, end = report['verification_started_epoch'], report['verification_completed_epoch']
        if start < 1788957900:
            raise ValueError('risk_started_before_registered_collection_stop')
        metrics = dict(verified_chain_count=report['verified_chain_count'],
            source_capture_count=len(report['sources']), all_group_count=216, all_paired_cell_count=108,
            scheduled_issue_status_counts=compact['scheduled_issue_status_counts'],
            node_dispositions_by_horizon=compact['node_dispositions_by_horizon'],
            issue_attempt_status_counts=compact['issue_attempt_status_counts'])
        clocks = dict(original_source_read_completed_range=range_of(
            r['source_read_completed_epoch'] for r in report['sources']),
            immutable_file_read_start_range=range_of(r['audit_read_started_epoch'] for r in report['verified_files']),
            immutable_file_read_completion_range=range_of(r['audit_read_completed_epoch'] for r in report['verified_files']),
            source_cutoff_semantics='Registered source collection stops at 12:45 UTC; this later bounded non-atomic retained-file audit preserves original source clocks, partial stages and missing scheduled slots. No later maturity or absent price path is filled.')
    if read(BASE / settings['report'])[1] != original_ref or read(BASE / settings['compact'])[1] != compact_ref:
        raise ValueError('report_changed_during_documentation')
    check_prepared_pins()
    if file_ref(__file__) != own:
        raise ValueError('documentation_helper_changed')
    start, end = epoch(start), epoch(end)
    documented = epoch(time.time())
    if not start <= end <= documented:
        raise ValueError('evaluation_or_documentation_clock_order')
    registry = preparation['registries'][0 if kind == 'pilot' else 1]['registry']
    invocations = dict(evaluator=[str(BASE / settings['evaluator']), '--registry', registry['path'],
        '--registry-sha256' if kind == 'pilot' else '--expected-sha256', registry['sha256'],
        '--output-directory', str((BASE / settings['report']).parent)],
        presenter=[str(BASE / settings['presenter']), '--report', original_ref['path'],
        '--expected-sha256', original_ref['sha256'], '--output-directory', str((BASE / settings['compact']).parent)])
    value = dict(schema_version='continuation_retained_evaluation_acceptance_v1_20260909',
        kind=kind, status='accepted_existing_evaluator_and_complete_presentation', documented_epoch=documented,
        verification_started_epoch=start, verification_completed_epoch=end,
        verification_started_utc=utc(start), verification_completed_utc=utc(end),
        private_original_report=original_ref, sanitized_complete_presentation=compact_ref,
        presenter=file_ref(BASE / settings['presenter']), documentation_helper=own,
        owner_tool_invocation_arguments=invocations, invocation_scope='Owner tool calls execute the unchanged scripts with Python -B; this documentary helper does not launch them.',
        prepared_source_and_registry_pins=preparation_ref,
        resumption_receipt=file_ref(HERE / 'FINAL_EVALUATION_RESUMPTION_20260909.json'),
        source_closures_unchanged=True, evidence_issues=0, metrics=metrics, cutoff_clocks=clocks,
        independent_sample_size=None, rescoring_in_this_helper=False, runtime_writes=False,
        broker_requests=False, model_fits=False, can_place_orders=False, can_promote=False, research_only=True,
        limits=['Every original cell is retained; descriptors are not pooled independent-trial estimates.',
                'This wrapper checks accepted presenter source/linkage and complete-cell counts; it does not independently rerun presenter arithmetic.',
                'Original raw captures, per-origin reports and file inventories remain private local dependencies.',
                'Prior canceled-plan and early-close records remain unchanged; resumed work is separately dated.',
                'These evaluation/presentation operations do not change registered sources, registries, runtime, forecasting policy, models, accounts or workers.'])
    result = write_new(HERE / (kind.upper() + '_CONTINUATION_EVALUATION_ACCEPTANCE_20260909.json'), value)
    print(json.dumps(dict(receipt=result, start=utc(start), end=utc(end), metrics=metrics)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--kind', choices=KINDS, required=True)
    close(parser.parse_args().kind)
