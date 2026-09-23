"""One bounded read-only GBP/USD original-chain join, no GET or forecast writes."""
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2] / 'trad'
sys.path.insert(0, str(ROOT))
import oanda_curve_risk_attachment_v1 as attachment
import oanda_forecast_curve_file_store_v1 as store
import oanda_m1_risk_distribution_contract_v1 as risk_contract

REGISTRIES = {
    'pilot': ('recovered_second_curve_pilot_v1_20260909.json',
              'ae64f2cae6df44dad81b46deb16e95e4d5f67dabb7fc1e65289b96ba6b0fe2b2'),
    'risk': ('m1_risk_distributions_v1_20260909.json',
             '855c3bb96105c0cf0943ebbb5d28e6d10b6b9bf718573883f897161d08453674'),
}
FILES = []
TOTAL = 0


def need(ok, reason):
    if not ok:
        raise ValueError(reason)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def read(path):
    global TOTAL
    start = time.time()
    raw = store._read(path)
    done = time.time()
    TOTAL += len(raw)
    need(TOTAL <= 16*1024*1024 and len(FILES) < 160, 'diagnostic_total_bound')
    record = dict(path=str(path), sha256=sha(raw), bytes=len(raw),
                  read_started_epoch=start, read_completed_epoch=done)
    FILES.append(record)
    return raw, record


def document(path):
    raw, receipt = read(path)
    return risk_contract.decode(raw), receipt


def registries():
    result = {}
    for label, (name, expected) in REGISTRIES.items():
        value, record = document(ROOT/'config'/name)
        need(record['sha256'] == expected, 'diagnostic_registry_changed')
        for source, digest in value['source_bindings'].items():
            need(re.fullmatch(r'oanda_[a-z0-9_]+\.py', source), 'diagnostic_source_name')
            raw, _ = read(ROOT/source)
            need(sha(raw) == digest, 'diagnostic_frozen_source_changed')
        result[label] = value
    return result


def inventory(path, regex):
    store._safe_components(path)
    found = []
    for i, entry in enumerate(path.iterdir()):
        need(i < 600, 'diagnostic_cycle_inventory_bound')
        store._safe_components(entry)
        need(re.fullmatch(regex, entry.name), 'diagnostic_cycle_name')
        found.append(entry)
    return sorted(found, key=lambda p: int(p.name.removeprefix('cycle_')), reverse=True)


def main():
    need(len(sys.argv) == 2, 'new_output_directory_required')
    output = Path(sys.argv[1]).absolute()
    need(output.parent == HERE and re.fullmatch(r'actual_diagnostic_[0-9]{3}', output.name)
         and not output.exists(), 'diagnostic_fresh_external_output')
    own = sha(Path(__file__).read_bytes())
    module = sha(Path(attachment.__file__).read_bytes())
    start = time.time()
    regs = registries()
    pilot, risk = regs['pilot'], regs['risk']
    pilot_root, risk_root = Path(pilot['output_root']), Path(risk['output_root'])
    need(ROOT in pilot_root.parents and ROOT in risk_root.parents, 'diagnostic_runtime_roots')
    # Predeclared latest complete GBP/USD official-M issue; selection ignores
    # predictions/returns. Post-cutoff collection-only cycles remain excluded.
    cycles = inventory(pilot_root/'cycles', r'cycle_[0-9]{19}')
    eligible = [p for p in cycles if int(p.name[6:])/1e9 < pilot['issue_cutoff_epoch']]
    observation = None
    for directory in eligible[:12]:
        path = directory/'gbp_usd'/'official_midpoint'/'issued_observation.json'
        if path.is_file():
            observation, _ = document(path)
            observation_path = path
            break
    need(observation is not None and observation['status'] == 'issued_and_consumed', 'diagnostic_no_pilot_chain')
    curve_sha = observation['curve_sha256']
    need(re.fullmatch('[0-9a-f]{64}', curve_sha), 'diagnostic_curve_identity')
    selected_risk = None
    for directory in inventory(risk_root/'cycles', r'[0-9]{10}'):
        epoch = int(directory.name)
        if epoch < risk['first_reference_epoch'] or epoch > min(time.time(), risk['last_reference_epoch']):
            continue
        if (epoch-risk['first_reference_epoch']) % 300:
            continue
        path = directory/'gbp_usd'
        if (path/'consumption.json').is_file() and (path/'pair_completed.json').is_file():
            stage, _ = document(path/'pair_completed.json')
            if stage['status'] == 'issued_published_consumed':
                selected_risk = path
                break
    need(selected_risk is not None, 'diagnostic_no_risk_chain')
    raw, _ = read(selected_risk/'source_raw.json')
    receipt, _ = document(selected_risk/'capture_receipt.json')
    fit_raw, _ = read(Path(risk['fit_artifact_path']))
    objects, reads = [], {}
    paths = (
        pilot_root/'published'/'curves'/curve_sha/'curve.json',
        pilot_root/'published'/'curves'/curve_sha/'publication.json',
        pilot_root/'published'/'consumptions'/(observation['consumption']['consumption_sha256']+'.json'),
        selected_risk/'issued.json', selected_risk/'publication.json', selected_risk/'consumption.json')
    for name, path in zip(attachment.OBJECT_KEYS, paths):
        value, evidence = document(path)
        objects.append(value)
        reads[name] = dict(canonical_sha256=risk_contract.digest(value),
            read_started_epoch=evidence['read_started_epoch'], read_completed_epoch=evidence['read_completed_epoch'])
    prepared = objects[0]['prepared_curve']
    need(prepared['forecast_cohort'] == pilot['study_id']+'/GBP_USD/official_midpoint', 'diagnostic_curve_cohort')
    target = prepared['reference_epoch'] + 3600
    decision = time.time()
    result = attachment.attach_risk_for_target(*objects, risk_input_raw=raw, risk_input_receipt=receipt,
        metadata=risk['metadata']['GBP_USD'], fit_raw=fit_raw,
        expected_curve_sources=pilot['source_bindings'], expected_risk_sources=risk['source_bindings'],
        expected_curve_identity=dict(forecast_cohort=pilot['study_id']+'/GBP_USD/official_midpoint',
            model_sha256=pilot['model_sha256'], policy_sha256=pilot['curve_policy']['policy_sha256']),
        attachment_read_receipt=dict(schema_version=attachment.READ_SCHEMA, objects=reads),
        decision_epoch=decision, target_epoch=target, side_context=dict(role='distribution_context', side=0))
    need(registries() == regs and sha(Path(__file__).read_bytes()) == own
         and sha(Path(attachment.__file__).read_bytes()) == module, 'diagnostic_source_bracket')
    report = dict(schema_version='retained_curve_risk_attachment_diagnostic_v1_20260909',
        started_epoch=start, completed_epoch=time.time(), source_bindings_unchanged=True,
        helper_sha256=own, attachment_module_sha256=module,
        selected_pilot_observation=str(observation_path), selected_risk_directory=str(selected_risk),
        selection='latest_complete_GBP_USD_official_M_chain_per_registered_issue_schedule_without_outcome_selection',
        result=result, source_files=FILES, total_read_bytes=TOTAL,
        limits=['One same-instrument sample, not complete pair coverage or a performance evaluation.',
                'Both full original receipt contracts replayed; curve model-input inference is not recomputed here.',
                'The original S5 pilot issue window has closed; later collection is not a newly issued curve.',
                'New actual downstream reads never replace original issue/source availability or make expired H1 current.'])
    output.mkdir()
    target_file = output/'RETAINED_CURVE_RISK_ATTACHMENT_DIAGNOSTIC_20260909.json'
    with target_file.open('xb') as handle:
        data = risk_contract.canonical(report)
        handle.write(data); handle.flush(); os.fsync(handle.fileno())
    print(json.dumps(dict(path=str(target_file), sha256=sha(data), status=result['status'],
                         reason_codes=result['reason_codes'], bytes=len(data))))


if __name__ == '__main__':
    main()
