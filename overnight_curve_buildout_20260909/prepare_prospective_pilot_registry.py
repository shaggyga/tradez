"""Create a new immutable registry after the source/test review; does not start anything."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time

ROOT = Path(r'C:\Users\zmoor\Documents\forex\trad')
sys.path.insert(0,str(ROOT))
import oanda_recovered_curve_pilot_v1 as pilot
import oanda_forecast_curve_contract_v1 as contract
import oanda_recovered_second_curve_v1 as recovered


def registry_value(now):
    return dict(schema_version=pilot.SCHEMA,study_id=pilot.STUDY_ID,created_epoch=now,
        issue_cutoff_epoch=datetime(2026,9,9,8,55,tzinfo=timezone.utc).timestamp(),
        collection_stop_epoch=datetime(2026,9,9,13,tzinfo=timezone.utc).timestamp(),
        source_bindings=pilot.source_bindings(),instruments=list(pilot.INSTRUMENTS),
        price_conventions=list(pilot.CONVENTIONS),sampling_policy='retained_fit_window',cycle_sec=60,
        request_start_guard_sec=60,
        target_window_policy='nominal_management_boundary',
        output_root=str(ROOT/'data/oanda_training_manager'/pilot.STUDY_ID),
        model_path=r'D:\ForexRecovery\revamp_20260908T1353Z\artifacts\legacy_model_components\state\second_ridge_models_v1.json',
        model_sha256=recovered.ARTIFACT_SHA256,
        metadata={instrument:dict(instrument=instrument,base_currency=instrument[:3],quote_currency=instrument[4:],
            pip_size='0.01' if instrument=='USD_JPY' else '0.0001') for instrument in pilot.INSTRUMENTS},
        curve_policy=contract.make_policy(native_horizons_sec=list(recovered.HORIZONS),
            maximum_reference_age_sec=30,maximum_build_sec=10,maximum_issue_delay_sec=5,
            maximum_publication_delay_sec=5,maximum_decision_age_sec=14500,minimum_remaining_sec=1),
        orders_enabled=False,manager_activation=False,
        protocol=dict(original_model_fit_reused=True,new_model_fitting=False,original_study_modified=False,
            primary_forecast_replaced=False,smoothing=False,fabricated_bars=False,
            source_availability='actual_response_and_file_read_completion',
            input_sampling='13_real_rows_with_retained_original_55_to_70_second_endpoint_span',
            training_target='next_real_S5_bar_at_or_after_nominal_within_7_seconds',
            target_views=['exact_nominal_diagnostic','original_training_selection_window'],
            midpoint_variants='official_M_and_BA_average_separate_historical_ingestion_unknown',
            decision_observations='remaining_move_at_independently_observed_current_stream_quote',
            manager_action_scope='no_positions_or_manager_commands_created',
            outcome_independence='overlapping_horizons_pairs_and_variants_not_independent_trials',
            evaluation_selection='retain_every_attempt_and_every_horizon_including_withheld_and_missing'),
        **contract.AUTHORITY)


def main():
    registry = registry_value(time.time())
    raw = contract.canonical_bytes(registry)
    path = ROOT/'config'/f'{pilot.STUDY_ID}.json'
    with path.open('xb') as handle:
        handle.write(raw)
        handle.flush()
        import os
        os.fsync(handle.fileno())
    checksum = hashlib.sha256(raw).hexdigest()
    pilot.load_registry(path,checksum)
    print(json.dumps(dict(path=str(path),sha256=checksum,source_count=len(registry['source_bindings']),
        issue_cutoff_epoch=registry['issue_cutoff_epoch'],collection_stop_epoch=registry['collection_stop_epoch'])))


if __name__ == '__main__': main()
