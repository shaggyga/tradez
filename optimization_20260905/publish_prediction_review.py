"""Preserve the saved-data assessment in canonical source and independently check it."""
from pathlib import Path
import hashlib
import json

OUT = Path(__file__).resolve().parent
ROOT = OUT.parent / 'trad'
source = OUT / 'predictions'
receipt_path = source / 'PREDICTION_QUALITY_RECEIPT.json'
receipt = json.loads(receipt_path.read_text(encoding='utf-8'))
for item in receipt['sources'] + receipt['code_sources']:
    assert hashlib.sha256(Path(item['path']).read_bytes()).hexdigest() == item['sha256']

top = json.loads((ROOT / 'data/oanda_training_manager/state/top_signal_position_ledger_v1.json').read_text(encoding='utf-8-sig'))
rows = top['by_horizon']
assert sum(row['n'] for row in rows) == 8414
assert sum(row['direction_hits'] for row in rows) == 4156
assert sum(row['wins'] for row in rows) == 716
overall = receipt['current_top_signal_shadow']['overall']
assert overall['direction_accuracy'] == 4156 / 8414
assert overall['after_cost_win_rate'] == 716 / 8414

# Preserve the original assessment bytes and original calculation script.
(ROOT / 'FOREX_PREDICTION_QUALITY_20260906.json').write_bytes(receipt_path.read_bytes())
script = ROOT / 'docs/validation/assess_saved_predictions_20260906.py'
script.parent.mkdir(parents=True, exist_ok=True)
script.write_bytes((source / 'assess_saved_predictions.py').read_bytes())
report = (source / 'PREDICTION_QUALITY.md').read_text(encoding='utf-8')
report = report.replace(
    'in `PREDICTION_QUALITY_RECEIPT.json` and `assess_saved_predictions.py`.',
    'in [the canonical receipt](../FOREX_PREDICTION_QUALITY_20260906.json) and '
    '[the preserved calculation script](validation/assess_saved_predictions_20260906.py). '
    'The script records the original local project path and writes its receipt beside itself; '
    'for reproduction, copy it to a disposable review directory before running with Python -B. '
    'The original input snapshots remain local and are not all included in the source-only vault export.'
)
report = report.replace(
    '**time-uniform excess over the configured economic threshold**',
    '**time-uniform bounds for clipped excess (net pips minus the configured economic threshold, clipped to ±60)**'
)
(ROOT / 'docs/FOREX_PREDICTION_QUALITY_20260906.md').write_text(report, encoding='utf-8')
print(json.dumps({'verified_saved_sources': len(receipt['sources']), 'verified_code_sources': len(receipt['code_sources']), 'headline_counts_verified': True, 'prediction_receipt_sha256': hashlib.sha256(receipt_path.read_bytes()).hexdigest()}))
