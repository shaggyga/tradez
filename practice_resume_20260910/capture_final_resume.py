"""Read-only, credential-redacted confirmation of the resumed practice trial."""
import hashlib
import json
import sys
import time
from pathlib import Path

ROOT = Path(r'C:\Users\zmoor\Documents\forex\trad')
OUT = ROOT.parent / 'practice_resume_20260910'
sys.path.insert(0, str(ROOT))
from oanda_practice_trial_runner_v1 import CONFIG, STATE, TRIAL_ID, validate_config
from oanda_practice_trial_broker_v1 import PracticeTrialBroker

config = validate_config(CONFIG, require_enabled=True)
broker = PracticeTrialBroker.from_credentials(
    ROOT / 'creds', expected_account_sha256=config['account_sha256'], trial_id=TRIAL_ID)
try:
    account = broker.account_summary()
    trades = broker.open_trades()
    orders = broker.pending_orders()
finally:
    broker.close()

receipt = {
    'schema_version': 'practice_resume_final_v1_20260910',
    'completed_epoch': time.time(),
    'scope': 'Independent GET-only broker observation; runtime files are separate timestamped observations.',
    'config_validated_enabled': True,
    'trial_config_sha256': hashlib.sha256(CONFIG.read_bytes()).hexdigest(),
    'account': account,
    'open_trades': trades,
    'pending_orders': orders,
    'runtime': {name: json.loads((STATE / (name + '.json')).read_text(encoding='utf-8-sig'))
                for name in ('status', 'watchdog', 'bootstrap')},
    'windows_task': json.loads((OUT / 'RECOVERY_TASK_FINAL_20260910.json').read_text(encoding='utf-8-sig')),
}
path = OUT / 'RESUME_FINAL_VERIFIED_20260910.json'
if path.exists():
    raise SystemExit('Refusing to replace a retained observation')
path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n', encoding='utf-8')
status = receipt['runtime']['status']
print(json.dumps({
    'receipt': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
    'trial_enabled': status['enabled'], 'worker_state': status['state'],
    'worker_pid': status['pid'], 'worker_age_sec': time.time() - status['observed_epoch'],
    'entry_halt_latched': status['entry_halt_latched'],
    'loss_stop_latched': status['loss_stop_latched'],
    'balance': account['balance'], 'NAV': account['NAV'],
    'open_trades': [{k: row.get(k) for k in ('id', 'instrument', 'currentUnits', 'price', 'stopLossOrder')}
                    for row in trades['rows']],
    'pending_order_count': len(orders['rows']),
    'task_state': receipt['windows_task']['state'],
}, indent=2))
