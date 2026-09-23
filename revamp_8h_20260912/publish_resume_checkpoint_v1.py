"""Publish the resumed run's dated status without rewriting earlier checkpoints."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib, json

ROOT = Path(__file__).resolve().parent
VAULT = Path('C:/Users/zmoor/OneDrive/thevault/projects/forex')
sha = lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
now = datetime.now(timezone.utc).isoformat()
out = ROOT / 'publication_checkpoint_005'
out.mkdir(exist_ok=False)
target = VAULT / 'REVAMP_8H_20260912/CHECKPOINT_005'
target.mkdir(exist_ok=False)
package = ROOT / 'runtime/portable_recovery_profile_001'
resume = ROOT / 'RUN_RESUMED_001.json'
freeze = ROOT / 'direction_richer_archive_003/PROTOCOL_FREEZE_003.json'
assert sha(freeze) == '870a22150ab6124f37cc32480ab9577af8e3cdee1cd9214880a2135950167083'
assert (package / 'VAULT_PUBLICATION_RECEIPT_001.json').is_file()
source = VAULT / 'source/current_source_profile_20260912_001'
assert sha(source / 'forex_current_source_profile_001.zip') == 'a7c7301001e6fc61d844914731d0c22a907894576b9ffdaf23371dc8cece7d6d'
text = f'''# Resumed Forex work — September 12, 2026

Status observed at {now}. The user resumed work after checkpoint 004. That earlier closeout and its stopped partial files remain historical evidence.

The same frozen revision 003 is preparing the richer history in a new exclusive `prepared_002` directory. No feature, population, target, cost, model or fold choice changed. Its first two pair outputs match the stopped preparation exactly. No completed richer preparation or fitted result is claimed at this checkpoint. The follow-on plan remains six fixed HGB fits, an independent availability reconciliation, numerical result checks and exact saved-artifact replay.

The [current source profile](../../source/current_source_profile_20260912_001/README.md) now preserves the ten integrated repair groups and original compact/richer source identities. Its ZIP was independently checked against the source manifest, directory verification and a dry relocation. It contains 205 unique source objects with 221 bindings; 89 Python objects pass syntax parsing. It is a portable source/provenance inspection package. Market arrays, fitted model payloads, credentials, private databases, native dependencies and runnable broker/manager recovery remain outside its verified scope. Existing vault archives were not replaced.

Typed signal candidate 004 separates side-profit classifier probabilities and ranking scores from midpoint direction probabilities. Its 120 author tests, 11 root probes and 16 independent successor probes pass; these overlap in purpose and are not operational or predictive validation. It remains staged. Later root review identified unsupported inherited feature metadata and boolean top-level numeric aliases for a separately versioned correction.

The caller audit found another concrete gap: the existing staged passive outcome v3 accepts nullable magnitude but requires numeric probability in normalization, SQL and Brier scoring. It cannot safely consume these typed signals. A narrow successor is being designed using the same outcome recording, original clocks and maturity infrastructure; no existing ledger migration or live registration is underway.

The completed compact/peer archive comparison is unchanged: direction is roughly 49.4–52.5%, 32 of 36 learned cells have negative selected-position net results, two have small positive results and two take no positions. No setup is positive across all three assessment periods. Those are overlapping historical hypothetical decisions after the declared costs, not broker trades or account returns.

No trading, collector, manager or service has been activated during this continuation. The previous finite trial deadline remains unchanged. The earlier clock-monitor startup was rejected by automatic approval review as “blocked by policy”; it has not been retried or bypassed. Offline research can continue independently.

See [the prior closeout](../CHECKPOINT_004/README.md) for the ten repair groups and remaining model, news and position-management gaps. Later checkpoints should supersede this dated progress observation.
'''
with (target / 'README.md').open('x', encoding='utf-8', newline='\n') as f:
    f.write(text)
with (out / 'README.md').open('x', encoding='utf-8', newline='\n') as f:
    f.write(text)
vault_readme = VAULT / 'README.md'
before = vault_readme.read_bytes()
(out / 'VAULT_README_BEFORE.md').write_bytes(before)
old = before.decode('utf-8')
heading, rest = old.split('\n', 1)
notice = ('\n\n**September 12 — run resumed:** [Current continued-work checkpoint]'
          '(REVAMP_8H_20260912/CHECKPOINT_005/README.md). The richer-history preparation has resumed under its unchanged freeze; '
          'the current repair-source recovery package is verified and retained. Typed probability and outcome-consumer work continues offline.\n')
assert sha(vault_readme) == hashlib.sha256(before).hexdigest()
vault_readme.write_text(heading + notice + rest, encoding='utf-8', newline='\n')
for href in ('../../source/current_source_profile_20260912_001/README.md', '../CHECKPOINT_004/README.md'):
    assert (target / href).resolve().is_file()
receipt = {'published_utc': now, 'checkpoint_readme': str(target / 'README.md'),
           'checkpoint_sha256': sha(target / 'README.md'), 'resume_receipt_sha256': sha(resume),
           'richer_freeze_sha256': sha(freeze), 'source_package_publication_sha256': sha(package / 'VAULT_PUBLICATION_RECEIPT_001.json'),
           'vault_readme_before_sha256': hashlib.sha256(before).hexdigest(),
           'vault_readme_after_sha256': sha(vault_readme), 'prior_checkpoints_changed': False,
           'services_or_orders_activated': False}
with (out / 'PUBLICATION_RECEIPT.json').open('x', encoding='utf-8') as f:
    json.dump(receipt, f, indent=2); f.write('\n')
with (ROOT / 'WORK_LOG.md').open('a', encoding='utf-8') as f:
    f.write(f'\n\n### {now} — resumed source recovery and signal/caller checkpoint\n\n'
            'Current ten-repair source profile passed manifest, ZIP and independent directory/relocation verification and was added to the vault. '
            'Its scope is 205 unique source objects / 221 bindings / 89 Python syntax checks, not executable model or manager recovery. '
            'Root verifier v1 failed only because START_HERE.txt is generated inside the ZIP; v2 verifies its exact reviewed sealer literal. '
            'The original failure and package bytes are retained. Typed004 passes 120 author + 11 root + 16 independent test calls; '
            'additional metadata/top-level alias corrections remain separate. Existing passive v3 requires numeric midpoint probability; '
            'a nullable-probability successor is needed and is not yet complete. No richer model result or service activation is claimed. '
            'Vault checkpoint005 records this dated resumed state.\n')
print(json.dumps(receipt))
