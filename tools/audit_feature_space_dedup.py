"""Read-only audit of the archived unified feature space.

The source parquet is never modified.  A manifest is written separately so a
future fit can explicitly select one representative from each exact duplicate
group and exclude constants/near duplicates.
"""
from __future__ import annotations
import hashlib, json, os, sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

SOURCE = Path(r"C:\Users\zmoor\AppData\Local\ForexResearchData\unified_intrahour_v1\unified_training_matrix.parquet")
REGISTRY = Path(r"C:\Users\zmoor\Documents\forex\feature_horizon_audit_20260908\retained\unified\unified_feature_registry.csv")
OUT = Path(r"C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager\price_only_phase1_20260914\feature_space_dedup_audit.json")

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda: f.read(1024 * 1024), b''):
            h.update(b)
    return h.hexdigest()

def main() -> int:
    if not SOURCE.exists():
        raise SystemExit(f"missing source: {SOURCE}")
    pf = pq.ParquetFile(SOURCE)
    names = list(pf.schema_arrow.names)
    numeric = [n for n in names if n not in {'instrument','base_currency','quote_currency','timestamp','time'}
               and (str(pf.schema_arrow.field(n).type).startswith(('double','float','int','decimal')))]
    # fingerprints are calculated in bounded column batches to avoid creating a
    # second full matrix in memory.
    fingerprints: dict[str, str] = {}
    constants: list[str] = []
    mins: dict[str, float] = {}
    maxs: dict[str, float] = {}
    for start in range(0, len(numeric), 64):
        cols = numeric[start:start+64]
        df = pf.read(columns=cols).to_pandas()
        for c in cols:
            s = df[c]
            # pandas hashing canonicalizes NaN and is independent of the index.
            fingerprints[c] = hashlib.sha256(pd.util.hash_pandas_object(s, index=False).values.tobytes()).hexdigest()
            nunique = int(s.nunique(dropna=False))
            if nunique <= 1:
                constants.append(c)
            finite = s[np.isfinite(s)]
            if len(finite):
                mins[c], maxs[c] = float(finite.min()), float(finite.max())
    groups: dict[str, list[str]] = {}
    for c, fp in fingerprints.items():
        groups.setdefault(fp, []).append(c)
    exact = [g for g in groups.values() if len(g) > 1]
    # Registry names are checked separately: this catches aliases before values
    # are materialized and avoids mistaking related horizons for duplicates.
    registry_dupes: dict[str, list[str]] = {}
    if REGISTRY.exists():
        reg = pd.read_csv(REGISTRY)
        name_col = next((c for c in ('feature_name','name','feature') if c in reg.columns), None)
        if name_col:
            for n, count in reg[name_col].value_counts().items():
                if int(count) > 1:
                    registry_dupes[str(n)] = [str(n)] * int(count)
    selected = [c for c in numeric if c not in constants]
    for g in exact:
        selected.append(sorted(g)[0]) if sorted(g)[0] not in selected else None
        for c in g[1:]:
            if c in selected: selected.remove(c)
    result = {
        'schema_version': 'feature_space_dedup_audit_v1',
        'generated_utc': datetime.now(timezone.utc).isoformat(),
        'source': {'path': str(SOURCE), 'sha256': sha256_file(SOURCE), 'rows': pf.metadata.num_rows, 'columns': len(names), 'numeric_columns_audited': len(numeric)},
        'exact_duplicate_value_groups': [sorted(g) for g in sorted(exact, key=lambda x: (x[0], len(x)))],
        'exact_duplicate_column_count': sum(len(g)-1 for g in exact),
        'constant_columns': sorted(constants),
        'registry_duplicate_names': registry_dupes,
        'representative_numeric_columns': selected,
        'dropped_for_research_only': sorted(set(constants) | {c for g in exact for c in g[1:]}),
        'policy': 'Do not overwrite source. Future fitting may use representative_numeric_columns after independent time-blocked validation; exact equality is not evidence of predictive value.'
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(json.dumps({k: result[k] for k in ('source','exact_duplicate_value_groups','exact_duplicate_column_count','constant_columns','registry_duplicate_names')}, indent=2))
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
