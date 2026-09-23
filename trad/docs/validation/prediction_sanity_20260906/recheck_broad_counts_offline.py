"""Recompute direction and spread-positive counts from frozen text; no database."""
from pathlib import Path
from decimal import Decimal
import hashlib
import json
import sys

source=Path(sys.argv[1]) if len(sys.argv)>1 else Path(__file__).resolve().parent/'broad_signal/selected_rows.jsonl'
raw=source.read_bytes()
assert hashlib.sha256(raw).hexdigest()=='3a1f0dec20cc30b140afe5dd102b7ea96051b3a3ad5cb0d0d9ca7c2cc91ee851'
rows=[json.loads(line,parse_float=Decimal) for line in raw.decode('utf-8').splitlines()]
n=hits=flats=wins=up=down=0
spurious=[]
for r in rows:
    assert r['measurement_version']=='raw_all_signal_consensus_v5_strict_horizon_lineage'
    assert r['status']=='matured' and r['maturity_valid']==1
    side=Decimal(1 if r['direction']=='buy' else -1)
    assert r['direction'] in ('buy','sell')
    eb,ea,xb,xa=[Decimal(str(r[k])) for k in ('entry_bid','entry_ask','exit_bid','exit_ask')]
    actual=(xb+xa-eb-ea)/2
    net=xb-ea if side>0 else eb-xa
    n+=1;hits+=side*actual>0;flats+=actual==0;wins+=net>0;up+=actual>0;down+=actual<0
    if Decimal(str(r['gross_pips']))>0 and side*actual<=0:spurious.append(r['id'])
assert (n,hits,flats,wins)==(8414,4148,296,716),(n,hits,flats,wins)
print(json.dumps({'source_sha256':hashlib.sha256(raw).hexdigest(),'n':n,'decimal_direction_hits':hits,
    'decimal_direction_accuracy':hits/n,'decimal_flat_count':flats,
    'decimal_nonflat_direction_accuracy':hits/(n-flats),'after_spread_wins':wins,
    'after_spread_win_rate':wins/n,'up':up,'down':down,
    'fair_random_direction_expected_hit_rate_flats_miss':.5*(n-flats)/n,
    'stored_positive_direction_rows_not_positive_by_exact_quote_decimal':spurious,
    'proof_eligible':False,'production_database_connections':0},indent=2))
