"""Render the completed projection-policy attribution without choosing a forecast winner."""
import argparse,json
from pathlib import Path

def render(report):
    rows=report['rows']; lines=['# Currency projection policy results','','All six forecast variants, both cohorts, cost scenarios and policy arms are retained. Values are dependent offline candle scenarios and are not a winner selection, profitability claim or trading authorization.','','| Method | Scenario | Arm | Selected | Opened | Unopened | Net USD |','|---|---|---|---:|---:|---:|---:|']
    for row in rows:
        lines.append('| {method} | {scenario} | {arm} | {selected} | {opened} | {selected_without_open} | {net_account_pnl_usd} |'.format(**row))
    lines+=['','Selection and fill are separate. The source JSON retains financing statuses and terminal lot/pending state per row. No cross-cohort aggregation is reported because the development cohorts overlap.']
    return '\n'.join(lines)+'\n'

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--input',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.write_text(render(json.loads(a.input.read_text())),encoding='utf-8')
