"""Render retained metrics without selecting winners or creating new scores."""
from decimal import Decimal,localcontext
from datetime import datetime,timezone

def number(value,places=4):
    if value is None:return 'unavailable'
    with localcontext() as ctx:
        ctx.prec=60;return str(Decimal(str(value)).quantize(Decimal(10)**-places))

def render(report):
    t=report['original_technical_report'];m=report['original_matched_report'];remaining=report['original_remaining_report']
    coverage=report['original_forecast_coverage'];utc=lambda t:datetime.fromtimestamp(t,timezone.utc).isoformat()
    lines=['# Matched Forex development campaign','',
        'This aggregate report reveals previously inspected development outcomes. It is not protected confirmation, a model selection result, or observed broker execution. Original-record inspection hides later outcomes by default.','',
        f"All 68 instruments retained. {m['forecast_rows']} forecasts, {m['coverage_rows']} coverage rows, and {m['score_groups']} matched score groups across seven elapsed targets and frozen/scheduled adaptive procedures. The remaining-target cohort has {remaining['native_packets']} prepared native packets. No model is selected.",'',
        f"Forecast origins: {utc(coverage['first_origin_epoch'])} through {utc(coverage['last_origin_epoch'])}, {coverage['origin_count']} scheduled origins. Policy cohort: 2024-07-22 00:01 UTC to the original 2024-07-24 00:01 UTC target.",'',
        '## Original forecast scores','',
        'Overlapping pair/origin observations are not independent trials. Controls share feature-ready support with the learned models. Scores below are copied from the original verified payload.','',
        '| Target | Procedure | Method | Rows | MAE bps | MSE bps² |','|---|---|---|---:|---:|---:|']
    for s in report['original_scores']:
        lines.append('| '+' | '.join([s['target_id'].replace('technical_endpoint_midpoint_elapsed_',''),s['procedure'],s['method'],str(s['rows']),number(s['mae_bps']),number(s['mse_bps2'])])+' |')
    lines+=['','## Original policy results','',
        'Two declared candle execution scenarios; reference/optimized engine parity was verified in the policy checkpoint. Cash and the recovered momentum selector abstain; the latter lacks required score/confidence fields. There is no matched six-trading-arm claim.','',
        '| Model/scenario | Arm | Net USD | Financing USD | Fees USD | Missing decision values |','|---|---|---:|---:|---:|---:|']
    for group in report['policies']:
        r=group['report'];label=group['source']['dependency'].removeprefix('policy/').removesuffix('/reference')
        for arm,state in sorted(r['arms'].items()):
            lines.append('| '+' | '.join([label,arm,number(state['net_account_pnl_usd']),number(state['financing_usd']),number(state['fees_usd'],2),str(r['valuation_coverage'][arm]['decision_values_unavailable'])])+' |')
    lines+=['','## Movement and target limits','',
        'Episode MFE/MAE in the JSON companion are original observed replay-frame marks, not continuous extrema. Missing marks remain unknown. Intrabar barrier order and exact continuous drawdown are unsupported by the current close-only inputs.','',
        '| Elapsed minutes | Strict contiguous labels |','|---:|---:|']
    for h,n in t['contiguous_path_support'].items():lines.append(f'| {h} | {n} |')
    lines+=['',
        'A missing strict-contiguous label does not establish whether a gap was a normal venue closure or missing feed data. Daily-close and two/five-session targets remain blocked pending qualified venue/calendar evidence; elapsed endpoints do not resolve that gate.','',
        '## Readiness','',
        'Engineering readiness: false. Forecast evidence: bounded development. Policy evidence: declared candle scenarios. Demo authorization: not granted. Independent review: unperformed.','',
        'Next: reconcile the populated rolling feature registry and its preserved model lineage before proposing new feature/model experiments. GPT/advisor comparisons, paid calls, broker/service/account actions and D-drive work remain deferred.']
    return '\n'.join(lines)+'\n'
