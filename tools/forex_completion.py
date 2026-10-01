"""Render the reviewed R01-R15 delivery scorecard; never infer credit from files."""
from pathlib import Path
import argparse
import hashlib
import json

STAGES = ('implementation', 'verified_evidence', 'acceptance')
IDS = tuple(f'R{i:02d}' for i in range(1, 16))


def calculate(card, root):
    if card.get('schema') != 'forex.design_completion.v1':
        raise ValueError('unsupported scorecard schema')
    rows = card['requirements']
    if tuple(r['id'] for r in rows) != IDS:
        raise ValueError('exact ordered R01-R15 population required')
    root = Path(root).resolve()
    evidence = card['evidence']
    for key, item in evidence.items():
        relative = Path(item['path'])
        path = (root / relative).resolve()
        if relative.is_absolute() or not path.is_relative_to(root):
            raise ValueError('evidence path outside workspace')
        if hashlib.sha256(path.read_bytes()).hexdigest() != item['sha256']:
            raise ValueError('stale evidence: ' + key)
    earned = active_earned = active_possible = full = 0
    for row in rows:
        stages = row['stages']
        if tuple(stages) != STAGES or type(row['deferred']) is not bool:
            raise ValueError('fixed three-stage rubric required')
        prior = True
        for name in STAGES:
            stage = stages[name]
            if type(stage['passed']) is not bool or not stage['reason'].strip():
                raise ValueError('binary reviewed credit and rationale required')
            if stage['passed']:
                if row['deferred'] or not prior or not stage['evidence']:
                    raise ValueError('unearned/deferred milestone credit')
                if not set(stage['evidence']) <= set(evidence):
                    raise ValueError('unbound evidence')
            prior = stage['passed']
        points = sum(stages[s]['passed'] for s in STAGES)
        earned += points
        full += points == 3
        if not row['deferred']:
            active_possible += 3
            active_earned += points
    return dict(earned=earned, possible=45, percent=round(100*earned/45, 2),
                active_earned=active_earned, active_possible=active_possible,
                active_percent=round(100*active_earned/active_possible, 2) if active_possible else None,
                fully_accepted_requirements=full, total_requirements=15,
                evidence_files_verified=len(evidence),
                scope='Equal-weight delivery milestones, not elapsed effort, profitability, trading readiness or live pair coverage')


def render(card, result):
    lines = ['# Forex design completion scorecard', '',
        f"**{result['percent']:.1f}% overall delivery index ({result['earned']}/45 milestones).**",
        f"Active scope: {result['active_percent']:.1f}% ({result['active_earned']}/{result['active_possible']}); deferred GPT work stays in the overall denominator.",
        f"Fully accepted requirements: {result['fully_accepted_requirements']}/15.", '',
        'This fixed, equal-weight rubric credits a working implementation, verified bounded test/replay evidence, and full requirement acceptance separately. Partial work is not full acceptance. Historical tests are reused evidence, not newly run tests. This is not an estimate of hours remaining, forecast skill or trading readiness.', '',
        'The assessment is a same-task review. Evidence hashes are checked by `python tools/forex_completion.py`; a missing or changed binding invalidates the score until reviewed. Never refresh hashes or award credit merely to increase the percentage. Version changes to scope/rubric explicitly. Pair availability is reported separately.', '',
        '| Requirement | Points | Remaining acceptance work |', '|---|---:|---|']
    for row in card['requirements']:
        points = sum(row['stages'][s]['passed'] for s in STAGES)
        lines.append(f"| {row['id']} {row['title']} | {points}/3 | {row['remaining']} |")
    lines += ['', '## Evidence and update procedure', '',
        'The complete reasons, exact paths and SHA-256 identities are in `artifacts/design_completion.json`. At every checkpoint, review affected rows against new evidence, retain the 45-point denominator, rerun the checker and update this report. A new repair ticket does not add points or change the denominator. Deferred work is not silently counted as finished.', '',
        'Full-design authority: ' + card['design_path'], '',
        '## Reviewed evidence', '']
    for key, item in card['evidence'].items():
        lines.append(f"- {key}: `{item['path']}` — {item['scope']}")
    return '\n'.join(lines) + '\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--card', default='artifacts/design_completion.json')
    parser.add_argument('--write-report', type=Path)
    args = parser.parse_args()
    card = json.loads((args.root / args.card).read_bytes())
    result = calculate(card, args.root)
    if args.write_report:
        args.write_report.write_text(render(card, result), encoding='utf-8')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
