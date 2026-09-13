import argparse
import ast
import json
from pathlib import Path
import sys

import pytest
import oanda_all68_m1_forward_updater as updater


@pytest.fixture(autouse=True)
def no_real_client(monkeypatch):
    def refused(*args, **kwargs):
        raise AssertionError('real manager must remain unloaded')
    monkeypatch.setattr(updater, 'manager_module', refused)


def arguments(tmp_path, **overrides):
    values = dict(pairs=[], bootstrap_all_priced=False, max_requests_per_pair=1,
                  backfill_requests_per_pair=0, batch_size=5000, pause_seconds=0,
                  dry_run=False, report=tmp_path / 'reports' / 'latest.json',
                  interval_sec=0, duration_sec=0, no_gap_recovery=True)
    values.update(overrides)
    return argparse.Namespace(**values)


def fixture_route(monkeypatch):
    visited = []
    monkeypatch.setattr(updater, 'resolve_readonly_oanda_client', lambda: (
        object(), {'environment': 'practice', 'base_url': 'https://api-fxpractice.oanda.com'}))
    def write_fixture(client, path, **kwargs):
        visited.append(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('owned fixture\n')
        return {'instrument': updater.instrument_from_path(path), 'path': str(path),
                'rows_appended': 1, 'rows_backfilled': 0, 'error': ''}
    monkeypatch.setattr(updater, 'update_pair', write_fixture)
    return visited


def test_cli_default_preserves_legacy_paths(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['archive'])
    args = updater.parse_args()
    assert args.candle_root is None and args.quote_snapshot is None
    assert args.report == updater.DEFAULT_REPORT and args.heartbeat == updater.DEFAULT_HEARTBEAT


def test_cli_accepts_both_explicit_paths_with_spaces(monkeypatch, tmp_path):
    root, quote = tmp_path / 'new candles', tmp_path / 'new state' / 'quotes.json'
    monkeypatch.setattr(sys, 'argv', ['archive', '--candle-root', str(root), '--quote-snapshot', str(quote)])
    args = updater.parse_args()
    assert args.candle_root == root and args.quote_snapshot == quote


def test_explicit_bootstrap_uses_only_new_quotes_and_new_archive(monkeypatch, tmp_path):
    new_root, quote = tmp_path / 'new' / 'candles', tmp_path / 'new' / 'quotes.json'
    quote.parent.mkdir()
    quote.write_text(json.dumps({'quotes': {'GBP_USD': {}, 'EUR_USD': {}}}))
    old_root, old_quote = tmp_path / 'old' / 'candles', tmp_path / 'old' / 'quotes.json'
    monkeypatch.setattr(updater, 'CANDLE_ROOT', old_root)
    monkeypatch.setattr(updater, 'QUOTE_SNAPSHOT', old_quote)
    visited = fixture_route(monkeypatch)
    args = arguments(tmp_path, candle_root=new_root, quote_snapshot=quote, bootstrap_all_priced=True)
    assert updater.run_once(args) == 0
    assert visited == [new_root / 'EUR_USD_M1.csv', new_root / 'GBP_USD_M1.csv']
    assert not old_root.exists() and not old_quote.exists()
    report = json.loads(args.report.read_text())
    assert report['archive_paths'] == {'candle_root': str(new_root), 'quote_snapshot': str(quote)}
    assert report['total_rows_appended'] == 2


def test_explicit_pairs_create_only_under_selected_root(monkeypatch, tmp_path):
    visited = fixture_route(monkeypatch)
    selected = tmp_path / 'candles'
    assert updater.run_once(arguments(tmp_path, candle_root=selected, pairs=['usd/jpy', 'eur_usd'])) == 0
    assert visited == [selected / 'EUR_USD_M1.csv', selected / 'USD_JPY_M1.csv']


def test_existing_files_are_discovered_under_selected_root(monkeypatch, tmp_path):
    root = tmp_path / 'candles'
    root.mkdir()
    (root / 'EUR_USD_M1.csv').write_text('fixture')
    (root / 'ignore.csv').write_text('fixture')
    visited = fixture_route(monkeypatch)
    assert updater.run_once(arguments(tmp_path, candle_root=root)) == 0
    assert visited == [root / 'EUR_USD_M1.csv']


def test_two_consecutive_roots_do_not_mutate_module_defaults(monkeypatch, tmp_path):
    original = (updater.CANDLE_ROOT, updater.QUOTE_SNAPSHOT)
    visited = fixture_route(monkeypatch)
    for name in ['first', 'second']:
        assert updater.run_once(arguments(tmp_path, candle_root=tmp_path/name, pairs=['EUR_USD'])) == 0
    assert visited == [tmp_path/'first/EUR_USD_M1.csv', tmp_path/'second/EUR_USD_M1.csv']
    assert (updater.CANDLE_ROOT, updater.QUOTE_SNAPSHOT) == original


def test_legacy_programmatic_namespace_and_noarg_helpers_still_work(monkeypatch, tmp_path):
    selected = tmp_path / 'legacy'
    monkeypatch.setattr(updater, 'CANDLE_ROOT', selected)
    monkeypatch.setattr(updater, 'candle_files', lambda: [selected/'EUR_USD_M1.csv'])
    visited = fixture_route(monkeypatch)
    assert updater.run_once(arguments(tmp_path)) == 0
    assert visited == [selected/'EUR_USD_M1.csv']


def test_legacy_bootstrap_noarg_quote_hook_is_preserved(monkeypatch, tmp_path):
    monkeypatch.setattr(updater, 'CANDLE_ROOT', tmp_path/'legacy')
    monkeypatch.setattr(updater, 'candle_files', lambda: [])
    monkeypatch.setattr(updater, 'priced_instruments', lambda: ['EUR_USD'])
    visited = fixture_route(monkeypatch)
    assert updater.run_once(arguments(tmp_path, bootstrap_all_priced=True)) == 0
    assert visited == [tmp_path/'legacy/EUR_USD_M1.csv']


@pytest.mark.parametrize('content', ['invalid json', '{}', '{"quotes": {}}'])
def test_bad_explicit_snapshot_cannot_fall_back_to_old_quotes(monkeypatch, tmp_path, content):
    quote = tmp_path/'quotes.json'
    quote.write_text(content)
    def fallback(*args, **kwargs):
        raise AssertionError('unexpected client or fallback')
    monkeypatch.setattr(updater, 'resolve_readonly_oanda_client', fallback)
    args = arguments(tmp_path, candle_root=tmp_path/'new', quote_snapshot=quote, bootstrap_all_priced=True)
    with pytest.raises(SystemExit, match='No practice-priced instruments'):
        updater.run_once(args)
    assert not args.report.exists() and not args.candle_root.exists()


def test_empty_explicit_directory_fails_before_credentials(monkeypatch, tmp_path):
    def forbidden(*args, **kwargs):
        raise AssertionError('credentials not required for missing input')
    monkeypatch.setattr(updater, 'resolve_readonly_oanda_client', forbidden)
    with pytest.raises(SystemExit, match='No .*M1.csv files'):
        updater.run_once(arguments(tmp_path, candle_root=tmp_path/'missing'))


