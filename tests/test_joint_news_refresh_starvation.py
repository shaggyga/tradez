"""Reproduce the frozen v11 scheduler defect without importing/fitting models.

Execute the actual tick AST with controlled completion during quote/settlement
work. The proposed patch is deliberately not installed into a sealed cohort.
"""
import ast
from pathlib import Path
import types
import unittest

SOURCE = Path(__file__).resolve().parents[1] / 'trad/oanda_joint_price_news_forecast_study_v11.py'
OLD = 'self.finish_work();self.schedule_work();self.score()'
NEW = 'self.finish_work();self.schedule_news();self.schedule_work();self.score()'


def tick_function(corrected):
    text = SOURCE.read_text(encoding='utf-8')
    if corrected:
        if text.count(OLD) != 1:
            raise AssertionError('Frozen source no longer matches proposed correction')
        text = text.replace(OLD, NEW)
    tree = ast.parse(text)
    runner = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'PairRunner')
    tick = next(n for n in runner.body if isinstance(n, ast.FunctionDef) and n.name == 'tick')
    namespace = {'number': float, 'summary_boundary': types.SimpleNamespace(SummaryBoundaryError=RuntimeError)}
    exec(compile(ast.Module(body=[tick], type_ignores=[]), str(SOURCE), 'exec'), namespace)
    return namespace['tick']


class ControlledRunner:
    def __init__(self, *, due=True, completes=True):
        self.future = object()
        self.news_future = None
        self.news_bootstrap_future = None
        self.fresh_capture_pair = None
        self.states = {}
        self.last_poll = 0
        self.now = 1000
        self.due = due
        self.completes = completes
        self.completed = False
        self.news_dispatches = 0
        self.pair_dispatches = 0
        self.scores = 0

    def clock(self): return self.now
    def finish_news(self): pass
    def finish_work(self):
        if self.completed:
            self.future = None
            self.completed = False
    def schedule_news(self):
        if self.future is None and self.news_future is None and self.due:
            self.news_future = object()
            self.news_dispatches += 1
    def schedule_work(self):
        if self.future is None and self.news_future is None:
            self.future = object()
            self.pair_dispatches += 1
    def poll_quotes(self): self.completed = self.completes
    def score(self): self.scores += 1
    def publish_status(self): pass


class RefreshOrderingTests(unittest.TestCase):
    def test_original_starves_overdue_refresh_across_repeated_completions(self):
        runner = ControlledRunner()
        tick = tick_function(False)
        for _ in range(20):
            tick(runner)
            runner.now += 3
        self.assertEqual(runner.news_dispatches, 0)
        self.assertEqual(runner.pair_dispatches, 20)

    def test_correction_dispatches_overdue_news_before_more_pair_work(self):
        runner = ControlledRunner()
        tick_function(True)(runner)
        self.assertEqual(runner.news_dispatches, 1)
        self.assertEqual(runner.pair_dispatches, 0)
        self.assertEqual(runner.scores, 1)

    def test_correction_keeps_pair_work_when_refresh_not_due(self):
        runner = ControlledRunner(due=False)
        tick_function(True)(runner)
        self.assertEqual(runner.news_dispatches, 0)
        self.assertEqual(runner.pair_dispatches, 1)

    def test_correction_does_not_interrupt_inflight_pair(self):
        runner = ControlledRunner(completes=False)
        tick_function(True)(runner)
        self.assertEqual(runner.news_dispatches, 0)
        self.assertIsNotNone(runner.future)

    def test_correction_does_not_duplicate_inflight_news(self):
        runner = ControlledRunner()
        runner.news_future = object()
        tick_function(True)(runner)
        self.assertEqual(runner.news_dispatches, 0)
        self.assertEqual(runner.pair_dispatches, 0)


if __name__ == '__main__':
    unittest.main()
