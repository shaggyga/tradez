from types import SimpleNamespace
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'trad'))
import oanda_joint_news_scheduler_v1 as m
from test_joint_news_refresh_starvation import ControlledRunner


def test_completion_is_serviced_before_next_ledger_not_after_full_sweep():
    r=ControlledRunner(due=False,completes=False);events=[]
    r.future=SimpleNamespace(done=lambda:r.completed)
    class Ledger:
        def __init__(self,n):self.n=n
        def settle(self):
            events.append(self.n)
            if self.n==1:r.completed=True
            if self.n==2:assert r.pair_dispatches==1
    r.states={'a':{'families':{'f':{'ledger':Ledger(1)}}},'b':{'families':{'f':{'ledger':Ledger(2)}}}}
    m.ScheduledRunner.tick(r)
    assert events==[1,2] and r.pair_dispatches==1


def test_unfinished_work_is_not_preempted():
    r=ControlledRunner();r.future=SimpleNamespace(done=lambda:False)
    assert m.ScheduledRunner.service_completed(r) is False
    assert r.pair_dispatches==r.news_dispatches==0


def test_completed_news_drained_before_new_pair_work():
    r=ControlledRunner(due=False);r.future=None
    r.news_future=SimpleNamespace(done=lambda:True);calls=[]
    def finish():calls.append('news');r.news_future=None
    r.finish_news=finish
    assert m.ScheduledRunner.service_completed(r) is True
    assert calls==['news'] and r.pair_dispatches==1


def test_settlement_error_does_not_skip_siblings_or_handoff():
    r=ControlledRunner(due=False,completes=False);r.future=SimpleNamespace(done=lambda:r.completed);errors=[];seen=[]
    class Ledger:
        def __init__(self,n):self.n=n
        def settle(self):
            seen.append(self.n)
            if self.n==1:r.completed=True;raise ValueError('refused')
    r.error=lambda p,e:errors.append((p,str(e)))
    r.states={'a':{'families':{'f':{'ledger':Ledger(1)}}},'b':{'families':{'f':{'ledger':Ledger(2)}}}}
    m.ScheduledRunner.tick(r)
    assert seen==[1,2] and errors==[('a','refused')] and r.pair_dispatches==1


def test_quantum_preserves_round_robin_and_does_not_interrupt_ledger(monkeypatch):
    r=ControlledRunner(due=False,completes=False);r.future=SimpleNamespace(done=lambda:False)
    elapsed=[0.];seen=[]
    monkeypatch.setattr(m.time,'perf_counter',lambda:elapsed[0])
    class Ledger:
        def __init__(self,n):self.n=n
        def settle(self):
            seen.append(('start',self.n));elapsed[0]+=3;seen.append(('end',self.n))
    r.states={str(n):{'families':{'f':{'ledger':Ledger(n)}}} for n in range(3)}
    for n in range(3):
        m.ScheduledRunner.tick(r);r.now+=3
        assert len(seen)==2*(n+1)
    assert seen==[(action,n) for n in range(3) for action in ('start','end')]
