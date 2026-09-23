"""Deterministic worker patch: bounded news refresh with pair fit handoff.

Build-time only. The runtime keeps the original price/family fairness, source,
clock, expiry, account and issue guards. No historical receipt is modified.
"""
import ast

import compact_news_worker_bootstrap_patch_v1 as bootstrap

once = bootstrap.once


def apply_after_bootstrap(text):
    text = once(text,
        "SCHEDULING_VERSION='shared_revision_history_native_and_compact_bootstrap_v3_20260914'",
        "SCHEDULING_VERSION='shared_revision_history_completion_cadence_and_fit_handoff_v4_20260914'")
    text = once(text,
        "        self.news_bootstrap_next_monotonic=0.;self.news_bootstrap_report=None\n",
        "        self.news_bootstrap_next_monotonic=0.;self.news_bootstrap_report=None\n"
        "        self.news_refresh_next_monotonic=0.\n")
    text = once(text,
        "        if self.news_future is None or not self.news_future.done():return\n        try:\n",
        "        if self.news_future is None or not self.news_future.done():return\n"
        "        # A slow successful or failed read gets a complete quiet interval.\n"
        "        # This is scheduling only; retained clocks/authority never move.\n"
        "        self.news_refresh_next_monotonic=self.monotonic()+schedule.NEWS_CAPTURE_INTERVAL_SECONDS\n"
        "        try:\n")
    text = once(text,
        "    def schedule_news(self):\n",
        "    def schedule_news(self):\n"
        "        # A current pair capture/fit owns its useful-work window. Never\n"
        "        # queue a competing lock-holding news task behind that work.\n"
        "        if getattr(self,'future',None) is not None:return\n")
    text = once(text,
        "        if self.news_future is not None:return\n"
        "        state,dispatch=schedule.dispatch_shared_capture(self.news_schedule,self.monotonic())\n",
        "        if self.news_future is not None:return\n"
        "        now=self.monotonic()\n"
        "        if now<getattr(self,'news_refresh_next_monotonic',0.):return\n"
        "        state,dispatch=schedule.dispatch_shared_capture(self.news_schedule,now)\n")
    text = once(text,
        "        self.news_schedule=state\n"
        "        try:self.news_future=self.news_pool.submit(capture_shared_owned,self.news_session,tuple(self.queue),clock=self.clock)\n",
        "        self.news_schedule=state\n"
        "        self.news_refresh_next_monotonic=now+schedule.NEWS_CAPTURE_INTERVAL_SECONDS\n"
        "        try:self.news_future=self.news_pool.submit(capture_shared_owned,self.news_session,tuple(self.queue),clock=self.clock)\n")
    text = once(text,
        "        except Exception:\n"
        "            self.news_schedule,_=schedule.failed_shared_capture(self.news_schedule,current_failure_serial=self.news_schedule.failure_serial)\n",
        "        except Exception:\n"
        "            self.news_refresh_next_monotonic=self.monotonic()+schedule.NEWS_CAPTURE_INTERVAL_SECONDS\n"
        "            self.news_schedule,_=schedule.failed_shared_capture(self.news_schedule,current_failure_serial=self.news_schedule.failure_serial)\n")
    text = once(text,
        "    def schedule_work(self):\n        if self.future is not None:return\n",
        "    def schedule_work(self):\n"
        "        if self.future is not None:return\n"
        "        # Avoid a dispatch race before the news executor acquires its\n"
        "        # session lock. Existing eligible handles are not renewed here.\n"
        "        if self.news_future is not None or getattr(self,'news_bootstrap_future',None) is not None:return\n")
    text = once(text,
        "                'news_capture_interval_seconds':60,'news_capture_in_flight':self.news_future is not None,\n",
        "                'news_capture_interval_seconds':60,'news_capture_in_flight':self.news_future is not None,\n"
        "                'news_refresh_cadence_basis':'completion_or_failure_plus_interval',\n"
        "                'news_refresh_wait_seconds':max(0.,getattr(self,'news_refresh_next_monotonic',0.)-self.monotonic()),\n"
        "                'news_refresh_waits_for_pair_work':self.future is not None,\n")
    text = once(text,
        "        self.finish_news();self.schedule_news();self.finish_work();self.schedule_work()\n",
        "        self.finish_news();self.finish_work()\n"
        "        # A completed pair capture gets the existing guarded fair-fit\n"
        "        # opportunity before an overdue refresh can monopolize the lock.\n"
        "        # If no fit qualifies, news is free to proceed; no forced signal.\n"
        "        if (getattr(self,'future',None) is None and self.news_future is None\n"
        "                and getattr(self,'news_bootstrap_future',None) is None\n"
        "                and getattr(self,'fresh_capture_pair',None) is not None):\n"
        "            handoff_now=number(self.clock());self.schedule_fit(handoff_now,int(handoff_now//900))\n"
        "        self.schedule_news();self.schedule_work()\n")
    ast.parse(text)
    return text


def apply(text):
    """Apply to the same unpatched canonical worker accepted by V1."""
    return apply_after_bootstrap(bootstrap.apply(text))
