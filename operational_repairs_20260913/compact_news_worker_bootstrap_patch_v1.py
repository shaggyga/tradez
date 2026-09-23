"""Deterministic worker-only patch for separate asynchronous news bootstrap."""
import ast


def once(text, before, after):
    if text.count(before) != 1:
        raise ValueError("unexpected_worker_bootstrap_patch_site")
    return text.replace(before, after, 1)


def apply(text):
    text = once(text,
        "SCHEDULING_VERSION='shared_revision_history_and_native_generation_retry_v2_20260913'",
        "SCHEDULING_VERSION='shared_revision_history_native_and_compact_bootstrap_v3_20260914'")
    text = once(text,
        "        self.news_pool=ThreadPoolExecutor(max_workers=1,thread_name_prefix='joint-revision-news-capture')\n",
        "        self.news_pool=ThreadPoolExecutor(max_workers=1,thread_name_prefix='joint-revision-news-capture')\n"
        "        self.news_bootstrap_future=None;self.news_bootstrap_complete=False\n"
        "        self.news_bootstrap_next_monotonic=0.;self.news_bootstrap_report=None\n")
    text = once(text,
        "    def finish_news(self):\n        self.observe_news_failure()\n",
        "    def finish_news(self):\n        self.observe_news_failure()\n"
        "        bootstrap=getattr(self,'news_bootstrap_future',None)\n"
        "        if bootstrap is not None:\n"
        "            if not bootstrap.done():return\n"
        "            try:\n"
        "                report=bootstrap.result()\n"
        "                if (type(report) is not dict or report.get('status')!='cache_prepared_fresh_capture_required'\n"
        "                    or report.get('fresh_health_proven') is not False\n"
        "                    or report.get('capture_handle_returned') is not False\n"
        "                    or report.get('original_availability_unchanged') is not True):\n"
        "                    raise ValueError('explicit_non_authorizing_bootstrap_required')\n"
        "                self.news_bootstrap_report=report;self.news_bootstrap_complete=True\n"
        "                self.record_scheduler_event('news_bootstrap_completed_fresh_capture_required',None,\n"
        "                    elapsed_sec=report.get('elapsed_sec'),manifest_sha256=report.get('manifest_sha256'))\n"
        "            except Exception as exc:\n"
        "                self.news_bootstrap_complete=False;self.news_bootstrap_report=None\n"
        "                self.news_bootstrap_next_monotonic=self.monotonic()+60\n"
        "                self.error('news_bootstrap',exc)\n"
        "            self.news_bootstrap_future=None\n"
        "            # Bootstrap invalidates old handles. A separate, ordinary\n"
        "            # capture will establish current health and issue authority.\n"
        "            self.news_capture=None;self.history_share=None\n"
        "            self.observe_news_failure()\n")
    text = once(text,
        "    def schedule_news(self):\n        if self.news_future is not None:return\n",
        "    def schedule_news(self):\n"
        "        if not getattr(self,'news_bootstrap_complete',False):\n"
        "            now=self.monotonic()\n"
        "            if getattr(self,'news_bootstrap_future',None) is not None or now<getattr(self,'news_bootstrap_next_monotonic',0.):return\n"
        "            self.news_bootstrap_next_monotonic=now+60\n"
        "            self.news_bootstrap_future=self.news_pool.submit(self.news_io.bootstrap_inputs,self.news_session,clock=self.clock)\n"
        "            self.record_scheduler_event('news_bootstrap_dispatched',None)\n"
        "            return\n"
        "        if self.news_future is not None:return\n")
    text = once(text,
        "                'news_capture_interval_seconds':60,'news_capture_in_flight':self.news_future is not None,\n",
        "                'news_capture_interval_seconds':60,'news_capture_in_flight':self.news_future is not None,\n"
        "                'news_bootstrap_phase':('complete_fresh_capture_still_required' if getattr(self,'news_bootstrap_complete',False)\n"
        "                    else 'running' if getattr(self,'news_bootstrap_future',None) is not None else 'pending_or_retry'),\n"
        "                'news_bootstrap_report':getattr(self,'news_bootstrap_report',None),\n")
    ast.parse(text)
    return text
