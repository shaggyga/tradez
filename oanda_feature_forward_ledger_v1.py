"""New owner-local append-only feature forward ledger. No old ledger imports.

SQLite DELETE journalling and a conservative page cap bound this database plus
its rollback journal below max_bytes. Capacity refusal never prunes evidence.
Actual event publication is the owner's observation AFTER committing its frozen
batch, then a durable acknowledgement. It is not an archive generation clock.
"""
from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal, localcontext, Context
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import time
import zlib

try:
    import oanda_feature_forward_protocol_v1 as p
except ModuleNotFoundError:
    from trad import oanda_feature_forward_protocol_v1 as p

MAX_LEDGER_BYTES = 512 * 1024 * 1024
ABSOLUTE_MAX_LEDGER_BYTES = 4096 * 1024 * 1024
RESERVE = 2 * 1024 * 1024
SCHEMA = "feature_forward_owned_ledger_v1"


def safe_path(value):
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts or (os.name == "nt" and path.drive.upper() != "C:"):
        raise ValueError("absolute_C_path_required")
    if any(":" in part for part in path.parts[1:]):
        raise ValueError("alternate_data_stream_refused")
    for item in reversed((path, *path.parents)):
        if item.exists() or item.is_symlink():
            if item.is_symlink() or (hasattr(item, "is_junction") and item.is_junction()):
                raise ValueError("reparse_path_refused")
    return path


def parse_time(value):
    if type(value) is not str:
        raise ValueError("explicit_UTC_quote_time_required")
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("timezone_required")
    return p.epoch(dt.timestamp())


def unpack(raw):
    obj = zlib.decompressobj()
    decoded = obj.decompress(raw, p.MAX_BATCH_BYTES + 1)
    if len(decoded) > p.MAX_BATCH_BYTES or not obj.eof or obj.unused_data or obj.unconsumed_tail:
        raise ValueError("compressed_batch_bound_or_trailing_stream")
    return json.loads(decoded)


class ForwardLedger:
    def __init__(self, path, *, clock=time.time, max_bytes=MAX_LEDGER_BYTES, minimum_free_bytes=64*1024*1024):
        self.path = safe_path(path)
        if self.path.suffix != ".sqlite" or not self.path.parent.is_dir():
            raise ValueError("existing_owned_parent_and_sqlite_name_required")
        if type(max_bytes) is not int or not 16*1024*1024 <= max_bytes <= ABSOLUTE_MAX_LEDGER_BYTES:
            raise ValueError("ledger_byte_bound")
        if type(minimum_free_bytes) is not int or minimum_free_bytes < 0:
            raise ValueError("free_space_bound")
        self.clock, self.max_bytes, self.minimum_free_bytes = clock, max_bytes, minimum_free_bytes
        resource_identity = p.canonical({"max_bytes": max_bytes, "minimum_free_bytes": minimum_free_bytes}).decode()
        self.last_refusal = None
        self.publication_guard = None
        self.settlement_guard = None
        self._last_clock = 0.0
        sources = {Path(module.__file__).name: hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
                   for module in (p, p.exact)}
        sources[Path(__file__).name] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        for name in ("oanda_feature_move_mapping_v1.py", "oanda_feature_observations_v1.py",
                     "oanda_feature_research_clock_v1.py", "oanda_feature_forward_worker_v1.py"):
            source = safe_path(Path(__file__).absolute().parent/name)
            sources[name] = hashlib.sha256(source.read_bytes()).hexdigest()
        self.source_pins = sources
        source_identity = p.digest(sources)
        # Refuse foreign SQLite files rather than creating tables in old cohorts.
        existed = self.path.exists()
        self._lock_handle = None
        lock_path = safe_path(str(self.path)+".owner.lock")
        handle = lock_path.open("a+b")
        try:
            if handle.seek(0, 2) == 0:
                handle.write(b"0")
                handle.flush()
            handle.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._lock_handle = handle
        except (OSError, BlockingIOError) as exc:
            handle.close()
            raise ValueError("forward_ledger_owner_already_active") from exc
        try:
            self.db = sqlite3.connect(self.path, isolation_level=None, timeout=2)
        except BaseException:
            self._unlock()
            raise
        try:
            if existed:
                # Verify ownership before any persistent pragma or DDL.
                row = self.db.execute("SELECT value FROM ff_meta WHERE key='schema'").fetchone()
                if row != (SCHEMA,):
                    raise ValueError("foreign_ledger_refused")
                if self.db.execute("SELECT value FROM ff_meta WHERE key='protocol'").fetchone() != (p.PROTOCOL_SHA256,):
                    raise ValueError("protocol_mismatch")
                if self.db.execute("SELECT value FROM ff_meta WHERE key='source_identity'").fetchone() != (source_identity,):
                    raise ValueError("ledger_source_generation_changed")
                if self.db.execute("SELECT value FROM ff_meta WHERE key='resource_identity'").fetchone() != (resource_identity,):
                    raise ValueError("ledger_resource_configuration_changed")
            self.db.execute("PRAGMA busy_timeout=2000")
            self.db.execute("PRAGMA synchronous=FULL")
            mode = self.db.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
            if mode != "delete":
                raise ValueError("delete_journal_required")
            self.db.execute("PRAGMA temp_store=MEMORY")
            self.db.execute("PRAGMA cache_size=-4096")
            if not existed:
                self.db.executescript("""
                BEGIN IMMEDIATE;
                CREATE TABLE ff_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE ff_batches(id TEXT PRIMARY KEY,bucket INTEGER NOT NULL,window INTEGER NOT NULL,
                  digest TEXT NOT NULL,body BLOB NOT NULL,prepared REAL NOT NULL,quote_highwater INTEGER NOT NULL,
                  UNIQUE(bucket,window));
                CREATE TABLE ff_publications(batch TEXT PRIMARY KEY,completed REAL NOT NULL,body TEXT NOT NULL);
                CREATE INDEX ff_publication_time ON ff_publications(completed);
                CREATE TABLE ff_quote_receipts(id TEXT PRIMARY KEY,body TEXT NOT NULL);
                CREATE TABLE ff_quotes(seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,
                  pair TEXT NOT NULL,market REAL NOT NULL,available REAL NOT NULL,body TEXT NOT NULL);
                CREATE INDEX ff_quote_pair_time ON ff_quotes(pair,market,seq);
                CREATE INDEX ff_quote_available ON ff_quotes(available);
                CREATE TABLE ff_jobs(id TEXT PRIMARY KEY,batch TEXT NOT NULL,pair TEXT NOT NULL,horizon INTEGER NOT NULL);
                CREATE INDEX ff_job_pair ON ff_jobs(pair,batch,horizon);
                CREATE TABLE ff_categories(batch TEXT NOT NULL,pair TEXT NOT NULL,total INTEGER NOT NULL,
                  alert INTEGER NOT NULL,control INTEGER NOT NULL,excluded INTEGER NOT NULL,PRIMARY KEY(batch,pair));
                CREATE TABLE ff_outcomes(job TEXT PRIMARY KEY,status TEXT NOT NULL,body TEXT NOT NULL,available REAL NOT NULL);
                CREATE TABLE ff_compact_outcomes(job TEXT PRIMARY KEY,status TEXT NOT NULL,magnitude INTEGER,probes TEXT);
                CREATE TABLE ff_sources(id TEXT PRIMARY KEY,digest TEXT NOT NULL);
                CREATE TABLE ff_refusals(seq INTEGER PRIMARY KEY AUTOINCREMENT,observed REAL NOT NULL,body TEXT NOT NULL);
                COMMIT;
                """)
                with self.transaction(1024, reserved=True):
                    self.db.executemany("INSERT INTO ff_meta VALUES (?,?)", [("schema", SCHEMA), ("protocol", p.PROTOCOL_SHA256), ("source_identity", source_identity), ("source_pins", p.canonical(sources).decode()), ("resource_identity", resource_identity)])
                for table in ("ff_batches", "ff_publications", "ff_quote_receipts", "ff_quotes", "ff_jobs", "ff_categories", "ff_outcomes", "ff_compact_outcomes", "ff_sources", "ff_refusals"):
                    for action in ("UPDATE", "DELETE"):
                        self.db.execute(f"CREATE TRIGGER {table}_{action.lower()} BEFORE {action} ON {table} BEGIN SELECT RAISE(ABORT,'append_only'); END")
            page = self.db.execute("PRAGMA page_size").fetchone()[0]
            # A rollback journal can contain each existing database page once.
            # Reserve room for journal headers and safety margin, not just data.
            ceiling = (max_bytes - 4*RESERVE)//(2*page)
            actual = self.db.execute(f"PRAGMA max_page_count={ceiling}").fetchone()[0]
            if actual > ceiling:
                raise ValueError("existing_database_exceeds_bound")
            self.capacity(0, reserved=True)
            for table, col in (("ff_batches", "prepared"), ("ff_publications", "completed"), ("ff_quotes", "available"), ("ff_outcomes", "available"), ("ff_refusals", "observed")):
                self._last_clock = max(self._last_clock, self.db.execute(f"SELECT coalesce(max({col}),0) FROM {table}").fetchone()[0])
            last = self.db.execute("SELECT body FROM ff_refusals ORDER BY seq DESC LIMIT 1").fetchone()
            self.last_refusal = json.loads(last[0]) if last else None
        except BaseException:
            self.db.close()
            self._unlock()
            raise

    def close(self):
        try:
            self.db.close()
        finally:
            self._unlock()

    def _unlock(self):
        if self._lock_handle is not None:
            handle, self._lock_handle = self._lock_handle, None
            try:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()

    def verify_sources(self):
        for name, expected in self.source_pins.items():
            if hashlib.sha256(safe_path(Path(__file__).absolute().parent/name).read_bytes()).hexdigest() != expected:
                raise ValueError("forward_source_changed")

    def now(self):
        value = p.epoch(self.clock())
        if value < self._last_clock:
            raise ValueError("owner_clock_backstep")
        self._last_clock = value
        return value

    def capacity(self, estimate, *, reserved=False):
        sizes = 0
        for suffix in ("", "-journal", "-wal", "-shm", ".owner.lock"):
            file = safe_path(str(self.path) + suffix)
            if file.exists():
                sizes += file.stat().st_size
        pages = self.db.execute("PRAGMA page_count").fetchone()[0]
        page_size = self.db.execute("PRAGMA page_size").fetchone()[0]
        logical_ceiling = (self.max_bytes-4*RESERVE)//2 - (0 if reserved else RESERVE)
        if sizes + 2*estimate + RESERVE > self.max_bytes or pages*page_size + estimate > logical_ceiling:
            raise ValueError("ledger_capacity_refused")
        if shutil.disk_usage(self.path.parent).free < self.minimum_free_bytes + 2*estimate + RESERVE:
            raise ValueError("free_space_refused")

    @contextmanager
    def transaction(self, estimate, *, reserved=False):
        self.capacity(estimate, reserved=reserved)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.capacity(0, reserved=reserved)
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def refusal(self, reason):
        self.last_refusal = {"status": "admission_refused", "reason": str(reason)[:300], "observed_epoch": self.now()}
        try:
            with self.transaction(2048, reserved=True):
                self.db.execute("INSERT INTO ff_refusals(observed,body) VALUES (?,?)", (self.last_refusal["observed_epoch"], p.canonical(self.last_refusal).decode()))
        except (ValueError, sqlite3.Error):
            self.last_refusal["durable_refusal_unavailable_capacity"] = True
        return self.last_refusal

    def due(self):
        bucket = int(self.now() // p.PROTOCOL["decision_cadence_sec"])
        found = self.db.execute("SELECT count(*) FROM ff_batches WHERE bucket=?", (bucket,)).fetchone()[0]
        return found < len(p.PROTOCOL["comparison_windows_sec"])

    def publish_comparisons(self, mapping_by_window):
        self.verify_sources()
        if not isinstance(mapping_by_window, dict) or set(mapping_by_window) != set(p.PROTOCOL["comparison_windows_sec"]):
            raise ValueError("all_comparison_windows_required")
        now = self.now()
        prepared = [p.prepare_comparisons(mapping_by_window[w], window_sec=w, observed_epoch=now)
                    for w in p.PROTOCOL["comparison_windows_sec"]]
        results = []
        for payload in prepared:
            try:
                results.append(self._publish(payload, now))
            except (ValueError, sqlite3.Error) as exc:
                results.append(self.refusal(exc))
        return results

    def _publish(self, payload, prepared):
        if self.publication_guard:
            self.publication_guard()
        sources = []
        for row in payload["source_snapshots"]:
            identity = p.digest([row.get("source_schema_id"), row.get("snapshot_id")])
            value = p.digest(row)
            found = self.db.execute("SELECT digest FROM ff_sources WHERE id=?", (identity,)).fetchone()
            if found and found[0] != value:
                raise ValueError("source_identity_collision")
            sources.append((identity, value))
        row = self.db.execute("SELECT id,digest,body,prepared,quote_highwater FROM ff_batches WHERE bucket=? AND window=?",
                              (payload["decision_bucket"], payload["window_sec"])).fetchone()
        if row:
            batch = row[0]
            if self.db.execute("SELECT 1 FROM ff_publications WHERE batch=?", (batch,)).fetchone():
                return {"status": "cadence_already_published", "batch_id": batch}
            # A crash after immutable preparation can only publish those exact
            # bytes. Never replace them with the caller's later observations.
            payload, prepared, highwater = unpack(row[2]), row[3], row[4]
        else:
            raw = p.canonical(payload)
            compressed = zlib.compress(raw, 6)
            batch = p.digest([p.PROTOCOL_SHA256, payload["decision_bucket"], payload["window_sec"], hashlib.sha256(raw).hexdigest()])
            highwater = self.db.execute("SELECT coalesce(max(seq),0) FROM ff_quotes").fetchone()[0]
            with self.transaction(len(compressed)*2+len(payload["pairs"])*4096+8192):
                self.db.execute("INSERT INTO ff_batches VALUES (?,?,?,?,?,?,?)", (batch, payload["decision_bucket"], payload["window_sec"], hashlib.sha256(raw).hexdigest(), compressed, prepared, highwater))
                for identity, value in sources:
                    found = self.db.execute("SELECT digest FROM ff_sources WHERE id=?", (identity,)).fetchone()
                    if found and found[0] != value:
                        raise ValueError("source_identity_collision")
                    self.db.execute("INSERT OR IGNORE INTO ff_sources VALUES (?,?)", (identity, value))
                for pair in payload["pairs"]:
                    count = payload["pair_category_counts"][pair["instrument"]]
                    self.db.execute("INSERT INTO ff_categories VALUES (?,?,?,?,?,?)", (batch, pair["instrument"],
                                    count["total"], count["alert"], count["control"], count["excluded"]))
                    for horizon in p.PROTOCOL["horizons_sec"]:
                        job = p.digest([batch, pair["instrument"], horizon])
                        self.db.execute("INSERT INTO ff_jobs VALUES (?,?,?,?)", (job, batch, pair["instrument"], horizon))
        clock_evidence = self.publication_guard() if self.publication_guard else None
        completed = self.now()  # after frozen commit AND completion clock proof
        if completed < prepared or completed-prepared > p.PROTOCOL["source_max_age_sec"]:
            raise ValueError("publication_clock_or_preparation_age")
        latest = payload.get("latest_observed_utc")
        if any(row["forward_category"] != "excluded" for row in payload["comparisons"]):
            if latest is None or not 0 <= completed-p.utc_epoch(latest) <= p.PROTOCOL["source_max_age_sec"]:
                raise ValueError("source_expired_before_publication")
        references = {}
        for pair in payload["pairs"]:
            quote = self.db.execute("SELECT seq,market,available,body FROM ff_quotes WHERE pair=? AND seq<=? AND market<=? AND available<=? ORDER BY seq DESC LIMIT 1",
                                    (pair["instrument"], highwater, completed, completed)).fetchone()
            if quote and completed-quote[1] <= p.PROTOCOL["quote_market_max_age_sec"]:
                references[pair["instrument"]] = {"seq": quote[0], "market_epoch": quote[1], "available_epoch": quote[2], **json.loads(quote[3])}
        publication = {"batch_id": batch, "completed_epoch": completed, "prepared_epoch": prepared,
                       "quote_highwater": highwater, "reference_quotes": references,
                       "clock_gate": clock_evidence,
                       "clock_scope": "owner_observed_preceding_commit_then_durable_acknowledgement"}
        with self.transaction(len(p.canonical(publication))+8192, reserved=True):
            self.db.execute("INSERT INTO ff_publications VALUES (?,?,?)", (batch, completed, p.canonical(publication).decode()))
        return {"status": "published", "batch_id": batch, "publication_epoch": completed,
                "comparisons": len(payload["comparisons"]), "references": len(references)}

    def _entry(self, pair, publication, highwater):
        return self.db.execute("SELECT seq,market,available,body FROM ff_quotes WHERE pair=? AND seq>? AND market>? AND market<=? AND available>? AND available<=? ORDER BY seq LIMIT 1", (pair, highwater, publication, publication+60, publication, publication+60)).fetchone()

    def _target(self, pair, entry, target):
        return self.db.execute("SELECT seq,market,available,body FROM ff_quotes WHERE pair=? AND seq>? AND market>=? AND market<=? AND available>=? AND available<=? ORDER BY seq LIMIT 1", (pair, entry[0], target, target+60, target, target+60)).fetchone()

    def _quote_needed(self, pair, market, admitted, cache):
        jobs = self.db.execute("SELECT j.batch,j.horizon,p.completed,p.body FROM ff_jobs j JOIN ff_publications p ON p.batch=j.batch LEFT JOIN ff_outcomes o ON o.job=j.id WHERE j.pair=? AND o.job IS NULL AND p.completed>=? AND ((p.completed<? AND p.completed+60>=?) OR (p.completed+j.horizon<=? AND p.completed+j.horizon+60>=?))", (pair, admitted-3660, admitted, admitted, admitted, admitted)).fetchall()
        for batch, horizon, publication, raw in jobs:
            key = (batch, pair)
            if key not in cache:
                ack = json.loads(raw)
                cache[key] = (ack, self._entry(pair, publication, ack["quote_highwater"]))
            ack, entry = cache[key]
            if pair not in ack["reference_quotes"]:
                continue
            if entry is None:
                if publication < market <= publication+60 and publication < admitted <= publication+60:
                    return True
            else:
                target = publication+horizon
                if target <= market <= target+60 and target <= admitted <= target+60 and self._target(pair, entry, target) is None:
                    return True
        return False

    def observe_quotes(self, quotes, *, read_started_epoch, read_completed_epoch, source_receipt=None, force_reference=False):
        start, end, admitted = p.epoch(read_started_epoch), p.epoch(read_completed_epoch), self.now()
        if not start <= end <= admitted:
            raise ValueError("quote_read_clock_order")
        if not isinstance(quotes, dict) or len(quotes) > 68:
            raise ValueError("bounded_quote_inventory")
        if type(force_reference) is not bool:
            raise ValueError("explicit_reference_capture_flag")
        receipt_body = p.canonical(source_receipt).decode()
        if len(receipt_body) > 256*1024:
            raise ValueError("shared_quote_receipt_bound")
        receipt_id = hashlib.sha256(receipt_body.encode()).hexdigest()
        result = Counter()
        demand_cache = {}
        for pair, row in quotes.items():
            try:
                p.instrument(pair)
                body = p.price_quote(row)
                market = parse_time(row.get("time"))  # never generation time
                if market > start or admitted-market > p.PROTOCOL["quote_market_max_age_sec"]:
                    raise ValueError("future_or_stale_quote")
                identity = p.digest([pair, row.get("source", ""), market])
                # Stable quote identity excludes this read's later clock.
                body.update(tradeable=True, time=row["time"], source=str(row.get("source", ""))[:160])
                found = self.db.execute("SELECT body FROM ff_quotes WHERE id=?", (identity,)).fetchone()
                if found:
                    previous = json.loads(found[0])
                    if any(previous[key] != body[key] for key in body):
                        raise ValueError("quote_identity_collision")
                    result["duplicate"] += 1
                    continue
                if not force_reference and not self._quote_needed(pair, market, admitted, demand_cache):
                    result["observed_not_needed"] += 1
                    continue
                body.update(read_started_epoch=start, read_completed_epoch=end, owner_admitted_epoch=admitted,
                            source_receipt_sha256=receipt_id)
                with self.transaction(4096+len(receipt_body), reserved=True):
                    old = self.db.execute("SELECT body FROM ff_quote_receipts WHERE id=?", (receipt_id,)).fetchone()
                    if old and old[0] != receipt_body:
                        raise ValueError("quote_receipt_collision")
                    self.db.execute("INSERT OR IGNORE INTO ff_quote_receipts VALUES (?,?)", (receipt_id, receipt_body))
                    self.db.execute("INSERT INTO ff_quotes(id,pair,market,available,body) VALUES (?,?,?,?,?)", (identity, pair, market, admitted, p.canonical(body).decode()))
                result["admitted"] += 1
            except (ValueError, TypeError, sqlite3.Error) as exc:
                result["refused"] += 1
                self.refusal(f"quote:{pair}:{exc}")
        return dict(result)

    def settle(self, *, limit=512):
        if type(limit) is not int or not 1 <= limit <= 4096:
            raise ValueError("settlement_limit")
        now = self.now()
        jobs = self.db.execute("SELECT j.id,j.batch,j.pair,j.horizon,p.completed,p.body FROM ff_jobs j JOIN ff_publications p ON p.batch=j.batch LEFT JOIN ff_outcomes o ON o.job=j.id WHERE o.job IS NULL AND p.completed+j.horizon<=? ORDER BY p.completed+j.horizon,j.id LIMIT ?", (now, limit)).fetchall()
        result = Counter()
        for job, batch, pair, horizon, publication, raw in jobs:
            ack = json.loads(raw)
            target = publication+horizon
            reference = ack["reference_quotes"].get(pair)
            entry = self._entry(pair, publication, ack["quote_highwater"])
            last = None
            if entry:
                last = self._target(pair, entry, target)
            reason = None
            if reference is None:
                reason = "reference_quote_unavailable"
            elif entry is None and now > publication+60:
                reason = "subsequent_entry_unavailable"
            elif last is None and now > target+60:
                reason = "target_quote_unavailable"
            if reason is None and last is None:
                result["pending"] += 1
                continue
            body = {"job_id": job, "batch_id": batch, "instrument": pair, "horizon_sec": horizon,
                    "publication_epoch": publication, "target_epoch": target, "reason": reason,
                    "reference": reference, "entry": None, "target": None, "score": None}
            if not reason:
                e, t = json.loads(entry[3]), json.loads(last[3])
                body.update(entry={"seq": entry[0], "market_epoch": entry[1], "available_epoch": entry[2], **e},
                            target={"seq": last[0], "market_epoch": last[1], "available_epoch": last[2], **t},
                            score=p.score_pair(reference, e, t), entry_delay_sec=entry[1]-publication,
                            target_delay_sec=last[1]-target, actual_holding_sec=last[1]-entry[1])
            status = "unknown" if reason else "scored"
            body["clock_gate"] = self.settlement_guard() if self.settlement_guard else None
            available = self.now()
            body["owner_score_observed_epoch"] = available
            body["availability_scope"] = "owner_observed_completed_computation_before_durable_commit_not_external_visibility"
            try:
                with self.transaction(len(p.canonical(body))+4096, reserved=True):
                    self.db.execute("INSERT INTO ff_outcomes VALUES (?,?,?,?)", (job, status, p.canonical(body).decode(), available))
                    self.db.execute("INSERT INTO ff_compact_outcomes VALUES (?,?,?,?)", (job, status,
                                    int(body["score"]["price_move_at_least_threshold"]) if body["score"] else None,
                                    p.canonical(body["score"]["probes"]).decode() if body["score"] else None))
                result[status] += 1
            except (ValueError, sqlite3.Error) as exc:
                self.refusal(f"settlement:{exc}")
                result["capacity_refused"] += 1
        return dict(result)

    def iter_events(self):
        """Bounded one-comparison-batch decode at a time; no display truncation."""
        for batch, raw in self.db.execute("SELECT id,body FROM ff_batches ORDER BY bucket,window"):
            payload = unpack(raw)
            ack = self.db.execute("SELECT completed FROM ff_publications WHERE batch=?", (batch,)).fetchone()
            outcomes = {pair: {} for pair in (item["instrument"] for item in payload["pairs"])}
            for pair, horizon, status, body in self.db.execute("SELECT j.pair,j.horizon,o.status,o.body FROM ff_jobs j LEFT JOIN ff_outcomes o ON o.job=j.id WHERE j.batch=?", (batch,)):
                outcomes[pair][horizon] = (status or ("pending" if ack else "unpublished"), json.loads(body) if body else None)
            for row in payload["comparisons"]:
                for horizon in p.PROTOCOL["horizons_sec"]:
                    status, outcome = outcomes[row["instrument"]][horizon]
                    yield {"event_id": p.event_id(batch, row["instrument"], row["feature_id"], horizon),
                           "batch_id": batch, "window_sec": payload["window_sec"], "horizon_sec": horizon,
                           "comparison": row, "status": status, "outcome": outcome}

    def summary(self):
        groups = {}
        counts = {table.removeprefix("ff_"): self.db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                  for table in ("ff_batches", "ff_publications", "ff_quotes", "ff_quote_receipts", "ff_jobs", "ff_outcomes", "ff_refusals")}
        paired = {}
        with localcontext(Context(prec=80)):
            # The compact rows are created in the same transactions as their
            # complete immutable originals. Summary work scales with pair/jobs,
            # not full feature populations or repeated decompression.
            query = """SELECT b.window,j.horizon,c.total,c.alert,c.control,c.excluded,
                        pub.completed,o.status,o.magnitude,o.probes
                        FROM ff_jobs j JOIN ff_batches b ON b.id=j.batch
                        JOIN ff_categories c ON c.batch=j.batch AND c.pair=j.pair
                        LEFT JOIN ff_publications pub ON pub.batch=j.batch
                        LEFT JOIN ff_compact_outcomes o ON o.job=j.id ORDER BY j.rowid"""
            for window, horizon, total, alert, control, excluded, published, status, large, probes in self.db.execute(query):
                key = f"{window}:{horizon}"
                if total:
                    count = groups.setdefault(key, Counter())
                    state = status or ("pending" if published is not None else "unpublished")
                    count["all_feature_events"] += total
                    count[state] += total
                    for category, amount in (("alert", alert), ("control", control), ("excluded", excluded)):
                        if not amount:
                            continue
                        count[category] += amount
                        count[f"{category}_{state}"] += amount
                        if state == "scored":
                            label = p.confusion(category, {"price_move_at_least_threshold": bool(large)})
                            count["excluded_with_scored_pair" if label == "excluded" else label] += amount
                group = paired.setdefault(key, {"all_pair_jobs": 0, "pending": 0, "unknown": 0, "scored": 0, "probes": {}})
                group["all_pair_jobs"] += 1
                group[status or "pending"] += 1
                if status != "scored":
                    continue
                for name, score in json.loads(probes).items():
                    probe = group["probes"].setdefault(name, {"count": 0, "positive_after_spread": 0, "direction_correct": 0,
                                                             "net_bps_sum": Decimal(0), "stress_net_bps_sum": {cost: Decimal(0) for cost in p.PROTOCOL["cost_stress_bps"]}})
                    probe["count"] += 1
                    probe["positive_after_spread"] += int(score["positive_after_spread"])
                    probe["direction_correct"] += int(score["direction_correct"])
                    probe["net_bps_sum"] += Decimal(score["net_bps"])
                    for cost in p.PROTOCOL["cost_stress_bps"]:
                        probe["stress_net_bps_sum"][cost] += Decimal(score["stress_net_bps"][cost])
            for group in paired.values():
                for probe in group["probes"].values():
                    probe["mean_net_bps"] = probe["net_bps_sum"] / probe["count"]
        return {"schema_version": SCHEMA, "protocol": p.PROTOCOL, "protocol_sha256": p.PROTOCOL_SHA256,
                "counts": counts, "feature_event_counts": {key: dict(value) for key, value in groups.items()},
                "pair_probe_counts": p.exact.to_jsonable(paired),
                "last_refusal": self.last_refusal, "generated_epoch": self.now(),
                "resource_limits": {"max_ledger_and_sidecar_bytes": self.max_bytes, "minimum_free_bytes": self.minimum_free_bytes},
                "can_place_orders": False, "can_promote": False,
                "limitations": ["Exploratory magnitude thresholds; no inferred feature direction or calibrated forecast.",
                                "Overlapping features, windows and horizons share outcomes; counts are not independent trials.",
                                "Hypothetical bid/ask probes exclude financing, slippage, guaranteed fills and account returns.",
                                "Unavailable and unknown observations remain in separate full-denominator counts.",
                                "Finite ledger capacity; no automatic deletion or 48-hour capacity guarantee."]}
