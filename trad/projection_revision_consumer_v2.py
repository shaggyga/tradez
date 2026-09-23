"""Incremental validation of the original immutable consumer receipt history.

V1 receipt, source, clock, publication and coverage checks remain unchanged.
The new contract bounds expansion *per step*, not the sum of historical scans
re-expanded on every call. A bootstrap step never returns a current context.
Only a subsequent fresh complete database capture can expose a context.

The optional durable cache stores exact raw inventory and progress diagnostics,
not trusted semantic attestations. After process restart every receipt is
validated again in bounded steps. An incomplete/failed suffix never makes the
previous context current. No original consumer/publication database is written.
The separately bounded frozen publisher reader still owns publication proofs.
"""
from __future__ import annotations

import copy
import base64
import hashlib
import json
from pathlib import Path
import sqlite3
import threading
import time

import projection_revision_consumer_v1 as legacy

publisher = legacy.publisher
reader = legacy.reader
compact = legacy.compact
need = legacy.need
SCHEMA = "incremental_original_consumer_validation_v2_20260916"
MAX_STEP_BYTES = 64 * 1024 * 1024
MAX_STEP_SECONDS = 20.0
MAX_STEP_RECEIPTS = 64
MAX_CACHE_BYTES = 192 * 1024 * 1024
MAX_CACHE_PAYLOAD = 128 * 1024 * 1024
MAX_INVENTORY_ROWS = legacy.MAX_OBSERVATIONS * 2 + legacy.MAX_SCAN_OBJECTS
CACHE_SQL = """
CREATE TABLE cache_profile(id INTEGER PRIMARY KEY CHECK(id=1),body BLOB NOT NULL);
CREATE TABLE inventory(kind TEXT NOT NULL,key TEXT NOT NULL,body BLOB NOT NULL,PRIMARY KEY(kind,key));
CREATE TABLE progress(id INTEGER PRIMARY KEY CHECK(id=1),body BLOB NOT NULL);
CREATE TABLE inventory_seal(id INTEGER PRIMARY KEY CHECK(id=1),body BLOB NOT NULL);
"""


class BootstrapPending(ValueError):
    """No current context is available; call another bounded step later."""
    def __init__(self, progress):
        super().__init__(progress["status"])
        self.progress = copy.deepcopy(progress)


def _inventory_seal(rows):
    """Detect cache truncation/corruption; never attest semantic validation."""
    digest = hashlib.sha256()
    count = size = 0
    for kind, key, body in rows:
        body = bytes(body)
        digest.update(reader.encoded([kind, key, len(body)]))
        digest.update(body)
        count += 1
        size += len(body)
        need(count <= MAX_INVENTORY_ROWS and size <= MAX_CACHE_PAYLOAD, "validation_cache_inventory_bound")
    return {"sha256": digest.hexdigest(), "rows": count, "bytes": size}


def operations_profile_for(publication_path, observation_path, *, cohort_id,
                           consumer_id, expected_policy, input_identity):
    """Pin the same operational reader generation in transport and joint IO.

    The historical named consumer/store identity is unchanged. A process-local
    cache path is deliberately excluded from this shared operational profile.
    """
    publication_path = publisher.path_for(publication_path)
    observation_path = publisher.path_for(observation_path)
    publisher.policy_current(expected_policy)
    modules = (legacy, publisher, compact, reader)
    bindings = {Path(module.__file__).name: hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
                for module in modules}
    bindings[Path(__file__).name] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    return {"schema_version": SCHEMA, "cohort_id": cohort_id, "consumer_id": consumer_id,
            "publication_path": str(publication_path), "observation_path": str(observation_path),
            "publication_identity": list(publisher.identity(publication_path)),
            "observation_identity": list(publisher.identity(observation_path)),
            "policy_sha256": reader.sha(expected_policy), "input_identity": copy.deepcopy(input_identity),
            "source_bindings": bindings, "original_receipt_contract": legacy.SCHEMA,
            "cache_restart_policy": "raw_inventory_only; revalidate_semantics_before_current_context",
            "history_work_contract": "complete_original_history; bounded_expansion_per_step",
            **reader.INERT}


class IncrementalReader:
    """One owner, bounded disk cache, complete immutable prefix validation.

    Each step captures every original observation, acknowledgment and scan
    object, including unacknowledged observations, under a short read lock.
    All previously captured rows must match exactly. Only newly acknowledged
    receipts undergo semantic replay. A late acknowledgment inserted before
    the validated order causes nonauthorizing rebootstrap, preserving original
    first-consumer knowledge instead of appending a synthetic new clock.

    Publisher proof preparation has its own frozen cold/warm bounds. Consumer
    work obeys byte/document bounds and cooperative time checks between bounded
    documents; no semantic validation runs while holding a source read lock.
    """
    def __init__(self, publication_path, observation_path, *, cohort_id, consumer_id,
                 expected_policy, input_identity, cache_path, operations_profile=None,
                 max_step_bytes=MAX_STEP_BYTES, max_step_seconds=MAX_STEP_SECONDS,
                 max_step_receipts=MAX_STEP_RECEIPTS):
        need(type(max_step_bytes) is int and legacy.MAX_DOCUMENT <= max_step_bytes <= MAX_STEP_BYTES,
             "bounded_step_expansion_required")
        need(type(max_step_receipts) is int and 1 <= max_step_receipts <= MAX_STEP_RECEIPTS,
             "bounded_step_receipt_count_required")
        need(type(max_step_seconds) in (int, float) and 0 < max_step_seconds <= 30,
             "bounded_step_seconds_required")
        self.arguments = {"cohort_id": cohort_id, "consumer_id": consumer_id,
                          "expected_policy": copy.deepcopy(expected_policy), "input_identity": copy.deepcopy(input_identity)}
        self.publication_path = publisher.path_for(publication_path)
        self.observation_path = publisher.path_for(observation_path)
        self.profile = operations_profile_for(self.publication_path, self.observation_path, **self.arguments)
        if operations_profile is not None:
            need(reader.encoded(operations_profile) == reader.encoded(self.profile), "exact_incremental_reader_profile_required")
        self.cache_path = publisher.path_for(cache_path, missing=True)
        need(self.cache_path not in (self.publication_path, self.observation_path,
                                    Path(input_identity["path"])), "separate_validation_cache_required")
        self.max_step_bytes, self.max_step_seconds = max_step_bytes, float(max_step_seconds)
        self.max_step_receipts = max_step_receipts
        self.lock = threading.RLock()
        self.cache = self._open_cache()
        self.inventory = {kind: {} for kind in ("observations", "acknowledgments", "scan_objects")}
        for kind, key, body in self.cache.execute("SELECT kind,key,body FROM inventory"):
            need(kind in self.inventory, "cache_inventory_kind_invalid")
            self.inventory[kind][key] = bytes(body)
        self.frames = {}
        self.validated = []
        self.publication_heads = []
        self.complete = False
        self.failed = False
        self.capture_count = 0
        self.last_progress = None

    def _open_cache(self):
        fresh = not self.cache_path.exists()
        if fresh:
            with self.cache_path.open("xb"):
                pass
        need(self.cache_path.stat().st_size <= MAX_CACHE_BYTES, "validation_cache_disk_bound")
        con = sqlite3.connect(self.cache_path, timeout=2, check_same_thread=False)
        try:
            con.execute("PRAGMA synchronous=FULL")
            need(con.execute("PRAGMA journal_mode").fetchone()[0] == "delete", "validation_cache_delete_journal_required")
            page = con.execute("PRAGMA page_size").fetchone()[0]
            con.execute("PRAGMA max_page_count=" + str(MAX_CACHE_BYTES // page))
            con.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, MAX_CACHE_PAYLOAD)
            if fresh:
                con.executescript(CACHE_SQL)
                with con:
                    con.execute("INSERT INTO cache_profile VALUES(1,?)", (reader.encoded(self.profile),))
                    con.execute("INSERT INTO inventory_seal VALUES(1,?)", (reader.encoded(_inventory_seal(())),))
            tables = {row[0] for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            need(tables == {"cache_profile", "inventory", "progress", "inventory_seal"}, "exact_validation_cache_schema_required")
            reference = sqlite3.connect(":memory:")
            try:
                reference.executescript(CACHE_SQL)
                query = "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE type IN ('table','index','trigger') ORDER BY type,name"
                need(con.execute(query).fetchall() == reference.execute(query).fetchall(), "exact_validation_cache_schema_required")
            finally:
                reference.close()
            rows = con.execute("SELECT id,body FROM cache_profile").fetchall()
            need(len(rows) == 1 and rows[0] == (1, reader.encoded(self.profile)), "validation_cache_profile_changed")
            count, size = con.execute("SELECT COUNT(*),COALESCE(SUM(length(body)),0) FROM inventory").fetchone()
            need(count <= MAX_INVENTORY_ROWS and size <= MAX_CACHE_PAYLOAD, "validation_cache_inventory_bound")
            seal = con.execute("SELECT id,body FROM inventory_seal").fetchall()
            actual = _inventory_seal(con.execute("SELECT kind,key,body FROM inventory ORDER BY kind,key"))
            need(seal == [(1, reader.encoded(actual))], "validation_cache_inventory_corrupt")
            return con
        except BaseException:
            con.close()
            raise

    def close(self):
        with self.lock:
            self.cache.close()

    def _capture(self, publication, deadline):
        profile = legacy.profile_for(self.observation_path, self.arguments["consumer_id"], publication)
        ident = tuple(self.profile["observation_identity"])
        def capture(short_deadline):
            result = {}
            total = 0
            with legacy.connection(self.observation_path, short_deadline, ident=ident) as con:
                legacy.schema_and_profile(con, profile)
                for kind, ordering in (("observations", "observation_sequence"),
                                       ("acknowledgments", "observation_id"), ("scan_objects", "object_sha")):
                    rows = []
                    for row in con.execute("SELECT * FROM " + kind + " ORDER BY " + ordering):
                        need(time.monotonic() <= short_deadline, "consumer_packed_capture_time_bound")
                        values = tuple(row)
                        need(all(type(v) in (str, int) for v in values), "consumer_immutable_primitive_required")
                        total += sum(len(v.encode()) if type(v) is str else 8 for v in values)
                        need(total <= legacy.MAX_JSON, "consumer_packed_capture_byte_bound")
                        rows.append(values)
                    result[kind] = tuple(rows)
            return result, total
        (result, retained), attempts = publisher._capture_with_retry(capture, deadline)
        self.capture_count += 1
        return profile, result, retained, attempts

    def _check_inventory(self, capture):
        present = {kind: {str(row[0]): reader.encoded(row) for row in rows} for kind, rows in capture.items()}
        new = []
        for kind, old in self.inventory.items():
            need(all(present[kind].get(key) == body for key, body in old.items()),
                 "verified_consumer_inventory_changed:" + kind)
            new.extend((kind, key, body) for key, body in present[kind].items() if key not in old)
        seal = _inventory_seal((kind, key, body) for kind, rows in sorted(present.items())
                               for key, body in sorted(rows.items()))
        # Cache writes cannot attest validation. Retaining the entire capture,
        # including unvalidated suffixes, prevents later deletion from hiding a
        # failed original record. Cache state and source evidence are separate.
        with self.cache:
            self.cache.executemany("INSERT INTO inventory VALUES(?,?,?)", new)
            self.cache.execute("UPDATE inventory_seal SET body=? WHERE id=1", (reader.encoded(seal),))
        self.inventory = present

    def _check_generation(self):
        actual = operations_profile_for(self.publication_path, self.observation_path, **self.arguments)
        need(reader.encoded(actual) == reader.encoded(self.profile), "incremental_reader_generation_changed")

    @staticmethod
    def _snapshot_size(receipt, profile, publication_frames, scan_frames):
        scan = receipt["completed_scan"]
        need(type(scan) is dict and set(scan) == {"schema_version", "snapshot_core", "evidence_refs"}
             and scan["schema_version"] == legacy.SCAN_SCHEMA, "typed_compact_completed_scan_required")
        core, refs = scan["snapshot_core"], scan["evidence_refs"]
        need(type(core) is dict and not set(core) & {"policy", "input_identity", "entries"}
             and type(refs) is list and len(refs) <= reader.MAX_ROWS, "compact_scan_reference_bound")
        base = {**core, "policy": profile["publication_profile"]["policy"],
                "input_identity": profile["publication_profile"]["input_identity"], "entries": []}
        size = len(reader.encoded(base)) + max(0, len(refs) - 1)
        for ref in refs:
            need(type(ref) is dict and len(ref) == 1, "exact_scan_reference_required")
            field, key = next(iter(ref.items()))
            need(field in ("publication_evidence_sha256", "stored_evidence_sha256") and type(key) is str,
                 "scan_object_sha_required")
            frames = publication_frames if field == "publication_evidence_sha256" else scan_frames
            need(key in frames, "scan_object_missing")
            size += frames[key][0]
        need(size <= legacy.MAX_DOCUMENT, "expanded_scan_document_bound")
        return size

    def _progress(self, status, capture, work, count, started, retained, attempts):
        result = {"schema_version": SCHEMA, "status": status, "validated_observations": len(self.validated),
                  "captured_observations": count, "captured_original_rows": sum(map(len, capture.values())),
                  "step_expanded_bytes": work, "historical_expanded_bytes": sum(item[4] for item in self.validated),
                  "consumer_step_seconds": time.monotonic() - started, "packed_retained_bytes": retained,
                  "capture_count": self.capture_count, "capture_attempts": attempts,
                  "current_context_available": False, "cache_is_semantic_authority": False,
                  "original_availability_unchanged": True, **reader.INERT}
        with self.cache:
            self.cache.execute("INSERT OR REPLACE INTO progress VALUES(1,?)", (reader.encoded(result),))
        self.last_progress = result
        return copy.deepcopy(result)

    def _context(self, publication, profile, objects, progress):
        """Assemble V1's opaque indexes from already validated original rows."""
        entries = [item[1] for item in self.validated]
        owned = {"schema_version": legacy.SCHEMA, "profile": profile, "publication_integrity": publication,
                 "observations": entries, "scan_objects": objects, "consumer_observation_readback_proven": True,
                 "joint_model_consumption_proven": False, **reader.INERT}
        manifest = legacy._manifest_from_owned(owned)
        _, readback_sha, readback_bytes = legacy.bounded_owned(manifest)
        first = [None] * (len(publication["batches"]) + 1)
        assigned, rows = 0, []
        for _, item, observed, coverage, _ in self.validated:
            head = item["observation"]["capture"]["head"]["sequence"]
            while assigned < head:
                assigned += 1
                first[assigned] = observed
            rows.append((observed, head, reader.encoded(coverage)))
        versions, headers = {}, {}
        member_bytes = compressed_bytes = 0
        for event, sequence, body in compact.member_records(publication):
            member_bytes += body[0]
            compressed_bytes += len(body[2])
            need(member_bytes <= compact.MAX_INDEX_EXPANDED and compressed_bytes <= compact.MAX_INDEX_COMPRESSED,
                 "prepared_member_byte_bound")
            versions.setdefault(event, []).append((sequence, body))
        frozen = tuple((event, tuple(seq for seq, _ in values), tuple(body for _, body in values))
                       for event, values in sorted(versions.items()))
        metadata = {"schema_version": legacy.CONTEXT_SCHEMA, "readback_sha256": readback_sha,
                    "readback_bytes": readback_bytes, "consumer_profile": profile,
                    "publication_profile": publication["profile"],
                    "source_bindings": {"consumer": profile["consumer_sha256"], "publisher": profile["publisher_sha256"],
                        "compact_store": self.profile["source_bindings"][Path(compact.__file__).name],
                        "reader": self.profile["source_bindings"][Path(reader.__file__).name]},
                    "validation_scope": "complete_supplied_history_validated_not_database_capture_authenticated",
                    "observation_count": len(rows), "publication_count": len(publication["batches"]),
                    "expanded_scan_bytes_checked": progress["historical_expanded_bytes"],
                    "member_bytes": member_bytes, "compressed_member_bytes": compressed_bytes,
                    "expanded_evidence_bytes_checked": compact.manifest(publication)["expanded_evidence_bytes_verified"],
                    "manifest_bytes": readback_bytes, "manifest_sha256": readback_sha,
                    "readback_digest_scope": "compact_manifest_not_expanded_legacy_readback",
                    "consumer_receipt_max_epoch": max((item["acknowledgment"]["receipt_observed_epoch"] for item in entries), default=None),
                    "evidence_encoding": compact.SCHEMA, "semantic_complete_validations": 1,
                    "incremental_validation": {"schema_version": SCHEMA, "operations_profile_sha256": reader.sha(self.profile),
                        "step_expanded_bytes": progress["step_expanded_bytes"], "historical_expanded_bytes": progress["historical_expanded_bytes"],
                        "fresh_original_capture_count": self.capture_count, "cache_is_semantic_authority": False}}
        context = legacy._make_prepared((legacy._CONTEXT_KEY, reader.encoded(metadata), profile["consumer_id"],
                                        tuple(row[0] for row in rows), tuple(rows), tuple(first), frozen))
        legacy._recipe_capture[context] = (publication, reader.encoded({key: value for key, value in owned.items()
                                        if key not in ("publication_integrity", "scan_objects")}), objects)
        for event, sequence, body in compact.header_records(publication):
            headers.setdefault(event, []).append((sequence, body))
        legacy._context_headers[context] = tuple((event, tuple(seq for seq, _ in values), tuple(body for _, body in values))
                                                for event, values in sorted(headers.items()))
        legacy._context_event_ids[context] = tuple(value[0] for value in frozen)
        return context

    def _step(self, authorize):
        with self.lock:
            had_complete_prefix = self.complete and not self.failed
            self.complete = False
            self.failed = False
            try:
                self._check_generation()
                publication = publisher.read_published(self.publication_path, **{key: value for key, value in self.arguments.items()
                                                       if key != "consumer_id"})
                heads = publication["publication_heads"]
                need(heads[:len(self.publication_heads)] == self.publication_heads,
                     "verified_publication_prefix_changed")
                self.publication_heads = heads
                started = time.monotonic()
                deadline = started + self.max_step_seconds
                profile, capture, retained, attempts = self._capture(publication, deadline)
                self._check_inventory(capture)
                acknowledgments = {row[0]: row for row in capture["acknowledgments"]}
                all_rows = [(row, acknowledgments[row[1]]) for row in capture["observations"] if row[1] in acknowledgments]
                # A late acknowledgment can fill an earlier gap. Its original
                # time must be replayed in order, never appended as new knowledge.
                if [item[0] for item in self.validated] != all_rows[:len(self.validated)]:
                    self.validated = []
                    had_complete_prefix = False
                work, processed = 0, 0
                for key, body in capture["scan_objects"]:
                    if key in self.frames:
                        continue
                    value = publisher.checked_json(body)
                    size = value.get("expanded_bytes")
                    need(type(size) is int and 0 <= size <= compact.MAX_OBJECT, "typed_scan_cas_required")
                    if work + size > self.max_step_bytes or time.monotonic() >= deadline:
                        break
                    frame = legacy.decode_frame(body)
                    need(frame[1] == key, "stored_scan_object_digest_invalid")
                    self.frames[key] = frame
                    work += size
                objects_complete = len(self.frames) == len(capture["scan_objects"])
                objects = compact.owned_evidence(self.frames)
                if objects_complete:
                    index = legacy._publication_index(publication)
                    published_frames = compact.evidence_frames(publication)
                    for packed in all_rows[len(self.validated):]:
                        if processed >= self.max_step_receipts or time.monotonic() >= deadline:
                            break
                        row, ackrow = packed
                        sequence, oid, receipt_sha, receipt_body = row
                        _, ack_sha, ack_body = ackrow
                        receipt, ack = publisher.checked_json(receipt_body), publisher.checked_json(ack_body)
                        need(legacy.sha(receipt) == receipt_sha and legacy.sha(ack) == ack_sha and receipt["observation_id"] == oid,
                             "consumer_stored_digest_invalid")
                        need(type(receipt["observation_sequence"]) is int and receipt["observation_sequence"] == sequence,
                             "consumer_stored_sequence_invalid")
                        expected_size = self._snapshot_size(receipt, profile, published_frames, self.frames)
                        if work + expected_size > self.max_step_bytes:
                            break
                        item = {"observation": receipt, "acknowledgment": ack,
                                "receipt_sha256": receipt_sha, "ack_sha256": ack_sha}
                        observed, coverage, size = legacy._validate_observation(receipt, ack, profile, index, objects)
                        need(size == expected_size, "exact_expanded_scan_size_required")
                        if self.validated:
                            prior = self.validated[-1][1]["observation"]
                            need(sequence > prior["observation_sequence"] and observed >= prior["consumer_observed_epoch"]
                                 and receipt["capture"]["head"]["sequence"] >= prior["capture"]["head"]["sequence"],
                                 "consumer_supplied_observation_order")
                        self.validated.append((packed, item, observed, coverage, size))
                        work += size
                        processed += 1
                self._check_generation()
                done = objects_complete and len(self.validated) == len(all_rows)
                # A document may finish after the cooperative time boundary.
                # Its checked progress is reusable, but cannot authorize now.
                timely = time.monotonic() <= deadline
                self.complete = done and timely
                status = "prefix_verified_requires_fresh_read" if self.complete else "bootstrapping"
                progress = self._progress(status, capture, work, len(all_rows), started, retained, attempts)
                if authorize and had_complete_prefix and self.complete:
                    context = self._context(publication, profile, objects, progress)
                    need(time.monotonic() <= deadline, "consumer_context_assembly_time_bound")
                    self._check_generation()
                    return context
                if authorize:
                    raise BootstrapPending(progress)
                return progress
            except BootstrapPending:
                raise
            except BaseException:
                self.complete = False
                self.failed = True
                raise

    def bootstrap_step(self):
        return self._step(False)

    def read_observations(self):
        return self._step(True)

    read_and_prepare = read_observations


class ManifestReplay:
    """Bounded original archived-proof replay; never an actual current read.

    Publisher proofs use the unchanged bounded publication restore. Consumer
    scans then resume across steps without the legacy cumulative 512 MiB work
    refusal. The manifest is owned once and every streamed object is matched to
    its exact hash/size before use. No live database or validation cache opens.
    """
    def __init__(self, manifest, objects, *, max_step_bytes=MAX_STEP_BYTES,
                 max_step_seconds=MAX_STEP_SECONDS, max_step_receipts=MAX_STEP_RECEIPTS):
        need(type(max_step_bytes) is int and legacy.MAX_DOCUMENT <= max_step_bytes <= MAX_STEP_BYTES,
             "bounded_step_expansion_required")
        need(type(max_step_receipts) is int and 1 <= max_step_receipts <= MAX_STEP_RECEIPTS,
             "bounded_step_receipt_count_required")
        need(type(max_step_seconds) in (int, float) and 0 < max_step_seconds <= 30,
             "bounded_step_seconds_required")
        owned, _, _ = legacy.bounded_owned(manifest)
        need(type(owned) is dict and set(owned) == {"schema_version", "publication", "consumer", "scan_objects"}
             and owned["schema_version"] == legacy.MANIFEST_SCHEMA, "typed_consumer_manifest_required")
        self.stream = iter(objects)
        self.publication = publisher.restore_publication_manifest(owned["publication"], self.stream)
        consumer = owned["consumer"]
        need(type(consumer) is dict and not set(consumer) & {"publication_integrity", "scan_objects"},
             "exact_consumer_manifest_required")
        need(consumer["schema_version"] == legacy.SCHEMA and consumer["consumer_observation_readback_proven"] is True,
             "typed_consumer_readback_required")
        profile = consumer["profile"]
        need(legacy.exact(profile["publication_profile"], self.publication["profile"]), "consumer_publication_profile_mismatch")
        need(profile["schema_version"] == legacy.SCHEMA
             and profile["consumer_sha256"] == hashlib.sha256(Path(legacy.__file__).read_bytes()).hexdigest()
             and profile["publisher_sha256"] == self.publication["profile"]["publisher_sha256"],
             "current_consumer_publisher_generation_required")
        need(type(profile["max_scan_age_sec"]) is int and profile["max_scan_age_sec"] == legacy.MAX_SCAN_AGE_SEC
             and profile["scan_age_basis"] == "source_read_started_epoch", "exact_coverage_policy_required")
        entries = consumer["observations"]
        need(type(entries) is list and len(entries) <= legacy.MAX_OBSERVATIONS
             and type(owned["scan_objects"]) is list and len(owned["scan_objects"]) <= legacy.MAX_SCAN_OBJECTS,
             "consumer_observation_list_bound")
        for item in entries:
            reader._integer(item["observation"]["observation_sequence"], "consumer_sequence_integer", 1, legacy.MAX_OBSERVATIONS)
        self.entries = sorted(entries, key=lambda item: item["observation"]["observation_sequence"])
        need(len({item["observation"]["observation_id"] for item in entries}) == len(entries)
             and len({item["observation"]["observation_sequence"] for item in entries}) == len(entries),
             "original_unique_archived_observations_required")
        self.descriptors = owned["scan_objects"]
        self.profile = profile
        self.index = legacy._publication_index(self.publication)
        self.publication_frames = compact.evidence_frames(self.publication)
        self.frames, self.validated = {}, []
        self.max_step_bytes, self.max_step_receipts = max_step_bytes, max_step_receipts
        self.max_step_seconds = float(max_step_seconds)
        self.complete = False
        self.failed = False
        self._result = None
        self.steps = 0

    def step(self):
        need(not self.failed, "archived_replay_failed_restart_required")
        started = time.monotonic()
        deadline = started + self.max_step_seconds
        work = 0
        try:
            publisher.policy_current(self.publication["profile"]["policy"])
            for descriptor in self.descriptors[len(self.frames):]:
                need(type(descriptor) is list and len(descriptor) == 4, "exact_ordered_scan_manifest_required")
                key, size, packed_sha, packed_size = descriptor
                need(type(key) is str and reader.re.fullmatch("[0-9a-f]{64}", key)
                     and (not self.frames or key > next(reversed(self.frames)))
                     and type(size) is int and 0 <= size <= compact.MAX_OBJECT,
                     "ordered_unique_scan_manifest_required")
                if work + size > self.max_step_bytes or time.monotonic() >= deadline:
                    break
                item = next(self.stream, None)
                need(type(item) is dict and set(item) == {"kind", "object_sha", "expanded_bytes", "packed_sha", "zlib_base64"}
                     and item["kind"] == "scan" and item["object_sha"] == key and item["expanded_bytes"] == size
                     and item["packed_sha"] == packed_sha, "exact_ordered_scan_object_required")
                encoded = item["zlib_base64"]
                need(type(encoded) is str and len(encoded) <= legacy.MAX_DOCUMENT * 4 // 3 + 4, "scan_frame_bound")
                packed = base64.b64decode(encoded, validate=True)
                need(type(packed_size) is int and len(packed) == packed_size and hashlib.sha256(packed).hexdigest() == packed_sha,
                     "scan_frame_digest_invalid")
                frame = size, key, packed
                compact.expand(frame)
                self.frames[key] = frame
                work += size
            objects_complete = len(self.frames) == len(self.descriptors)
            objects = compact.owned_evidence(self.frames)
            if objects_complete:
                need(next(self.stream, None) is None, "unexpected_extra_recipe_object")
                processed = 0
                for item in self.entries[len(self.validated):]:
                    if processed >= self.max_step_receipts or time.monotonic() >= deadline:
                        break
                    receipt, ack = item["observation"], item["acknowledgment"]
                    need(legacy.sha(receipt) == item["receipt_sha256"] and legacy.sha(ack) == item["ack_sha256"],
                         "consumer_supplied_digest_invalid")
                    size = IncrementalReader._snapshot_size(receipt, self.profile, self.publication_frames, self.frames)
                    if work + size > self.max_step_bytes:
                        break
                    observed, coverage, actual = legacy._validate_observation(receipt, ack, self.profile, self.index, objects)
                    need(actual == size, "exact_expanded_scan_size_required")
                    if self.validated:
                        prior = self.validated[-1][1]["observation"]
                        need(receipt["observation_sequence"] > prior["observation_sequence"]
                             and observed >= prior["consumer_observed_epoch"]
                             and receipt["capture"]["head"]["sequence"] >= prior["capture"]["head"]["sequence"],
                             "consumer_supplied_observation_order")
                    self.validated.append((None, item, observed, coverage, size))
                    work += size
                    processed += 1
            self.steps += 1
            self.complete = objects_complete and len(self.validated) == len(self.entries) and time.monotonic() <= deadline
            return {"schema_version": SCHEMA, "status": "historical_replay_complete" if self.complete else "bootstrapping",
                    "validated_observations": len(self.validated), "captured_observations": len(self.entries),
                    "historical_expanded_bytes": sum(item[4] for item in self.validated), "step_expanded_bytes": work,
                    "current_context_available": False, "actual_database_capture_performed": False, **reader.INERT}
        except BaseException:
            self.complete = False
            self.failed = True
            raise

    def prepared_context(self):
        need(self.complete and not self.failed, "complete_archived_replay_required")
        if self._result is None:
            # Reuse only the already-validated index assembler, never a live
            # capture method. These metadata extras disclose replay provenance.
            helper = object.__new__(IncrementalReader)
            helper.validated = self.validated
            helper.capture_count = 0
            helper.profile = {"schema_version": SCHEMA, "scope": "archived_semantic_replay_only",
                "source_bindings": {Path(module.__file__).name: hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
                                    for module in (legacy, publisher, compact, reader)}}
            helper.profile["source_bindings"][Path(__file__).name] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
            self._result = helper._context(self.publication, self.profile, compact.owned_evidence(self.frames),
                {"historical_expanded_bytes": sum(item[4] for item in self.validated), "step_expanded_bytes": 0})
        return self._result


def restore_manifest(manifest, objects, *, max_total_seconds=330, progress_callback=None):
    """Historical-only convenience driver; each replay step remains bounded."""
    need(type(max_total_seconds) in (int, float) and 0 < max_total_seconds <= 330,
         "bounded_archived_replay_duration_required")
    started = time.monotonic()
    replay = ManifestReplay(manifest, objects)
    while True:
        need(time.monotonic() - started <= max_total_seconds, "archived_replay_total_time_bound")
        progress = replay.step()
        if progress_callback is not None:
            progress_callback(copy.deepcopy(progress))
        if replay.complete:
            result = replay.prepared_context()
            need(time.monotonic() - started <= max_total_seconds, "archived_replay_total_time_bound")
            return result


# Existing consumers can retain the original pure selection and archive APIs.
# Large archives use ManifestReplay/restore_manifest; arbitrary V1 recipe
# imports do not inherit this process's validated prefix.
PreparedConsumer = legacy.PreparedConsumer
prepared_context_metadata = legacy.prepared_context_metadata
latest_consumed_from_context = legacy.latest_consumed_from_context
latest_consumed_as_of = legacy.latest_consumed_as_of
selected_timing = legacy.selected_timing
iter_selected_headers = legacy.iter_selected_headers
resolve_selected_member = legacy.resolve_selected_member
resolve_evidence = legacy.resolve_evidence
export_recipe = legacy.export_recipe
export_manifest = legacy.export_manifest
iter_recipe_objects = legacy.iter_recipe_objects
prepare_consumed_context = legacy.prepare_consumed_context
observe_published = legacy.observe_published
