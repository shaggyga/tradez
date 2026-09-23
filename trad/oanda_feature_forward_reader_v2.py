"""Bounded immutable forward-frame cache; unchanged feature and ranking math.

The twelve-frame reader cannot supply five wholly preceding five-minute
comparisons at one-minute cadence. Retain 32 verified frames and decode only
new frames, preserving their original feature/availability clocks. No source,
ledger, threshold, outcome or old category is rewritten by this module.
"""
from __future__ import annotations

from collections import OrderedDict
import bisect
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import json
from pathlib import Path
import time

import oanda_feature_move_mapping_v1 as mapper

VERSION = "feature_forward_verified_cache_v2_20260916"
FRAME_LIMIT = 32
MAX_NEW_FRAMES = 3
MAX_REFRESH_SECONDS = 8.0
MAX_EXPANDED_BYTES = 64 * 1024 * 1024
MAX_COMPRESSED_BYTES = 16 * 1024 * 1024
MAX_CACHE_BYTES = 64 * 1024 * 1024
MAX_FEATURE_VALUES = mapper.MAX_FEATURE_VALUES
MAX_DISCOVERY_FILES = 512
MAX_FRAME_IDENTITIES = 2048
MAX_ENDPOINT_AGE_AT_START_SECONDS = 40


def _hash(value):
    return hashlib.sha256(value).hexdigest()


def _safe(path):
    path = Path(path).absolute()
    if any(p.is_symlink() or (hasattr(p, "is_junction") and p.is_junction())
           for p in (path, *path.parents)):
        raise ValueError("linked_forward_source_refused")
    return path


def _identity(path):
    st = path.stat()
    return st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns


def _json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate_forward_json_key")
            result[key] = value
        return result
    return json.loads(raw, object_pairs_hook=pairs,
        parse_constant=lambda value: (_ for _ in ()).throw(ValueError("nonfinite_forward_json")))


def _read_small(path, limit=16384):
    path = _safe(path)
    before = _identity(path)
    if before[2] > limit:
        raise ValueError("forward_receipt_byte_bound")
    raw = path.read_bytes()
    if len(raw) > limit or _identity(path) != before:
        raise ValueError("forward_receipt_changed_during_read")
    return _json(raw), _hash(raw), before


def _receipt_path(archive_root, frame):
    stamp = mapper._epoch(frame.get("generated_utc"))
    if stamp is None:
        raise ValueError("forward_original_clock_required")
    at = datetime.fromtimestamp(stamp, timezone.utc)
    identity = dict(schema_version="feature_observation_archive_v1",
                    snapshot_id=frame["snapshot_id"], source_schema_id=frame["source_schema_id"])
    name = "obs_"+_hash(mapper._canonical(identity))[:32]+".json.publication.json"
    return archive_root/at.strftime("date=%Y%m%d")/at.strftime("hour=%H")/name


def read_verified_frame(path, archive_root, *, as_of_epoch):
    """Bind compressed bytes, decoded frame and external publication receipt."""
    path, archive_root = _safe(path), _safe(archive_root)
    before = _identity(path)
    if before[2] > MAX_COMPRESSED_BYTES:
        raise ValueError("compressed_forward_frame_bound")
    compressed = path.read_bytes()
    if len(compressed) > MAX_COMPRESSED_BYTES or _identity(path) != before:
        raise ValueError("forward_frame_changed_during_read")
    import io
    with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as stream:
        raw = stream.read(MAX_EXPANDED_BYTES+1)
    if len(raw) > MAX_EXPANDED_BYTES:
        raise ValueError("expanded_forward_frame_bound")
    envelope = _json(raw)
    if not isinstance(envelope, dict) or envelope.get("schema_version") != "feature_observation_forward_frame_v1":
        raise ValueError("forward_frame_schema_required")
    frame, identity = envelope.get("frame"), envelope.get("source_identity")
    if not isinstance(frame, dict) or not isinstance(identity, dict):
        raise ValueError("forward_frame_identity_required")
    if (identity != {k: frame.get(k) for k in ("snapshot_id", "source_schema_id")} or
        frame.get("source_payload_sha256") != envelope.get("payload_sha256")):
        raise ValueError("forward_frame_identity_mismatch")
    digest = _hash(mapper._canonical(frame))
    if digest != envelope.get("frame_sha256"):
        raise ValueError("forward_frame_digest_mismatch")
    if path.name != "forward_"+_hash(mapper._canonical(identity))[:32]+".json.gz":
        raise ValueError("forward_frame_filename_identity_mismatch")
    receipt_path = _receipt_path(archive_root, frame)
    receipt, receipt_sha, receipt_identity = _read_small(receipt_path)
    if (receipt.get("schema_version") != "feature_observation_publication_receipt_v1" or
        any(receipt.get(k) != frame.get(k) for k in ("snapshot_id", "source_schema_id")) or
        receipt.get("payload_sha256") != envelope["payload_sha256"] or
        receipt.get("frame_sha256") != digest or
        receipt.get("publication_scope") != "archive_visible_before_this_receipt"):
        raise ValueError("forward_external_publication_receipt_mismatch")
    relative = receipt.get("archive_relative_path")
    expected_archive = receipt_path.with_name(receipt_path.name.replace(".publication.json", ".gz"))
    if relative != expected_archive.relative_to(archive_root).as_posix():
        raise ValueError("forward_archive_receipt_path_mismatch")
    generated = mapper._epoch(frame.get("generated_utc"))
    published = mapper._epoch(receipt.get("publication_completed_utc"))
    source_read = mapper._epoch(receipt.get("source_read_completed_utc"))
    if (generated is None or published is None or source_read is None or
        not source_read <= generated <= published <= as_of_epoch):
        raise ValueError("forward_frame_unavailable_at_cutoff")
    original = datetime.fromtimestamp(generated, timezone.utc)
    if path.parent.name != original.strftime("hour=%H") or path.parent.parent.name != original.strftime("date=%Y%m%d"):
        raise ValueError("forward_frame_partition_clock_mismatch")
    # Validate each new frame once; the exact digest remains attached to cache.
    prepared, rejected = mapper._prepare_frames([frame], as_of_epoch)
    if len(prepared) != 1 or rejected:
        raise ValueError("forward_frame_shape_or_clock_invalid")
    count = sum(len(group.get("values") or {}) for pair in frame["instruments"].values()
                for group in (pair.get("groups") or {}).values())
    projected, size = mapper._project_verified_frame(frame)
    return dict(path=path, identity=before, compressed_sha256=_hash(compressed),
        frame=projected, frame_sha256=digest, generated_epoch=generated,
        publication_epoch=published, receipt_path=receipt_path,
        receipt_identity=receipt_identity, receipt_sha256=receipt_sha,
        expanded_bytes=len(raw), retained_bytes=size, feature_values=count)


class VerifiedFrameCache:
    def __init__(self, forward_root, archive_root, *, monotonic=time.monotonic,
                 frame_limit=FRAME_LIMIT, max_new_frames=MAX_NEW_FRAMES,
                 max_refresh_seconds=MAX_REFRESH_SECONDS):
        if type(frame_limit) is not int or not 16 <= frame_limit <= FRAME_LIMIT:
            raise ValueError("frame_cache_limit_16_to_32_required")
        if type(max_new_frames) is not int or not 1 <= max_new_frames <= MAX_NEW_FRAMES:
            raise ValueError("bounded_new_frame_count_required")
        if not isinstance(max_refresh_seconds, (int, float)) or not 0 < max_refresh_seconds <= MAX_REFRESH_SECONDS:
            raise ValueError("bounded_frame_refresh_time_required")
        self.root, self.archive_root = _safe(forward_root), _safe(archive_root)
        self.frame_limit, self.max_new_frames = frame_limit, max_new_frames
        self.monotonic, self.max_refresh_seconds = monotonic, max_refresh_seconds
        self.cache, self.identities = {}, OrderedDict()
        self.poisoned = None
        self.last_report = {}

    def _check_cached_sources(self):
        for item in self.cache.values():
            if _identity(_safe(item["path"])) != item["identity"]:
                raise ValueError("cached_forward_frame_revised")
            if _identity(_safe(item["receipt_path"])) != item["receipt_identity"]:
                raise ValueError("cached_forward_publication_receipt_revised")

    def _discover(self, now):
        files = []
        # Two hours is ample for a 32-minute dense cache; no whole archive walk.
        for offset in range(3):
            at = datetime.fromtimestamp(now, timezone.utc)-timedelta(hours=offset)
            hour = _safe(self.root/at.strftime("date=%Y%m%d")/at.strftime("hour=%H"))
            if not hour.is_dir():
                continue
            for path in hour.glob("forward_*.json.gz"):
                path = _safe(path)
                files.append((_identity(path)[3], path))
                if len(files) > MAX_DISCOVERY_FILES:
                    raise ValueError("forward_discovery_file_bound")
        files.sort(key=lambda x: (x[0], x[1].name), reverse=True)
        return [path for _, path in files[:self.frame_limit]]

    def refresh(self, *, as_of_epoch):
        now = mapper._epoch(as_of_epoch)
        if now is None:
            raise ValueError("explicit_cache_cutoff_required")
        if self.poisoned:
            raise ValueError(self.poisoned)
        started = self.monotonic()
        report = dict(reader_version=VERSION, as_of_epoch=now, files_read=0,
            expanded_bytes_read=0, compressed_bytes_read=0, errors=[], bounded_sample=True)
        try:
            self._check_cached_sources()
            files = self._discover(now)
            wanted = set(files)
            for key in list(self.cache):
                if key not in wanted:
                    self.cache.pop(key)
            for path in files:
                if path in self.cache:
                    continue
                if report["files_read"] >= self.max_new_frames or self.monotonic()-started >= self.max_refresh_seconds:
                    break
                try:
                    item = read_verified_frame(path, self.archive_root, as_of_epoch=now)
                except (OSError, ValueError, KeyError, TypeError, EOFError) as exc:
                    report["errors"].append(type(exc).__name__+":"+str(exc)[:180])
                    # Count attempts, so malformed archives cannot defeat the work bound.
                    report["files_read"] += 1
                    continue
                identity = (item["frame"]["source_schema_id"], item["frame"]["snapshot_id"])
                known = self.identities.get(identity)
                if known is not None and known != item["frame_sha256"]:
                    raise ValueError("cached_snapshot_identity_collision")
                self.identities[identity] = item["frame_sha256"]
                if len(self.identities) > MAX_FRAME_IDENTITIES:
                    self.identities.popitem(last=False)
                if (sum(v["retained_bytes"] for v in self.cache.values())+item["retained_bytes"] > MAX_CACHE_BYTES or
                    sum(v["feature_values"] for v in self.cache.values())+item["feature_values"] > MAX_FEATURE_VALUES):
                    report["errors"].append("retained_cache_byte_or_value_bound")
                    break
                self.cache[path] = item
                report["files_read"] += 1
                report["expanded_bytes_read"] += item["expanded_bytes"]
                report["compressed_bytes_read"] += item["identity"][2]
        except (OSError, ValueError) as exc:
            self.poisoned = "forward_cache_integrity_refused:"+str(exc)[:180]
            raise ValueError(self.poisoned) from exc
        report.update(cache_frames=len(self.cache), retained_projection_bytes=sum(v["retained_bytes"] for v in self.cache.values()),
            retained_feature_values=sum(v["feature_values"] for v in self.cache.values()),
            elapsed_seconds=self.monotonic()-started,
            selection_stop="bounded_verified_cache_32_original_clock_frames",
            cache_missing_frames=sum(path not in self.cache for path in files))
        self.last_report = report
        return report

    def readiness(self, *, as_of_epoch):
        """Conservative work headroom; never expands the 75-second final gate."""
        now = mapper._epoch(as_of_epoch)
        if now is None:
            raise ValueError("explicit_cache_readiness_clock_required")
        items = sorted((v for v in self.cache.values() if v["publication_epoch"] <= now),
                       key=lambda v: (v["generated_epoch"], v["frame_sha256"]))
        if self.poisoned or not items:
            return dict(ready=False, reason="cache_integrity_or_history_unavailable", prior_clock_comparisons=0)
        last = items[-1]
        clocks = [v["generated_epoch"] for v in items]
        baseline = bisect.bisect_right(clocks, clocks[-1]-300)-1
        comparisons = 0
        schema = last["frame"]["source_schema_id"]
        if baseline >= 0 and clocks[-1]-300-clocks[baseline] <= 75:
            for index in range(baseline+1):
                previous = bisect.bisect_right(clocks, clocks[index]-300)-1
                if (previous >= 0 and clocks[index]-300-clocks[previous] <= 75 and
                    items[index]["frame"]["source_schema_id"] == schema and
                    items[previous]["frame"]["source_schema_id"] == schema):
                    comparisons += 1
        age = now-clocks[-1]
        reason = "insufficient_prior_clock_history" if comparisons < 5 else (
            "awaiting_fresher_endpoint_for_processing_headroom" if not 0 <= age <= MAX_ENDPOINT_AGE_AT_START_SECONDS else None)
        return dict(ready=reason is None, reason=reason, prior_clock_comparisons=comparisons,
            latest_source_epoch=clocks[-1], source_age_seconds=age,
            maximum_start_age_seconds=MAX_ENDPOINT_AGE_AT_START_SECONDS,
            final_source_age_limit_seconds=75,
            scope="clock support only; each feature still needs five valid prior deltas")

    def __call__(self, archive_root, *, as_of_utc, window_secs=(300,), instrument=None,
                 include_all_comparisons=True, reference_only=True, forward_frame_only=True, **kwargs):
        if _safe(archive_root) != self.root or tuple(window_secs) != (300,) or instrument is not None:
            raise ValueError("unfiltered_M5_bound_forward_cache_required")
        if not include_all_comparisons or not reference_only or not forward_frame_only or kwargs:
            raise ValueError("complete_forward_population_required")
        if self.poisoned:
            raise ValueError(self.poisoned)
        self._check_cached_sources()
        now = mapper._epoch(as_of_utc)
        if now is None:
            raise ValueError("explicit_mapping_cutoff_required")
        # Values/hashes were validated at admission. Availability is checked anew.
        items = sorted((item for item in self.cache.values() if item["publication_epoch"] <= now),
                       key=lambda item: (item["generated_epoch"], item["frame_sha256"]))
        frames = [item["frame"] for item in items]
        prepared = ([(item["generated_epoch"], item["frame_sha256"], item["frame"]) for item in items], {})
        result = mapper._build_feature_move_map(frames, as_of_utc=as_of_utc, window_sec=300,
            include_all_comparisons=True, prepared=prepared)
        result["archive"] = {**self.last_report, "mapping_cutoff_epoch": now,
            "reader_version": VERSION, "files_read": self.last_report.get("files_read", 0),
            "source_frame_receipts": [dict(path=str(item["path"]), frame_sha256=item["frame_sha256"],
                compressed_sha256=item["compressed_sha256"], publication_epoch=item["publication_epoch"],
                publication_receipt_path=str(item["receipt_path"]), publication_receipt_sha256=item["receipt_sha256"])
                for item in items],
            "history_semantics": "unchanged mapper: prior overlapping comparisons wholly preceding current M5 window; not independent trials",
            "future_or_late_published_frames_excluded": len(self.cache)-len(items)}
        return {300: result}
