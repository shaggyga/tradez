"""One-time exact-byte capture for the isolated MXN INEGI CPI V1 cohort.

This utility is intentionally not imported by the live collector.  It fetches a
small, frozen set of official INEGI resources, verifies every expected byte
count and SHA-256 before writing anything, extracts PDF text with the pinned
primary PDF runtime, and atomically retains the resulting artifacts as
read-only files.  The obsolete ``calendar.xml`` endpoint is retained only as a
quarantined later observation; it is not an authoritative calendar input.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import io
import json
import os
import stat
import sys
import unicodedata
import urllib.request
from pathlib import Path
from typing import Any

import pypdf


UTC = dt.timezone.utc
WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
ARCHIVE_DIR = (
    WORKSPACE_ROOT
    / "data"
    / "oanda_training_manager"
    / "source_archives"
    / "mxn_inegi_cpi_exact_v1"
)
MANIFEST_PATH = ARCHIVE_DIR / "source_observation_manifest_v1.json"
EXTRACTOR_ID = "pypdf_6_15_0_layout_nfc_lf_rstrip_formfeed_v1"


SOURCES: tuple[dict[str, Any], ...] = (
    {
        "artifact_id": "calendar_pdf",
        "url": "https://www.inegi.org.mx/contenidos/saladeprensa/doc/cal_2026.pdf",
        "filename": "calendar_2026_eaa5c61f.pdf",
        "expected_bytes": 286_364,
        "expected_sha256": "eaa5c61fe101b80119ab1fadad28d37e8b702fcd586e2b5e311d5da3b91de970",
        "role": "authoritative_schedule_anchor",
        "causal_use": "future_clock_only_when_observed_before_event",
        "semantic_valid": True,
        "extract_pdf_text": True,
    },
    {
        "artifact_id": "july_release_pdf",
        "url": (
            "https://www.inegi.org.mx/contenidos/saladeprensa/boletines/2026/"
            "inpc/inpc_2q2026_08.pdf"
        ),
        "filename": "july_2026_cpi_release_504f8c9f.pdf",
        "expected_bytes": 290_302,
        "expected_sha256": "504f8c9ff32fc428ba2d39d6ce06dec43343873d8dcab4ec9ba808438d75138a",
        "role": "authoritative_historical_release",
        "causal_use": "availability_counterfactual_archive_only",
        "semantic_valid": True,
        "extract_pdf_text": True,
    },
    {
        "artifact_id": "headline_annual_xml",
        "url": "https://www.inegi.org.mx/servicios/xml/INPCA_M_O.xml",
        "filename": "INPCA_M_O_e8c36462.xml",
        "expected_bytes": 679,
        "expected_sha256": "e8c36462209e65a08c6d8c8afe79bcc6eb01b96681a5f27b696dcf38955a7b56",
        "role": "historical_release_corroboration",
        "causal_use": "later_observed_current_snapshot_archive_only",
        "semantic_valid": True,
        "extract_pdf_text": False,
    },
    {
        "artifact_id": "headline_monthly_xml",
        "url": "https://www.inegi.org.mx/servicios/xml/INPCM_M_O.xml",
        "filename": "INPCM_M_O_a262c359.xml",
        "expected_bytes": 681,
        "expected_sha256": "a262c35928f3d034b209b1f521c112fc9419a0d1ded1b3c6d5bb385ee3e8b4e0",
        "role": "historical_release_corroboration",
        "causal_use": "later_observed_current_snapshot_archive_only",
        "semantic_valid": True,
        "extract_pdf_text": False,
    },
    {
        "artifact_id": "core_annual_xml",
        "url": "https://www.inegi.org.mx/servicios/xml/INPCAS_M_O.xml",
        "filename": "INPCAS_M_O_bc61647d.xml",
        "expected_bytes": 703,
        "expected_sha256": "bc61647d7ae366f9c2ba18d7dbc754205fb60899e455bd23ac1876f773471952",
        "role": "historical_release_corroboration",
        "causal_use": "later_observed_current_snapshot_archive_only",
        "semantic_valid": True,
        "extract_pdf_text": False,
    },
    {
        "artifact_id": "core_monthly_xml",
        "url": "https://www.inegi.org.mx/servicios/xml/INPCMS_M_O.xml",
        "filename": "INPCMS_M_O_d5c33c4e.xml",
        "expected_bytes": 705,
        "expected_sha256": "d5c33c4ea9917cec34fc49697f6d1c275fecc2338ca6f1c17f5a98f5b47af5e0",
        "role": "historical_release_corroboration",
        "causal_use": "later_observed_current_snapshot_archive_only",
        "semantic_valid": True,
        "extract_pdf_text": False,
    },
    {
        "artifact_id": "calendar_xml_later_revision",
        "url": "https://www.inegi.org.mx/servicios/xml/calendar.xml",
        "filename": "calendar_xml_later_revision_4af72939.xml",
        "expected_bytes": 13_370,
        "expected_sha256": "4af72939ac4754fa4016f0321f70562df5fcc4440cb3c70c6908fd524a017629",
        "role": "quarantined_later_observed_revision",
        "causal_use": "forbidden_for_prior_clock_or_proof",
        "semantic_valid": False,
        "extract_pdf_text": False,
    },
)


def _utc_now() -> str:
    return dt.datetime.now(tz=UTC).isoformat()


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _normalized_pdf_text(raw: bytes) -> tuple[str, int]:
    if pypdf.__version__ != "6.15.0":
        raise RuntimeError(f"unexpected_pypdf_version:{pypdf.__version__}")
    reader = pypdf.PdfReader(io.BytesIO(raw), strict=True)
    pages: list[str] = []
    for page in reader.pages:
        text = page.extract_text(extraction_mode="layout") or ""
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        text = "\n".join(line.rstrip(" \t") for line in text.split("\n"))
        pages.append(unicodedata.normalize("NFC", text).strip())
    return "\n\f\n".join(pages).strip() + "\n", len(reader.pages)


def _fetch(source: dict[str, Any]) -> tuple[bytes, dict[str, Any]]:
    requested_utc = _utc_now()
    request = urllib.request.Request(
        source["url"],
        headers={
            "User-Agent": "forex-research-mxn-inegi-cpi-exact-v1/1.0",
            "Accept": "application/pdf, application/xml, text/xml;q=0.9, */*;q=0.1",
        },
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        raw = response.read()
        status = int(response.status)
        final_url = response.geturl()
        headers = response.headers
    observed_utc = _utc_now()
    digest = _sha256(raw)
    if status != 200:
        raise RuntimeError(f"http_status_mismatch:{source['artifact_id']}:{status}")
    if final_url != source["url"]:
        raise RuntimeError(f"redirect_forbidden:{source['artifact_id']}:{final_url}")
    if len(raw) != source["expected_bytes"]:
        raise RuntimeError(
            f"byte_count_mismatch:{source['artifact_id']}:{len(raw)}"
        )
    if digest != source["expected_sha256"]:
        raise RuntimeError(f"sha256_mismatch:{source['artifact_id']}:{digest}")
    record = {
        "artifact_id": source["artifact_id"],
        "source_url": source["url"],
        "final_url": final_url,
        "request_started_utc": requested_utc,
        "observed_utc": observed_utc,
        "http_status": status,
        "http_date": headers.get("Date"),
        "content_type": headers.get("Content-Type"),
        "content_length_header": headers.get("Content-Length"),
        "raw_bytes": len(raw),
        "raw_sha256": digest,
        "archive_path": str(
            (ARCHIVE_DIR / source["filename"]).relative_to(WORKSPACE_ROOT)
        ).replace("\\", "/"),
        "role": source["role"],
        "causal_use": source["causal_use"],
        "semantic_valid": source["semantic_valid"],
        "raw_payload_retained": True,
        "read_only": True,
    }
    return raw, record


def _atomic_write(path: Path, raw: bytes) -> None:
    if path.exists():
        existing = path.read_bytes()
        if existing != raw:
            raise RuntimeError(f"existing_archive_mismatch:{path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("xb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _make_read_only(path: Path) -> None:
    os.chmod(path, stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH)
    if os.name == "nt":
        os.system(f'attrib +R "{path}" >NUL')


def main() -> int:
    if MANIFEST_PATH.exists():
        raise RuntimeError("capture_manifest_already_exists_refusing_recapture")

    fetched: list[tuple[dict[str, Any], bytes, dict[str, Any]]] = []
    for source in SOURCES:
        raw, record = _fetch(source)
        fetched.append((source, raw, record))

    extractions: list[tuple[Path, bytes, dict[str, Any]]] = []
    for source, raw, record in fetched:
        if not source["extract_pdf_text"]:
            record["pdf_extraction"] = None
            continue
        text, page_count = _normalized_pdf_text(raw)
        text_raw = text.encode("utf-8")
        text_name = f"{Path(source['filename']).stem}.{EXTRACTOR_ID}.txt"
        text_path = ARCHIVE_DIR / text_name
        record["pdf_extraction"] = {
            "extractor": "pypdf",
            "extractor_version": pypdf.__version__,
            "python_version": sys.version.split()[0],
            "transformation_id": EXTRACTOR_ID,
            "page_count": page_count,
            "text_archive_path": str(text_path.relative_to(WORKSPACE_ROOT)).replace(
                "\\", "/"
            ),
            "text_bytes": len(text_raw),
            "text_sha256": _sha256(text_raw),
            "transformations": [
                "pypdf_page_extract_text_layout",
                "crlf_and_cr_to_lf",
                "rstrip_space_and_tab_per_line",
                "unicode_nfc",
                "strip_each_page",
                "join_pages_with_lf_formfeed_lf",
                "single_terminal_lf",
            ],
        }
        extractions.append((text_path, text_raw, record["pdf_extraction"]))

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for source, raw, _record in fetched:
        path = ARCHIVE_DIR / source["filename"]
        _atomic_write(path, raw)
        written.append(path)
    for path, raw, _record in extractions:
        _atomic_write(path, raw)
        written.append(path)

    manifest = {
        "schema_version": 1,
        "source_id": "mxn_inegi_cpi_exact_v1",
        "source_contract_id": "mxn_inegi_cpi_exact_v1_20260817",
        "source_cohort_id": "mxn_inegi_cpi_exact_v1_20260817",
        "capture_completed_utc": _utc_now(),
        "capture_method": "python_urllib_request_read_all_then_atomic_replace",
        "primary_pdf_runtime": {
            "extractor": "pypdf",
            "extractor_version": pypdf.__version__,
            "python_version": sys.version.split()[0],
            "transformation_id": EXTRACTOR_ID,
        },
        "calendar_authority": "calendar_pdf",
        "calendar_xml_prior_expected_unretained": {
            "bytes": 12_922,
            "sha256": "e30b070e3db019249bddd3e22e72de504b55466dd23699df873cc3e773d6af91",
            "backfilled": False,
            "relabelled": False,
            "causal_use": "forbidden",
        },
        "optional_historical_xmls_retained": False,
        "artifacts": [record for _source, _raw, record in fetched],
        "policy": {
            "registered_with_live_collector": False,
            "enabled": False,
            "runtime_supported": False,
            "research_only": True,
            "shadow_only": True,
            "historical_july_availability_counterfactual_only": True,
            "future_clock_supports_prospective_collection_only": True,
            "consensus": None,
            "surprise": None,
            "direction": None,
            "proof_eligible": False,
            "promotion_eligible": False,
            "authorization_eligible": False,
            "execution_eligible": False,
            "supported_execution_decision": "no_trade",
        },
    }
    manifest_raw = (
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    _atomic_write(MANIFEST_PATH, manifest_raw)
    written.append(MANIFEST_PATH)

    for path in written:
        _make_read_only(path)
        reread = path.read_bytes()
        if path == MANIFEST_PATH:
            if reread != manifest_raw:
                raise RuntimeError("manifest_post_write_mismatch")
        elif not any(_sha256(reread) == source["expected_sha256"] for source in SOURCES):
            if not any(_sha256(reread) == item[2]["text_sha256"] for item in extractions):
                raise RuntimeError(f"retained_artifact_post_write_mismatch:{path}")

    print(
        json.dumps(
            {
                "archive_dir": str(ARCHIVE_DIR),
                "manifest_bytes": len(manifest_raw),
                "manifest_sha256": _sha256(manifest_raw),
                "artifact_count": len(fetched),
                "pdf_text_count": len(extractions),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
