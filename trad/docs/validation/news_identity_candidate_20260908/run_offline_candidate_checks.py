"""Run the new regression cases and the frozen admission tests offline."""
import hashlib
import datetime as dt
import json
from pathlib import Path
import socket
import sys
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT.parent / "workspace" / "trad"
sys.dont_write_bytecode = True
sys.path[:0] = [str(ROOT), str(SOURCE)]


def no_network(*args, **kwargs):
    raise RuntimeError("offline_candidate_network_disabled")


socket.socket.connect = no_network
socket.socket.connect_ex = no_network
socket.create_connection = no_network

import pytest

paths = [
    ROOT / "test_news_topic_identity_reconcile_candidate_v1.py",
    SOURCE / "test_oanda_news_causal_aggregation_guard_v1.py",
]
bound_sources = [
    SOURCE / "oanda_news_causal_aggregation_guard_v1.py",
    SOURCE / "oanda_local_news_sentiment.py",
    SOURCE / "oanda_news_classification_contract.py",
]
before = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in bound_sources}
run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
xml_path = ROOT / ("CANDIDATE_TEST_RESULTS_" + run_id + ".xml")
code = pytest.main([*(str(p) for p in paths), "-q", "-p", "no:cacheprovider",
                    "--junitxml=" + str(xml_path)])
(ROOT / "CANDIDATE_TEST_RESULTS_20260908.xml").write_bytes(xml_path.read_bytes())
after = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in bound_sources}
xml = ET.parse(ROOT / "CANDIDATE_TEST_RESULTS_20260908.xml").getroot()
suites = list(xml.iter("testsuite"))
receipt = {
    "schema": "offline_news_identity_candidate_tests_v1_20260908",
    "status": "passed" if code == 0 and before == after else "failed",
    "pytest_exit_code": int(code),
    "run_id": run_id,
    "counts": {key: sum(int(s.attrib.get(key, 0)) for s in suites)
               for key in ("tests", "errors", "failures", "skipped")},
    "frozen_sources_unchanged": before == after,
    "frozen_source_hashes": before,
    "artifacts": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in [
        *paths,
        ROOT / "news_topic_identity_reconcile_candidate_v1.py",
        ROOT / "CANDIDATE_TEST_RESULTS_20260908.xml",
        Path(__file__),
    ] if p.exists()},
    "execution": {"network_disabled": True, "worker_started": False,
                  "runtime_or_registry_changed": False, "prospective_test": False,
                  "can_place_orders": False, "can_promote": False},
}
(ROOT / "CANDIDATE_TEST_RECEIPT_20260908.json").write_text(
    json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
(ROOT / ("CANDIDATE_TEST_RECEIPT_" + run_id + ".json")).write_text(
    json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
print(json.dumps({key: receipt[key] for key in ("status", "counts", "frozen_sources_unchanged")}))
raise SystemExit(int(code) if before == after else 1)
