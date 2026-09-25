"""Regress whole-group omissions through the authenticated frame consumer."""
import hashlib
import json

import pytest

from curve_chronological_operator_v2 import DEFAULT_VARIANTS, _load_frame_predictions


def _write(path, value):
    path.write_text(json.dumps(value), encoding="utf-8")
    return {"path": "run/" + path.name, "bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def _fixture(tmp_path):
    origins = [1000, 2000]
    pairs = ["EUR_USD", "USD_JPY"]
    bases = ["ridge", "recovered_hgb"]
    horizons = [360, 720]
    members = []
    for horizon in horizons:
        for origin in origins:
            coverage = [
                {"origin_epoch": origin, "instrument": pair, "base_method": base,
                 "variant": variant, "horizon_minutes": horizon, "reason": "base_unavailable"}
                for pair in pairs for base in bases for variant in DEFAULT_VARIANTS
            ]
            members.append(_write(tmp_path / f"frame_{horizon}_{origin}.json",
                                  {"coverage": coverage, "predictions": []}))
    manifest = tmp_path / "MANIFEST.json"
    _write(manifest, {"files": members})
    anchor = hashlib.sha256(manifest.read_bytes()).hexdigest()
    bulk = tmp_path / "BULK.json"
    _write(bulk, {"manifest_sha256": anchor, "members": members})
    contract = {"feature_definition": {"horizons_minutes": horizons}}
    population = {"origins": origins, "instruments": pairs, "base_methods": bases}
    return bulk, manifest, anchor, contract, population, members


def _load(tmp_path, fixture):
    bulk, manifest, anchor, contract, population, _ = fixture
    return _load_frame_predictions(tmp_path, bulk, contract, population, manifest, anchor)


def test_complete_explicit_unavailable_coverage_is_accepted(tmp_path):
    fixture = _fixture(tmp_path)
    rows, coverage, checked, origins = _load(tmp_path, fixture)
    assert rows == []
    assert len(coverage) == 48
    assert len(checked) == 4
    assert origins == [1000, 2000]


@pytest.mark.parametrize("removed", ["origin", "instrument", "base_method"])
def test_whole_group_absence_rejected_by_actual_consumer(tmp_path, removed):
    fixture = _fixture(tmp_path)
    bulk, manifest, anchor, contract, population, members = fixture
    if removed == "origin":
        shortened = [item for item in members if not item["path"].endswith("_1000.json")]
        _write(bulk, {"manifest_sha256": anchor, "members": shortened})
        message = "curve_chronological_bulk_population_mismatch"
    else:
        for item in members:
            frame = tmp_path / item["path"].split("/")[-1]
            content = json.loads(frame.read_text())
            content["coverage"] = [x for x in content["coverage"] if x[removed if removed == "instrument" else "base_method"]
                                   != ("USD_JPY" if removed == "instrument" else "ridge")]
            item.update(_write(frame, content))
        _write(manifest, {"files": members})
        anchor = hashlib.sha256(manifest.read_bytes()).hexdigest()
        _write(bulk, {"manifest_sha256": anchor, "members": members})
        fixture = bulk, manifest, anchor, contract, population, members
        message = "curve_chronological_expected_coverage_population_mismatch"
    with pytest.raises(ValueError, match=message):
        _load(tmp_path, fixture)


def test_changed_pinned_frame_is_rejected(tmp_path):
    fixture = _fixture(tmp_path)
    frame = tmp_path / "frame_360_1000.json"
    frame.write_text(frame.read_text() + " ", encoding="utf-8")
    with pytest.raises(ValueError, match="curve_chronological_member_hash_mismatch"):
        _load(tmp_path, fixture)


def test_bad_trusted_manifest_anchor_is_rejected(tmp_path):
    fixture = _fixture(tmp_path)
    bulk, manifest, anchor, contract, population, members = fixture
    with pytest.raises(ValueError, match="curve_chronological_untrusted_manifest_anchor"):
        _load_frame_predictions(tmp_path, bulk, contract, population, manifest, "0" * 64)
