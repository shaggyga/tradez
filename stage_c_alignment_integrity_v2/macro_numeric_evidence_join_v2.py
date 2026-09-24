"""Design 14.5 numeric evidence consumer; never creates a forecast feature or surprise."""
import copy
import hashlib

from contracts import fingerprint


def require(value, reason):
    if not value:
        raise ValueError(reason)


def _index(rows, key, duplicate_reason):
    result = {}
    for row in rows:
        value = row[key]
        require(value not in result, duplicate_reason)
        result[value] = row
    return result


def _event_identity(row):
    event = row.get("original_numeric_event", row)
    return {
        "event_id": event.get("event_id"),
        "source_id": event.get("source_id"),
        "selected_numeric_version_ids": tuple(event.get("selected_numeric_version_ids", ())),
        "numeric_cache_key": event.get("numeric_cache_key"),
    }


def _same_identity(expected, candidate, kind):
    actual = _event_identity(candidate)
    require(actual["event_id"] == expected["event_id"], kind + "_event_id_mismatch")
    require(actual["source_id"] == expected["source_id"], kind + "_source_id_mismatch")
    require(actual["selected_numeric_version_ids"] == expected["selected_numeric_version_ids"], kind + "_selected_version_mismatch")
    if expected["numeric_cache_key"] is not None:
        require(actual["numeric_cache_key"] in {None, expected["numeric_cache_key"]}, kind + "_numeric_cache_mismatch")


def _by_event(snapshot):
    return _index(snapshot.get("events", ()), "event_id", "duplicate_numeric_event")


def _aux_by_event(snapshot, kind):
    result = {}
    for row in snapshot.get("events", ()):
        event_id = _event_identity(row)["event_id"]
        require(isinstance(event_id, str) and event_id, kind + "_event_id_missing")
        require(event_id not in result, "duplicate_" + kind + "_event")
        result[event_id] = row
    return result


def _qualification(event, component, unit, provenance):
    expected = _event_identity(event)
    require(event["status"] == "retained_numeric_observation_ready", "numeric_not_ready")
    require(expected["selected_numeric_version_ids"], "numeric_selected_version_missing")
    require(isinstance(expected["numeric_cache_key"], str) and expected["numeric_cache_key"], "numeric_cache_missing")
    if component is not None:
        _same_identity(expected, component, "component")
    if unit is not None:
        _same_identity(expected, unit, "unit")
    if provenance is not None:
        _same_identity(expected, provenance, "provenance")
    return {
        "status": "retained_numeric_evidence_joined" if all(x is not None for x in (component, unit, provenance)) else "retained_numeric_evidence_partially_qualified",
        "event_id": expected["event_id"],
        "source_id": expected["source_id"],
        "selected_version_ids": list(expected["selected_numeric_version_ids"]),
        "numeric_cache_key": expected["numeric_cache_key"],
        "numeric_cells": copy.deepcopy(event.get("numeric_cells", ())),
        "component_evidence": copy.deepcopy(component),
        "unit_evidence": copy.deepcopy(unit),
        "provenance_evidence": copy.deepcopy(provenance),
        "forecast_admission": False,
        "numeric_surprise": None,
        "fx_direction": None,
    }


def join_snapshot(meter_documents, numeric_snapshot, component_snapshot, unit_snapshot, provenance_snapshot):
    """Attach exact sidecar evidence to retained meter documents for one identical cutoff."""
    cutoff = numeric_snapshot["cutoff"]
    for snapshot, kind in ((component_snapshot, "component"), (unit_snapshot, "unit"), (provenance_snapshot, "provenance")):
        require(snapshot["cutoff"] == cutoff, kind + "_cutoff_mismatch")
    numeric = _by_event(numeric_snapshot)
    components = _aux_by_event(component_snapshot, "component")
    units = _aux_by_event(unit_snapshot, "unit")
    provenances = _aux_by_event(provenance_snapshot, "provenance")
    output = []
    for document in meter_documents:
        item = copy.deepcopy(document)
        event = numeric.get(item["event_id"])
        if event is None:
            item["numeric_evidence"] = {"status": "no_selected_numeric_event", "forecast_admission": False, "numeric_surprise": None, "fx_direction": None}
        elif item["source_id"] != event["source_id"]:
            item["numeric_evidence"] = {"status": "selected_numeric_source_mismatch", "event_id": event["event_id"], "forecast_admission": False, "numeric_surprise": None, "fx_direction": None}
        elif tuple(item["version_ids"]) != _event_identity(event)["selected_numeric_version_ids"]:
            item["numeric_evidence"] = {"status": "selected_numeric_version_not_in_document", "event_id": event["event_id"], "selected_version_ids": list(_event_identity(event)["selected_numeric_version_ids"]), "forecast_admission": False, "numeric_surprise": None, "fx_direction": None}
        else:
            item["numeric_evidence"] = _qualification(event, components.get(event["event_id"]), units.get(event["event_id"]), provenances.get(event["event_id"]))
        item["numeric_evidence_sha256"] = fingerprint(item["numeric_evidence"])
        output.append(item)
    return output


def join_all(meter_documents, numeric_source_asof, component_source_asof, unit_source_asof, provenance_source_asof):
    """Join one-to-one cutoffs; reject a future/missing/duplicate sidecar instead of borrowing it."""
    by_cutoff = {}
    for name, rows in (("numeric", numeric_source_asof), ("component", component_source_asof), ("unit", unit_source_asof), ("provenance", provenance_source_asof)):
        by_cutoff[name] = _index(rows, "cutoff", "duplicate_" + name + "_cutoff")
    require(set(by_cutoff["numeric"]) == set(by_cutoff["component"]) == set(by_cutoff["unit"]) == set(by_cutoff["provenance"]), "sidecar_cutoff_population_mismatch")
    docs = _index(meter_documents, "document_reference_sha256", "duplicate_meter_document")
    result = []
    for cutoff in sorted(by_cutoff["numeric"]):
        visible = [d for d in docs.values() if d["published_epoch"] <= _epoch(cutoff)]
        result.extend(join_snapshot(visible, by_cutoff["numeric"][cutoff], by_cutoff["component"][cutoff], by_cutoff["unit"][cutoff], by_cutoff["provenance"][cutoff]))
    return result


def _epoch(iso):
    from datetime import datetime
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()


def render_numeric_detail(evidence):
    """Structured UI-safe payload; caller renders through textContent, never HTML."""
    return {
        "status": evidence["status"],
        "event_id": evidence.get("event_id"),
        "source_id": evidence.get("source_id"),
        "selected_version_ids": evidence.get("selected_version_ids", []),
        "numeric_cells": evidence.get("numeric_cells", []),
        "component_evidence": evidence.get("component_evidence"),
        "unit_evidence": evidence.get("unit_evidence"),
        "provenance_evidence": evidence.get("provenance_evidence"),
        "forecast_admission": False,
        "numeric_surprise": None,
        "fx_direction": None,
    }


def build_joined_meter(blobs):
    """Build the existing meter, then replace its document payload with bound numeric detail."""
    from macro_meter_v2 import build
    required = {
        "numeric_source_asof.json",
        "repaired_component_source_asof.json",
        "unit_source_asof.json",
        "provenance_source_asof.json",
    }
    require(required <= set(blobs), "numeric_join_sidecars_missing")
    output = build({k: v for k, v in blobs.items() if k not in required})
    joined = join_all(
        output["meter_document_evidence.json"],
        _json(blobs["numeric_source_asof.json"]),
        _json(blobs["repaired_component_source_asof.json"]),
        _json(blobs["unit_source_asof.json"]),
        _json(blobs["provenance_source_asof.json"]),
    )
    output["meter_document_evidence.json"] = joined
    output["meter_report.json"] = {
        **output["meter_report.json"],
        "document_evidence_rows": len(joined),
        "numeric_joined_document_rows": sum(d["numeric_evidence"]["status"] == "retained_numeric_evidence_joined" for d in joined),
        "numeric_unavailable_document_rows": sum(d["numeric_evidence"]["status"] != "retained_numeric_evidence_joined" for d in joined),
        "numeric_surprises_computed": 0,
        "forecast_features_admitted": 0,
        "forecast_improvement_proven": False,
    }
    output["meter_view.html"] = render_joined_html(joined)
    return output


def _json(value):
    import json
    return json.loads(value)


def render_joined_html(documents):
    """Local, data-only drilldown for the new joined document payload."""
    import json
    data = json.dumps(documents, ensure_ascii=True, separators=(",", ":")).replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")
    return """<!doctype html><meta charset=\"utf-8\"><title>Numeric evidence drilldown</title>
<select id=\"document\"></select><pre id=\"detail\"></pre>
<script id=\"joined-data\" type=\"application/json\">"""+data+"""</script><script>
'use strict';const rows=JSON.parse(document.getElementById('joined-data').textContent),s=document.getElementById('document'),d=document.getElementById('detail');
rows.forEach((r,i)=>{const o=document.createElement('option');o.value=i;o.textContent=r.source_id+' · '+r.event_id;s.append(o)});
function show(){const r=rows[Number(s.value)];d.textContent=JSON.stringify({event_id:r.event_id,source_id:r.source_id,concepts:r.concepts,retained_text:r.retained_text,numeric_evidence:r.numeric_evidence},null,2)}s.onchange=show;show();</script>"""
