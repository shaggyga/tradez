"""Design 14.5 numeric evidence consumer; never creates a forecast feature or surprise."""
import copy
import hashlib
import math

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
        require(actual["numeric_cache_key"] == expected["numeric_cache_key"], kind + "_numeric_cache_mismatch")


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
    for sidecar, kind in ((component, "component"), (unit, "unit"), (provenance, "provenance")):
        if sidecar is not None:
            require(sidecar["original_numeric_event"] == event, kind + "_numeric_content_mismatch")
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
        item["cutoff_epoch"] = _epoch(cutoff)
        if "age_seconds" in item:
            require(item["published_epoch"] + item["age_seconds"] == item["cutoff_epoch"], "meter_document_cutoff_mismatch")
        event = numeric.get(item["event_id"])
        if event is None:
            item["numeric_evidence"] = {"status": "no_selected_numeric_event", "forecast_admission": False, "numeric_surprise": None, "fx_direction": None}
        elif item["source_id"] != event["source_id"]:
            item["numeric_evidence"] = {"status": "selected_numeric_source_mismatch", "event_id": event["event_id"], "forecast_admission": False, "numeric_surprise": None, "fx_direction": None}
        elif tuple(item["version_ids"]) != _event_identity(event)["selected_numeric_version_ids"]:
            item["numeric_evidence"] = {"status": "selected_numeric_version_not_in_document", "event_id": event["event_id"], "selected_version_ids": list(_event_identity(event)["selected_numeric_version_ids"]), "forecast_admission": False, "numeric_surprise": None, "fx_direction": None}
        elif event.get("status") != "retained_numeric_observation_ready":
            item["numeric_evidence"] = {"status": "original_numeric_gate_blocked", "original_status": event.get("status"), "forecast_admission": False, "numeric_surprise": None, "fx_direction": None}
        elif type(event.get("source_numeric_available_epoch")) not in (int, float) or not math.isfinite(event["source_numeric_available_epoch"]) or event["source_numeric_available_epoch"] > item["cutoff_epoch"]:
            item["numeric_evidence"] = {"status": "numeric_readiness_missing_or_future", "forecast_admission": False, "numeric_surprise": None, "fx_direction": None}
        else:
            item["numeric_evidence"] = _qualification(event, components.get(event["event_id"]), units.get(event["event_id"]), provenances.get(event["event_id"]))
        item["numeric_evidence_sha256"] = fingerprint(item["numeric_evidence"])
        output.append(item)
    return output


def join_all(meter_documents, numeric_source_asof, component_source_asof, unit_source_asof, provenance_source_asof, states):
    """Join one-to-one cutoffs; reject a future/missing/duplicate sidecar instead of borrowing it."""
    by_cutoff = {}
    for name, rows in (("numeric", numeric_source_asof), ("component", component_source_asof), ("unit", unit_source_asof), ("provenance", provenance_source_asof)):
        by_cutoff[name] = _index(rows, "cutoff", "duplicate_" + name + "_cutoff")
    require(set(by_cutoff["numeric"]) == set(by_cutoff["component"]) == set(by_cutoff["unit"]) == set(by_cutoff["provenance"]), "sidecar_cutoff_population_mismatch")
    docs = _index(meter_documents, "document_reference_sha256", "duplicate_meter_document")
    refs_by_cutoff = {}
    for state in states:
        refs_by_cutoff.setdefault(state["cutoff_epoch"], set()).update(state["document_references"])
    require(set(refs_by_cutoff) == {_epoch(c) for c in by_cutoff["numeric"]}, "meter_sidecar_cutoff_population_mismatch")
    require(set().union(*refs_by_cutoff.values()) == set(docs) if refs_by_cutoff else not docs, "meter_document_reference_population_mismatch")
    result = []
    for cutoff in sorted(by_cutoff["numeric"]):
        visible = [docs[r] for r in sorted(refs_by_cutoff[_epoch(cutoff)])]
        result.extend(join_snapshot(visible, by_cutoff["numeric"][cutoff], by_cutoff["component"][cutoff], by_cutoff["unit"][cutoff], by_cutoff["provenance"][cutoff]))
    require(len(result) == len(docs) and len({d["document_reference_sha256"] for d in result}) == len(docs), "meter_cross_cutoff_reference_reuse")
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
    from macro_component_state_v2 import validate_parent
    required = {
        "numeric_source_asof.json",
        "repaired_component_source_asof.json",
        "unit_source_asof.json",
        "provenance_source_asof.json",
        "numeric_material_cache.json", "component_evidence_cache.json", "unit_source_bindings.json",
        "unit_unresolved_evidence.json", "provenance_bindings.json",
        "numeric_parent_manifest.json", "component_parent_manifest.json", "unit_parent_manifest.json", "provenance_parent_manifest.json",
    }
    require(required <= set(blobs), "numeric_join_sidecars_missing")
    parent_ids = []
    for kind, names in (("numeric", ["numeric_source_asof.json", "numeric_material_cache.json"]),
                        ("component", ["repaired_component_source_asof.json", "component_evidence_cache.json"]),
                        ("unit", ["unit_source_asof.json", "unit_source_bindings.json", "unit_unresolved_evidence.json"]),
                        ("provenance", ["provenance_source_asof.json", "provenance_bindings.json"])):
        parent_ids.append(validate_parent(blobs, kind + "_parent_manifest.json", names))
    output = build({k: v for k, v in blobs.items() if k not in required})
    joined = join_all(
        output["meter_document_evidence.json"],
        _json(blobs["numeric_source_asof.json"]),
        _json(blobs["repaired_component_source_asof.json"]),
        _json(blobs["unit_source_asof.json"]),
        _json(blobs["provenance_source_asof.json"]),
        output["meter_currency_states.json"],
    )
    resolve_retained_bindings(joined, blobs)
    output["meter_document_evidence.json"] = joined
    output["meter_report.json"] = {
        **output["meter_report.json"],
        "document_evidence_rows": len(joined),
        "numeric_joined_document_rows": sum(d["numeric_evidence"]["status"] == "retained_numeric_evidence_joined" for d in joined),
        "numeric_unavailable_document_rows": sum(d["numeric_evidence"]["status"] != "retained_numeric_evidence_joined" for d in joined),
        "numeric_surprises_computed": 0,
        "forecast_features_admitted": 0,
        "forecast_improvement_proven": False,
        "numeric_parent_run_identities": parent_ids,
    }
    output["meter_view.html"] = render_joined_html(joined, output["meter_currency_states.json"])
    return output


def _json(value):
    import json
    return json.loads(value)


def render_joined_html(documents, states=()):
    """Extend the pinned predecessor inspector without changing its historical source."""
    from macro_meter_v2 import render_html
    html = render_html(states, documents)
    hook = "box.append(full)}"
    require(html.count(hook) == 1, "meter_inspector_extension_hook_changed")
    html = html.replace(hook, "box.append(full);text('h3','Numeric evidence and qualification',box);text('pre',JSON.stringify(d.numeric_evidence,null,2),box)}")
    html = html.replace("box.replaceChildren();text('h2',d.source_id,box);", "box.replaceChildren();if(!d){text('p','Document unavailable in this snapshot.',box);return}text('h2',d.source_id,box);")
    html = html.replace("el('summary').replaceChildren();text('h2',s.currency", "el('summary').replaceChildren();if(!s){el('concepts').replaceChildren();el('documents').replaceChildren();el('detail').replaceChildren();el('coverage').textContent='No source snapshot available.';text('p','No source snapshot available.',el('summary'));return}text('h2',s.currency")
    html = html.replace('<section id="summary">', '<button id="reset" type="button">Reset selection</button><section id="summary">')
    html = html.replace("el('currency').onchange=render;", "el('reset').onclick=()=>{el('currency').selectedIndex=0;el('cutoff').selectedIndex=0;render()};el('currency').onchange=render;")
    return html


def resolve_retained_bindings(documents, blobs):
    """Resolve exact selected-version details, retaining retrospective gaps as gaps."""
    versions = _index(_json(blobs["version_bindings.json"]), "version_id", "duplicate_meter_version")
    materials = _index(_json(blobs["numeric_material_cache.json"]), "numeric_cache_key", "duplicate_numeric_material")
    caches = {name: _index(_json(blobs[file]), "version_id", "duplicate_" + name + "_binding") for name, file in (
        ("component", "component_evidence_cache.json"), ("unit", "unit_source_bindings.json"), ("provenance", "provenance_bindings.json"))}
    for doc in documents:
        evidence = doc["numeric_evidence"]
        selected = [versions[v] for v in doc["version_ids"]]
        for version in selected:
            require(version["event_id"] == doc["event_id"] and version["source_id"] == doc["source_id"] and version["cache_key"] == doc["cache_key"], "selected_document_identity_mismatch")
        key = evidence.get("numeric_cache_key")
        if key is not None:
            require(key in materials, "numeric_material_missing")
            material = materials[key]
            require(material["source_id"] == doc["source_id"] and material["text_cache_key"] == doc["cache_key"] and all(v["content_sha256"] == material["content_sha256"] for v in selected), "numeric_material_content_mismatch")
            require(material["cells"] == evidence["numeric_cells"], "numeric_material_cells_mismatch")
            for cell in material["cells"]:
                require(fingerprint({k:v for k,v in cell.items() if k != "cell_sha256"}) == cell["cell_sha256"], "numeric_cell_hash_mismatch")
            evidence["numeric_material"] = copy.deepcopy(material)
        resolved = {}
        for kind, cache in caches.items():
            rows = []
            for version in selected:
                binding = cache.get(version["version_id"])
                if binding is None:
                    continue
                require(binding["source_id"] == version["source_id"] and binding["content_sha256"] == version["content_sha256"], kind + "_content_binding_mismatch")
                if kind == "provenance":
                    require(fingerprint({k:v for k,v in binding.items() if k != "binding_sha256"}) == binding["binding_sha256"], "provenance_binding_hash_mismatch")
                else:
                    for cell in binding["adapter"]["cells"]:
                        require(fingerprint({k:v for k,v in cell.items() if k != "evidence_sha256"}) == cell["evidence_sha256"], kind + "_cell_hash_mismatch")
                rows.append(copy.deepcopy(binding))
            resolved[kind] = {"status":"retrospective_exact_version_evidence" if rows else "no_scoped_binding_for_selected_version", "bindings":rows, "forecast_admission":False}
            sidecar = evidence.get(kind + "_evidence")
            if sidecar and kind == "provenance":
                refs = sidecar["selected_provenance_bindings"]
                require(set(refs) <= {r["binding_sha256"] for r in rows} and len(refs) == len(set(refs)), "provenance_reference_unresolved")
            elif sidecar:
                refs = sidecar["component_evidence_version_ids"]
                require(set(refs) <= {r["version_id"] for r in rows} and len(refs) == len(set(refs)), kind + "_reference_unresolved")
                expected_cells = [] if not refs else cache[refs[0]]["adapter"]["cells"]
                require(all(cache[v]["adapter"]["cells"] == expected_cells for v in refs) and sidecar["reconstructed_component_cells"] == expected_cells, kind + "_resolved_cells_mismatch")
        evidence["resolved_bindings"] = resolved
        evidence["unit_unresolved_evidence"] = copy.deepcopy(_json(blobs["unit_unresolved_evidence.json"]))
        doc["numeric_evidence_sha256"] = fingerprint(evidence)
