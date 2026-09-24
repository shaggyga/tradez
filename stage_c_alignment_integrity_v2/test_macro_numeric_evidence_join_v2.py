import copy
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import macro_numeric_evidence_join_v2 as join


def event():
    return {"event_id":"event","source_id":"source","status":"retained_numeric_observation_ready","selected_numeric_version_ids":["v1"],"numeric_cache_key":"cache","numeric_cells":[{"values":{"actual":"1"}}],"source_numeric_available_epoch":1}


def auxiliary(kind):
    return {"original_numeric_event":copy.deepcopy(event()),"kind":kind}


def document():
    return {"document_reference_sha256":"doc","event_id":"event","source_id":"source","version_ids":["v1"],"published_epoch":1}


def snapshots():
    n={"cutoff":"2026-01-01T00:00:00Z","events":[event()]}
    return n,{"cutoff":n["cutoff"],"events":[auxiliary("component")]},{"cutoff":n["cutoff"],"events":[auxiliary("unit")]},{"cutoff":n["cutoff"],"events":[auxiliary("provenance")]}


def test_exact_identity_join_and_no_forecast_admission():
    n,c,u,p=snapshots();row=join.join_snapshot([document()],n,c,u,p)[0]
    evidence=row["numeric_evidence"]
    assert evidence["status"]=="retained_numeric_evidence_joined"
    assert evidence["numeric_cells"][0]["values"]["actual"]=="1"
    assert evidence["forecast_admission"] is False and evidence["numeric_surprise"] is evidence["fx_direction"] is None
    assert row["numeric_evidence_sha256"]==join.fingerprint(evidence)


@pytest.mark.parametrize("target,field,value,reason",[("unit","source_id","wrong","unit_source_id_mismatch"),("provenance","selected_numeric_version_ids",["wrong"],"provenance_selected_version_mismatch")])
def test_sidecar_identity_mismatch_refused(target,field,value,reason):
    n,c,u,p=snapshots();sidecars={"numeric":n,"component":c,"unit":u,"provenance":p}
    row=sidecars[target]["events"][0]
    (row if target=="numeric" else row["original_numeric_event"])[field]=value
    with pytest.raises(ValueError,match=reason):join.join_snapshot([document()],n,c,u,p)


def test_document_version_or_source_never_falls_back():
    n,c,u,p=snapshots();d=document();d["version_ids"]=["old"]
    assert join.join_snapshot([d],n,c,u,p)[0]["numeric_evidence"]["status"]=="selected_numeric_version_not_in_document"
    d=document();d["source_id"]="other"
    assert join.join_snapshot([d],n,c,u,p)[0]["numeric_evidence"]["status"]=="selected_numeric_source_mismatch"


def test_cutoff_and_duplicate_refused():
    n,c,u,p=snapshots();c["cutoff"]="2026-01-02T00:00:00Z"
    with pytest.raises(ValueError,match="component_cutoff_mismatch"):join.join_snapshot([document()],n,c,u,p)
    n,c,u,p=snapshots();p["events"].append(auxiliary("duplicate"))
    with pytest.raises(ValueError,match="duplicate_provenance_event"):join.join_snapshot([document()],n,c,u,p)


def test_empty_or_missing_event_is_explicit_and_ui_payload_is_data_only():
    n,c,u,p=snapshots();d=document();d["event_id"]="none"
    evidence=join.join_snapshot([d],n,c,u,p)[0]["numeric_evidence"]
    assert evidence["status"]=="no_selected_numeric_event"
    payload=join.render_numeric_detail(evidence)
    assert payload["forecast_admission"] is False and "html" not in payload


def test_joined_view_keeps_text_concepts_beside_numeric_evidence():
    row = document(); row["retained_text"] = "Release text"; row["concepts"] = ["inflation"]
    html = join.render_joined_html([row])
    assert "retained_text" in html and "concepts" in html and "numeric_evidence" in html
    assert "innerHTML" not in html


def add_binding_inputs(root, inputs):
    import shutil
    for kind, folder, names in (
        ("numeric", "timed_20260922_194458/macro_numeric_state/runs/numeric-state-20260914-20260921", ["numeric_material_cache.json"]),
        ("component", "timed_20260922_194458/macro_component_state/runs/component-state-20260914-20260921", ["component_evidence_cache.json"]),
        ("unit", "next_unit_binding_20260922_204402/unit_binding/runs/unit-binding-20260914-20260921", ["unit_source_bindings.json", "unit_unresolved_evidence.json"]),
        ("provenance", "four_next_20260923_040615/provenance/runs/provenance-20260914-20260921", ["provenance_bindings.json"]),
    ):
        for name in names: shutil.copyfile(root/folder/name, inputs/name)
        shutil.copyfile(root/folder/"COMPLETION_MANIFEST.json", inputs/(kind+"_parent_manifest.json"))


def test_build_requires_all_four_sidecars():
    with pytest.raises(ValueError,match="numeric_join_sidecars_missing"):
        join.build_joined_meter({})


def test_retained_real_sidecars_build_an_actual_joined_consumer(tmp_path):
    root = os.environ.get("FOREX_NUMERIC_JOIN_EVIDENCE")
    if not root:
        pytest.skip("retained evidence root not supplied")
    root = Path(root)
    meter = root / "four_next_20260923_040615" / "meter" / "inputs"
    numeric = root / "timed_20260922_194458" / "macro_numeric_state" / "runs" / "numeric-state-20260914-20260921"
    component = root / "timed_20260922_194458" / "macro_component_state" / "runs" / "component-state-20260914-20260921"
    unit = root / "next_unit_binding_20260922_204402" / "unit_binding" / "runs" / "unit-binding-20260914-20260921"
    provenance = root / "four_next_20260923_040615" / "provenance" / "runs" / "provenance-20260914-20260921"
    from macro_meter_operator_v2 import BASE_INPUTS
    blobs = {name:(meter/name).read_bytes() for name in BASE_INPUTS}
    for name, directory in {
        "numeric_source_asof.json":numeric,
        "repaired_component_source_asof.json":component,
        "unit_source_asof.json":unit,
        "provenance_source_asof.json":provenance,
    }.items():
        blobs[name] = (directory/name).read_bytes()
    add_binding_inputs(root, tmp_path)
    blobs.update({p.name:p.read_bytes() for p in tmp_path.iterdir()})
    output = join.build_joined_meter(blobs)
    docs = output["meter_document_evidence.json"]
    assert docs and all("numeric_evidence" in doc for doc in docs)
    assert all(doc["numeric_evidence"]["forecast_admission"] is False for doc in docs)
    assert "innerHTML" not in output["meter_view.html"] and "fetch(" not in output["meter_view.html"]
    data = json.loads(output["meter_view.html"].split('type="application/json">',1)[1].split("</script>",1)[0])
    assert data['documents'] == docs
    assert len(docs) == len({d['document_reference_sha256'] for d in docs}) == 476
    index = {d['document_reference_sha256']: d for d in docs}
    assert len(output['meter_currency_states.json']) == 168 and len(output['meter_pair_views.json']) == 544
    for s in output['meter_currency_states.json']:
        assert all(index[r]['cutoff_epoch'] == s['cutoff_epoch'] for r in s['document_references'])
    assert all(d['numeric_evidence']['resolved_bindings']['unit']['status'] == 'no_scoped_binding_for_selected_version' for d in docs)


@pytest.mark.parametrize('ready',[None, True, float('nan'), 9999999999])
def test_missing_or_future_numeric_readiness_stays_unavailable(ready):
    n,c,u,p=snapshots();n['events'][0]['source_numeric_available_epoch']=ready
    result=join.join_snapshot([document()],n,c,u,p)[0]['numeric_evidence']
    assert result['status']=='numeric_readiness_missing_or_future' and result['forecast_admission'] is False


@pytest.mark.parametrize('change',['cache','content'])
def test_sidecar_material_cannot_be_missing_or_different(change):
    n,c,u,p=snapshots()
    if change=='cache': u['events'][0]['original_numeric_event']['numeric_cache_key']=None
    else: u['events'][0]['original_numeric_event']['numeric_cells'][0]['values']['actual']='99'
    with pytest.raises(ValueError,match='unit_numeric_'):
        join.join_snapshot([document()],n,c,u,p)


def test_document_reference_stays_at_its_own_snapshot():
    n,c,u,p=snapshots();d=document();d['age_seconds']=join._epoch(n['cutoff'])-d['published_epoch']
    states=[{'cutoff_epoch':join._epoch(n['cutoff']),'document_references':['doc']}]
    result=join.join_all([d],[n],[c],[u],[p],states)
    assert len(result)==1 and result[0]['cutoff_epoch']==states[0]['cutoff_epoch']
    d['age_seconds']+=1
    with pytest.raises(ValueError,match='meter_document_cutoff_mismatch'):
        join.join_all([d],[n],[c],[u],[p],states)


def test_resolve_exact_version_content_and_provenance():
    d={**document(),'cache_key':'text','numeric_evidence':{'numeric_cache_key':'cache','numeric_cells':[]}}
    binding={'version_id':'v1','source_id':'source','content_sha256':'content','reference_binding':{'period':None},'unresolved':['missing_year']}
    binding['binding_sha256']=join.fingerprint(binding)
    values={'version_bindings.json':[{'version_id':'v1','event_id':'event','source_id':'source','cache_key':'text','content_sha256':'content'}],
        'numeric_material_cache.json':[{'numeric_cache_key':'cache','source_id':'source','text_cache_key':'text','content_sha256':'content','cells':[]}],
        'component_evidence_cache.json':[],'unit_source_bindings.json':[], 'unit_unresolved_evidence.json':{'unresolved_versions':[]},'provenance_bindings.json':[binding]}
    blobs={k:json.dumps(v).encode() for k,v in values.items()}
    join.resolve_retained_bindings([d],blobs)
    assert d['numeric_evidence']['resolved_bindings']['provenance']['bindings'][0]['unresolved']==['missing_year']
    values['numeric_material_cache.json'][0]['content_sha256']='wrong'
    blobs={k:json.dumps(v).encode() for k,v in values.items()}
    with pytest.raises(ValueError,match='numeric_material_content_mismatch'):join.resolve_retained_bindings([d],blobs)


def test_operator_runs_and_refuses_changed_recipe_inputs(tmp_path):
    root = os.environ.get("FOREX_NUMERIC_JOIN_EVIDENCE")
    if not root:
        pytest.skip("retained evidence root not supplied")
    import shutil
    import macro_numeric_evidence_join_operator_v2 as operator
    root = Path(root); inputs = tmp_path / "inputs"; inputs.mkdir()
    meter = root / "four_next_20260923_040615" / "meter" / "inputs"
    for name in operator.BASE_INPUTS[:9]: shutil.copyfile(meter/name, inputs/name)
    sources = {
        "numeric_source_asof.json":root / "timed_20260922_194458" / "macro_numeric_state" / "runs" / "numeric-state-20260914-20260921",
        "repaired_component_source_asof.json":root / "timed_20260922_194458" / "macro_component_state" / "runs" / "component-state-20260914-20260921",
        "unit_source_asof.json":root / "next_unit_binding_20260922_204402" / "unit_binding" / "runs" / "unit-binding-20260914-20260921",
        "provenance_source_asof.json":root / "four_next_20260923_040615" / "provenance" / "runs" / "provenance-20260914-20260921",
    }
    for name, source in sources.items(): shutil.copyfile(source/name, inputs/name)
    add_binding_inputs(root, inputs)
    recipe = tmp_path / "recipe.json"; recipe.write_text(json.dumps(operator.recipe_for(inputs), sort_keys=True), encoding="utf-8")
    result = operator.operate("run", recipe, operator.sha(recipe), inputs, tmp_path / "runs")
    assert result["status"] == "completed_verified"
    assert operator.operate("verify", recipe, operator.sha(recipe), inputs, tmp_path / "runs")["status"] == "completed_verified"
    (inputs / "numeric_source_asof.json").write_bytes((inputs / "numeric_source_asof.json").read_bytes()+b" ")
    assert operator.operate("status", recipe, operator.sha(recipe), inputs, tmp_path / "runs")["status"] == "review_required"


def test_operator_crash_resume_is_exact(tmp_path):
    root = os.environ.get("FOREX_NUMERIC_JOIN_EVIDENCE")
    if not root:
        pytest.skip("retained evidence root not supplied")
    import shutil
    import macro_numeric_evidence_join_operator_v2 as operator
    root = Path(root); inputs = tmp_path / "inputs"; inputs.mkdir()
    meter = root / "four_next_20260923_040615" / "meter" / "inputs"
    for name in operator.BASE_INPUTS[:9]: shutil.copyfile(meter/name, inputs/name)
    for name, source in {
        "numeric_source_asof.json":root / "timed_20260922_194458" / "macro_numeric_state" / "runs" / "numeric-state-20260914-20260921",
        "repaired_component_source_asof.json":root / "timed_20260922_194458" / "macro_component_state" / "runs" / "component-state-20260914-20260921",
        "unit_source_asof.json":root / "next_unit_binding_20260922_204402" / "unit_binding" / "runs" / "unit-binding-20260914-20260921",
        "provenance_source_asof.json":root / "four_next_20260923_040615" / "provenance" / "runs" / "provenance-20260914-20260921",
    }.items(): shutil.copyfile(source/name, inputs/name)
    add_binding_inputs(root, inputs)
    recipe=tmp_path / "recipe.json"; recipe.write_text(json.dumps(operator.recipe_for(inputs),sort_keys=True),encoding="utf-8")
    run=subprocess.run([sys.executable,"-I","-B",str(ROOT / "macro_numeric_evidence_join_operator_v2.py"),"run","--recipe",str(recipe),"--recipe-sha256",operator.sha(recipe),"--inputs",str(inputs),"--runs-dir",str(tmp_path / "runs"),"--test-crash-after","2"],capture_output=True,text=True,timeout=120)
    assert run.returncode==91
    assert operator.operate("resume",recipe,operator.sha(recipe),inputs,tmp_path / "runs")["status"]=="completed_verified"
