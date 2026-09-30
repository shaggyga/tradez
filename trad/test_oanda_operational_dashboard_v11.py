import hashlib
import json
import pytest
import oanda_operational_dashboard_selection_v1 as d
from test_oanda_operational_dashboard_selection_v1 import fixture, save, NOW


def selected_v11(tmp_path):
    root, selection, originals, _ = fixture(tmp_path)
    old, summary, heartbeat = originals['joint']
    new = old.with_name(d.JOINT_V11[4]); old.rename(new)
    source = selection['joint']; source['study_path'] = new.relative_to(root).as_posix()
    registry_path = root/source['registry_path']
    registry = json.loads(registry_path.read_bytes())
    old_source = d.KINDS['joint'][5]; new_source = d.JOINT_V11[5]
    (root/new_source).write_bytes((root/old_source).read_bytes())
    registry['source_bindings'][new_source] = registry['source_bindings'].pop(old_source)
    registry['schema_version'] = d.JOINT_V11[0]
    source['registry_sha256'] = save(registry_path, registry)
    activation_path = new/'activation_receipt.json'
    activation = json.loads(activation_path.read_bytes())
    activation.update(schema_version=d.JOINT_V11[3], study_root=str(new.resolve()),
        registry_file_sha256=source['registry_sha256'], registry_sha256=d.digest(registry),
        source_bindings=registry['source_bindings'])
    source['activation_sha256'] = save(activation_path, activation)
    summary.update(schema_version=d.JOINT_V11[1],registry_sha256=d.digest(registry))
    heartbeat.update(schema_version=d.JOINT_V11[2],registry_sha256=d.digest(registry),
        summary_boundary_version='immutable_family_summary_publication_v1_20260909')
    def write():
        summary['payload_sha256']=d.digest({k:v for k,v in summary.items() if k!='payload_sha256'})
        heartbeat['summary_sha256']=d.digest(summary)
        save(new/'summary.json',summary);save(new/'heartbeat.json',heartbeat)
        save(root/d.POINTER,selection)
    write()
    return root, summary, heartbeat, write


def test_v11_selection_projects_same_original_native_forecast(tmp_path):
    root,_,_,_=selected_v11(tmp_path)
    result=d.read_dashboard_sources(root,now_epoch=NOW)
    assert result['status']=='current'
    joint=result['joint']
    assert joint['study_version']=='joint_v11' and joint['primary_selection']['selected']=='v11'
    assert joint['rows'][0]['active_forecasts'][0]['target_epoch']==NOW+3500
    assert d.project_collection_status({},joint,now_epoch=NOW)['observations']['study']['current']


@pytest.mark.parametrize('defect',['boundary','readback','stale','schema'])
def test_v11_rejects_unqualified_publication_without_hiding_price(tmp_path,defect):
    root,summary,heartbeat,write=selected_v11(tmp_path)
    if defect=='boundary':heartbeat.pop('summary_boundary_version')
    if defect=='readback':heartbeat['summary_read_completed_epoch']=NOW-4
    if defect=='stale':summary['generated_epoch']=NOW-100
    if defect=='schema':summary['schema_version']=d.KINDS['joint'][1]
    write();result=d.read_dashboard_sources(root,now_epoch=NOW)
    assert result['joint']['status']=='unavailable'
    assert result['joint']['rows']==[] and result['price']['status']=='current'
