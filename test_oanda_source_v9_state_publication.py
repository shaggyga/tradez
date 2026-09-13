"""Completed V9 state survives inherited and final report publication faults."""
from datetime import timedelta
import json
from pathlib import Path

import pytest

import oanda_causal_source_factor_response_map_v9 as source
from test_oanda_source_publication_successors import T0, source_fixture


def cycle_fixture(tmp_path,monkeypatch):
    database,config,activation,connection=source_fixture(tmp_path)
    connection.close()
    monkeypatch.setattr(source,"_BASE_LOAD",lambda *args,**kwargs:[])
    state=tmp_path/"current.json"
    prior={"status":"ok","contract_id":source.CONTRACT_ID,"generated_utc":T0.isoformat(),"activation":activation}
    state.write_text(json.dumps(prior),encoding="utf-8")
    return dict(config_path=config,output_database=database,snapshot_path=state,report_path=tmp_path/"current.md",
                clock=lambda:T0+timedelta(seconds=91),mapping_database=tmp_path/"absent-map.sqlite",
                raw_database=tmp_path/"absent-raw.sqlite",candle_root=tmp_path/"absent-candles",
                quote_path=tmp_path/"absent-quotes.json",technical_path=tmp_path/"absent-technical.json")


def test_current_state_publishes_once_after_all_reports(tmp_path,monkeypatch):
    args=cycle_fixture(tmp_path,monkeypatch)
    prior=args["snapshot_path"].read_bytes()
    writes=[]
    atomic_write=source.base.atomic_write
    def capture(path,text):
        path=Path(path)
        if path==args["snapshot_path"]:
            value=json.loads(text)
            assert value["status"]=="ok" and value["activation"]
            assert value["raw_event_response_is_diagnostic_only"] is True
            assert args["report_path"].is_file()
            assert writes[-1]==args["report_path"]
        else:
            assert args["snapshot_path"].read_bytes()==prior
        writes.append(path)
        return atomic_write(path,text)
    monkeypatch.setattr(source.base,"atomic_write",capture)
    result=source.run_cycle(**args)
    assert json.loads(args["snapshot_path"].read_bytes())==result
    assert writes.count(args["snapshot_path"])==1
    assert writes[-1]==args["snapshot_path"]
    private=[path for path in writes if path not in (args["snapshot_path"],args["report_path"])]
    assert len(private)==2 and private[0].parent==private[1].parent
    assert private[0].parent.name.startswith(".source-v9-")
    assert not private[0].parent.exists()


@pytest.mark.parametrize("failure_point",["inherited_report","completed_report"])
def test_report_failure_preserves_last_completed_state(tmp_path,monkeypatch,failure_point):
    args=cycle_fixture(tmp_path,monkeypatch)
    prior=args["snapshot_path"].read_bytes()
    atomic_write=source.base.atomic_write
    current_writes=[]
    def fail_report(path,text):
        path=Path(path)
        if path==args["snapshot_path"]: current_writes.append(path)
        inherited_report=path.name=="report.md" and path.parent.name.startswith(".source-v9-")
        if (failure_point=="inherited_report" and inherited_report) or (failure_point=="completed_report" and path==args["report_path"]):
            raise OSError("injected report publication failure")
        return atomic_write(path,text)
    monkeypatch.setattr(source.base,"atomic_write",fail_report)
    with pytest.raises(OSError,match="injected report publication failure"):
        source.run_cycle(**args)
    assert args["snapshot_path"].read_bytes()==prior
    assert current_writes==[]
    assert not list(tmp_path.glob(".source-v9-*"))
