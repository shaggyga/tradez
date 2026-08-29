import datetime as dt
import json
import sqlite3
import zipfile
from io import BytesIO

from oanda_official_daily_rate_context import (
    parse_mof_jgb_csv,
    parse_rba_f2_csv,
    parse_boe_latest_yield_zip,
    parse_mas_sgs_benchmark_html,
    parse_hkma_daily_rate_json,
    run_once,
)


UTC = dt.timezone.utc


def config(path):
    path.write_text(
        json.dumps(
            {
                "contract_id": "test_daily_rates",
                "sources": [
                    {"source_id": "usd", "source_contract_id": "usd_v1", "provider": "US",
                     "currency": "USD", "series_id": "US2Y", "kind": "test"},
                    {"source_id": "eur", "source_contract_id": "eur_v1", "provider": "ECB",
                     "currency": "EUR", "series_id": "EU2Y", "kind": "test"},
                ],
                "limitations": ["not intraday"],
            }
        ),
        encoding="utf-8",
    )


def test_bootstrap_is_not_prospective_but_next_date_is(tmp_path):
    cfg = tmp_path / "config.json"
    config(cfg)
    common = dict(
        config_path=cfg, database=tmp_path / "rates.sqlite",
        output=tmp_path / "state.json", report=tmp_path / "report.md",
        archive=tmp_path / "archive",
    )
    first = run_once(
        **common, observed=dt.datetime(2026, 8, 12, 12, tzinfo=UTC),
        acquired={
            "usd": ([{"rate_date": "2026-08-11", "rate_pct": 3.0}], "usd1"),
            "eur": ([{"rate_date": "2026-08-11", "rate_pct": 2.0}], "eur1"),
        },
    )
    assert first["totals"]["prospective_rows"] == 0
    assert not first["cross_currency_differentials"][0]["prospective_eligible"]
    second = run_once(
        **common, observed=dt.datetime(2026, 8, 13, 12, tzinfo=UTC),
        acquired={
            "usd": ([{"rate_date": "2026-08-11", "rate_pct": 3.0},
                      {"rate_date": "2026-08-12", "rate_pct": 3.1}], "usd2"),
            "eur": ([{"rate_date": "2026-08-11", "rate_pct": 2.0},
                      {"rate_date": "2026-08-12", "rate_pct": 1.95}], "eur2"),
        },
    )
    assert second["totals"]["prospective_rows"] == 2
    assert second["currencies"]["USD"]["change_bps_1d"] == 10.0
    assert round(second["currencies"]["EUR"]["change_bps_1d"], 8) == -5.0
    differential = second["cross_currency_differentials"][0]
    assert differential["prospective_eligible"]
    assert round(differential["change_differential_bps_1d"], 8) == -15.0
    assert second["intraday_rate_confirmation"] is False
    db = sqlite3.connect(tmp_path / "rates.sqlite")
    assert db.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    db.close()


def test_upstream_treasury_prospective_identity_is_preserved(tmp_path):
    cfg = tmp_path / "config.json"
    cfg.write_text(
        json.dumps(
            {"contract_id": "test", "sources": [
                {"source_id": "usd", "source_contract_id": "usd_v1", "provider": "US",
                 "currency": "USD", "series_id": "US2Y", "kind": "test"}
            ]}
        ), encoding="utf-8"
    )
    result = run_once(
        config_path=cfg, database=tmp_path / "rates.sqlite",
        output=tmp_path / "state.json", report=tmp_path / "report.md",
        archive=tmp_path / "archive", observed=dt.datetime(2026, 8, 12, 12, tzinfo=UTC),
        acquired={"usd": ([{
            "rate_date": "2026-08-11", "rate_pct": 3.0,
            "first_seen_utc": "2026-08-12T00:01:00+00:00",
            "upstream_observation_id": "treasury_original",
            "upstream_prospective_eligible": True,
        }], "upstream")},
    )
    assert result["currencies"]["USD"]["prospective_eligible"]
    assert result["currencies"]["USD"]["observation_kind"] == "upstream_prospective_import"
    assert result["currencies"]["USD"]["observed_utc"] == "2026-08-12T00:01:00+00:00"


def test_japan_mof_parser_selects_two_year_and_normalizes_dates():
    raw = (
        "Interest Rate,,,,\n"
        "Date,1Y,2Y,3Y\n"
        "2026/7/30,1.243,1.497,1.638\n"
        "2026/7/31,1.255,1.507,1.658\n"
    ).encode("utf-8")
    assert parse_mof_jgb_csv(raw) == [
        {"rate_date": "2026-07-30", "rate_pct": 1.497},
        {"rate_date": "2026-07-31", "rate_pct": 1.507},
    ]


def test_rba_parser_uses_series_id_not_column_position():
    raw = (
        "F2 CAPITAL MARKET YIELDS\n"
        "Title,Three year,Two year\n"
        "Series ID,FCMYGBAG3D,FCMYGBAG2D\n"
        "11-Aug-2026,4.564,4.581\n"
        "12-Aug-2026,4.549,4.568\n"
    ).encode("utf-8")
    assert parse_rba_f2_csv(raw, "FCMYGBAG2D") == [
        {"rate_date": "2026-08-11", "rate_pct": 4.581},
        {"rate_date": "2026-08-12", "rate_pct": 4.568},
    ]


def test_boe_latest_zip_parser_selects_named_workbook_sheet_and_maturity():
    workbook_xml = b'''<?xml version="1.0" encoding="UTF-8"?>
    <workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
      xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
      <sheets><sheet name="4. spot curve" sheetId="1" r:id="rId1"/></sheets>
    </workbook>'''
    relationships_xml = b'''<?xml version="1.0" encoding="UTF-8"?>
    <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
      <Relationship Id="rId1" Target="worksheets/sheet1.xml"/>
    </Relationships>'''
    sheet_xml = b'''<?xml version="1.0" encoding="UTF-8"?>
    <worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
      <sheetData>
        <row r="4"><c r="A4"><v>0</v></c><c r="B4"><v>1</v></c><c r="C4"><v>2</v></c></row>
        <row r="6"><c r="A6"><v>46237</v></c><c r="B6"><v>3.9</v></c><c r="C6"><v>4.1</v></c></row>
        <row r="7"><c r="A7"><v>46238</v></c><c r="B7"><v>3.8</v></c><c r="C7"><v>4.0</v></c></row>
      </sheetData>
    </worksheet>'''
    inner = BytesIO()
    with zipfile.ZipFile(inner, "w") as workbook:
        workbook.writestr("xl/workbook.xml", workbook_xml)
        workbook.writestr("xl/_rels/workbook.xml.rels", relationships_xml)
        workbook.writestr("xl/worksheets/sheet1.xml", sheet_xml)
    outer = BytesIO()
    with zipfile.ZipFile(outer, "w") as archive:
        archive.writestr("OIS daily data current month.xlsx", inner.getvalue())
    rows = parse_boe_latest_yield_zip(
        outer.getvalue(), workbook_name="OIS daily data current month.xlsx",
        sheet_name="4. spot curve", maturity_years=2.0,
    )
    assert rows == [
        {"rate_date": "2026-08-03", "rate_pct": 4.1},
        {"rate_date": "2026-08-04", "rate_pct": 4.0},
    ]


def test_mas_parser_selects_third_yield_as_two_year_benchmark():
    raw = b'''<table><tbody>
      <tr><td class="date">13 Aug 2026</td>
        <td class="yield">1.55</td><td class="yield">1.65</td>
        <td class="price">102.34</td><td class="yield">1.66</td>
        <td class="price">98.49</td><td class="yield">1.95</td></tr>
      <tr><td class="date">14 Aug 2026</td>
        <td class="yield">1.55</td><td class="yield">1.65</td>
        <td class="price">102.34</td><td class="yield">1.66</td>
        <td class="price">98.54</td><td class="yield">1.94</td></tr>
    </tbody></table>'''
    assert parse_mas_sgs_benchmark_html(raw) == [
        {"rate_date": "2026-08-13", "rate_pct": 1.66},
        {"rate_date": "2026-08-14", "rate_pct": 1.66},
    ]


def test_hkma_parser_selects_configured_hibor_field_and_orders_dates():
    raw = json.dumps(
        {
            "result": {
                "records": [
                    {"end_of_day": "2026-07-31", "ir_1m": 2.67536},
                    {"end_of_day": "2026-07-30", "ir_1m": 2.6},
                ]
            }
        }
    ).encode("utf-8")
    assert parse_hkma_daily_rate_json(raw, value_field="ir_1m") == [
        {"rate_date": "2026-07-30", "rate_pct": 2.6},
        {"rate_date": "2026-07-31", "rate_pct": 2.67536},
    ]
