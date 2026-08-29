import oanda_opportunity_gate_existing_directions as gate


def test_deduplication_collapses_same_family_pair_minute_direction():
    rows=[("a","EUR_USD","buy","2026-08-08T12:00:01Z",1.0),("a","EUR_USD","buy","2026-08-08T12:00:40Z",3.0),
          ("b","EUR_USD","buy","2026-08-08T12:00:10Z",2.0)]
    result=gate.deduplicate(rows)
    assert len(result)==2
    assert next(row for row in result if row["family"]=="a")["net_pips"]==2.0


def test_summary_exposes_best_result_dependence():
    result=gate.summarize([-2,-1,6])
    assert result["average_net_pips"]==1
    assert result["average_without_best_pips"]==-1.5
