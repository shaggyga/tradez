from decimal import Decimal,Context,localcontext
import pytest
import summarize_matched_joint_dependence_v1 as s


def record(index,joint,price,neutral,actual=1):
    predictions={s.FAMILY:Decimal(joint),'matched_price_only':Decimal(price),'neutral_news_ablation':Decimal(neutral)}
    return dict(instrument='GBP_USD',decision_id=str(index),reference_epoch=index,target_epoch=index+3600,
        news_capture_sha256='shared',feature_cutoff_epoch=100,actual=Decimal(actual),predictions=predictions,
        scores={name:dict(side=(v>0)-(v<0),net_bps=str((v>0)-(v<0)),absolute_error_bps=str(abs(v-actual))) for name,v in predictions.items()})


def test_all_nine_direction_cells_retain_neutral_and_all_row_denominator():
    rows=[record(i,a,b,b) for i,(a,b) in enumerate((a,b) for a in (-1,0,1) for b in (-1,0,1))]
    result=s.summarize(rows)['comparisons']['matched_price_only']
    assert result['common_valid_denominator']==9 and result['same_direction_count']==3 and result['different_direction_count']==6
    assert all(x['count']==1 for x in result['direction_pairs'])


def test_descriptive_correlation_constant_is_missing_not_measured_zero():
    with localcontext(Context(prec=80)):
        assert s.correlation([Decimal(1)]*2,[Decimal(2),Decimal(3)]) is None
        assert s.correlation([Decimal(1),Decimal(2)],[Decimal(3),Decimal(2)])=='-1'


def test_low_ambient_precision_does_not_change_supplement():
    rows=[record(0,'0.123456789','0.987654321','-0.123456789'),record(1,'0.123456790','-0.987654321','0.123456789')]
    expected=s.summarize(rows)
    with localcontext(Context(prec=3)):assert s.summarize(rows)==expected


def test_repeated_snapshots_cannot_inflate_original_decision_count():
    row=record(0,1,1,1)
    with pytest.raises(ValueError,match='unique_nonempty'):s.summarize([row,row])


def test_retained_evidence_tamper_rejects(tmp_path,monkeypatch):
    monkeypatch.setattr(s,'BASE',tmp_path);path=tmp_path/'evidence.json';path.write_bytes(b'changed')
    with pytest.raises(ValueError,match='evidence_hash'):s.read_bound(path,s.digest(b'original'),1024)


@pytest.mark.parametrize('value',[None,True,'NaN'])
def test_missing_or_invalid_numeric_evidence_is_not_zero(value):
    with pytest.raises(ValueError):s.number(value)
