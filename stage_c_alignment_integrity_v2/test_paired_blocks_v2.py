import copy
import numpy as np
import pytest
from paired_blocks_v2 import paired_origins,block_indices,describe

def fixture():
    a=[];b=[];out=[]
    for t in range(1,21):
        for pair in ('EUR_USD','GBP_USD'):
            key=pair+str(t);f={'target_id':'target','decision_epoch':t*21600,'available_epoch':t*21600+2,'instrument':pair,'prediction':3.}
            a.append({'record_id':key,'procedure':'frozen','forecast':f});g={**f,'prediction':0.};b.append({'record_id':key,'procedure':'frozen','forecast':g})
            out.append({'record_id':key,'target_id':'target','value':2.,'available_epoch':t*21600+60,'label_end_epoch':t*21600+60})
    return a,b,out

def test_hand_calculated_errors_and_whole_panel_counts():
    panels,scope=paired_origins(*fixture(),asof=999999)
    assert scope['raw_paired_rows']==40 and scope['unique_origins']==20 and scope['maximum_label_span_seconds']==60
    assert all(p['rows']==2 and p['absolute_error_delta_sum']==-2 and p['squared_error_delta_sum']==-6 for p in panels)
    r=describe(panels,4);assert r['original_deltas']=={'absolute_error_delta_sum':-1.,'squared_error_delta_sum':-3.}
    assert r['interval']['absolute_error_delta_sum']=={'lower_2_5':-1.,'upper_97_5':-1.} and r['effective_sample_size'] is None

def test_joint_indices_are_deterministic_contiguous_blocks():
    a=block_indices(20,4);b=block_indices(20,4);np.testing.assert_array_equal(a,b)
    assert a.shape==(2000,20) and np.all(np.diff(a.reshape(2000,5,4),axis=2)==1)
    assert a.min()>=0 and a.max()<20

def test_full_window_and_empty_have_no_fabricated_interval():
    panels,_=paired_origins(*fixture(),asof=999999)
    assert describe(panels,20)['status']=='insufficient_distinct_time_blocks' and describe(panels,20)['interval'] is None
    assert describe([],4)['status']=='no_mature_paired_rows'

def test_pair_support_mismatch_and_duplicate_refused():
    a,b,out=fixture()
    with pytest.raises(ValueError,match='exact_paired_forecast_support'):paired_origins(a,b[:-1],out,asof=999999)
    with pytest.raises(ValueError,match='duplicate_paired'):paired_origins(a+[a[0]],b,out,asof=999999)

def test_clock_mismatch_and_unavailable_forecast_refused():
    a,b,out=fixture();b[0]['forecast']['available_epoch']+=1
    with pytest.raises(ValueError,match='clocks_or_instrument'):paired_origins(a,b,out,asof=999999)
    a,b,out=fixture()
    with pytest.raises(ValueError,match='forecast_not_available'):paired_origins(a,b,out,asof=1)

def test_labels_are_mature_and_future_values_not_examined():
    a,b,out=fixture();out[0]['available_epoch']=1000000;out[0]['value']={'future':'poison'}
    panels,scope=paired_origins(a,b,out,asof=999999);assert scope['raw_paired_rows']==39 and panels[0]['rows']==1
    out[0]['available_epoch']=1;out[0]['value']=2.
    with pytest.raises(ValueError,match='cannot_precede'):paired_origins(a,b,out,asof=999999)

def test_variable_panel_sizes_preserve_row_weighting():
    panels=[{'origin_epoch':1,'rows':1,'absolute_error_delta_sum':10.,'squared_error_delta_sum':100.},{'origin_epoch':2,'rows':9,'absolute_error_delta_sum':0.,'squared_error_delta_sum':0.}]
    r=describe(panels,1);assert r['original_deltas']['absolute_error_delta_sum']==1.

def test_shifted_equal_length_grids_are_not_same_joint_draw():
    panels,_=paired_origins(*fixture(),asof=999999)
    shifted=[{**p,'origin_epoch':p['origin_epoch']+21600} for p in panels]
    a,b=describe(panels,4),describe(shifted,4)
    assert a['indices_sha256']==b['indices_sha256']
    assert a['origin_grid_sha256']!=b['origin_grid_sha256']
    assert a['joint_draw_identity']!=b['joint_draw_identity']

def test_missing_interior_origin_is_not_compressed_into_regular_time():
    a,b,out=fixture()
    for o in out:
        if o['label_end_epoch']==5*21600+60:o['value']=None
    panels,scope=paired_origins(a,b,out,asof=999999)
    assert scope['origins_without_mature_rows']==[5*21600]
    assert scope['excluded_outcome_rows']=={'unavailable_endpoint_value':2}
    r=describe(panels,4)
    assert r['status']=='irregular_origin_grid_requires_calendar_block_design'
    assert r['origin_spacing_seconds']==[21600,43200] and r['interval'] is None and r['replicates']==0

@pytest.mark.parametrize('length',[0,-1,True])
def test_invalid_block_lengths(length):
    with pytest.raises(ValueError):block_indices(20,length)
