from currency_projection_policy_report_v2 import render
def test_report_keeps_all_rows_and_no_winner_language():
    out=render({'rows':[{'method':'ridge__direct','scenario':'x','arm':'cash','selected':0,'opened':0,'selected_without_open':0,'net_account_pnl_usd':'0'}]})
    assert '| ridge__direct | x | cash |' in out and 'not a winner selection' in out
