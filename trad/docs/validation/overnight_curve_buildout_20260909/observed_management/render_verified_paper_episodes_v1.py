"""Render all three verified paper episodes, without interpolating observations."""
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import time

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

BASE=Path(__file__).resolve().parent
SOURCE=BASE/'VERIFIED_THREE_EPISODE_PLOT_DATA_20260909.json'
EXPECTED='ebc2ac9c1153afdb9d3b6ff9dcb463a21f035ab0e72d409c538dc6d71102d26c'
OUTPUT=BASE/'verified_episode_figure_001'


def sha(raw):return hashlib.sha256(raw).hexdigest()


def main():
    raw=SOURCE.read_bytes()
    if sha(raw)!=EXPECTED or OUTPUT.exists():raise ValueError('fixed_input_or_fresh_output_required')
    data=json.loads(raw)
    if len(data['episodes'])!=3 or [len(e['rows']) for e in data['episodes']]!=[60,59,59]:
        raise ValueError('all_original_episodes_required')
    if any(data.get(k) is not False for k in ('GET','interpolation','new_forecasts','counterfactual_guard_pnl','runtime_writes')):
        raise ValueError('scope_mismatch')
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'axes.spines.top':False,
                         'axes.spines.right':False,'svg.fonttype':'none'})
    fig,axes=plt.subplots(2,3,figsize=(15,8.2),sharey='row',gridspec_kw={'height_ratios':[2.0,1.0]})
    fig.subplots_adjust(left=.065,right=.985,top=.80,bottom=.20,wspace=.12,hspace=.31)
    fig.suptitle('A fixed terminal estimate is not a refreshed forecast',x=.065,y=.965,ha='left',fontsize=21,fontweight='bold')
    fig.text(.065,.918,'GBP/USD · all three registered paper episodes · observations and recorded virtual positions',fontsize=12,color='#45515d')
    records=[]
    for index,episode in enumerate(data['episodes']):
        reference=episode['reference_epoch'];target=episode['original_target_epoch']
        ref=Decimal(episode['reference_price']);terminal=Decimal(episode['fixed_expected_terminal_price'])
        rows=episode['rows'];price_ax=axes[0,index];side_ax=axes[1,index]
        x=[(row['selected_quote']['available_epoch']-reference)/60 for row in rows]
        mid=[float((Decimal(row['selected_quote']['midpoint'])-ref)*10000) for row in rows]
        bid=[float((Decimal(row['selected_quote']['bid'])-ref)*10000) for row in rows]
        ask=[float((Decimal(row['selected_quote']['ask'])-ref)*10000) for row in rows]
        if not all(x[i]<=x[i+1] for i in range(len(x)-1)):raise ValueError('quote_availability_regression')
        predicted=float((terminal-ref)*10000)
        price_ax.axhline(0,color='#8a949e',linewidth=.7,zorder=1)
        price_ax.axhline(predicted,color='#bd640f',linestyle=(0,(5,4)),linewidth=1.2,zorder=1)
        price_ax.vlines(x,bid,ask,color='#6c98b6',alpha=.50,linewidth=1.15)
        price_ax.scatter(x,mid,s=16,color='#196691',zorder=3)
        price_ax.scatter([0],[0],marker='D',s=35,color='#39434c',zorder=4)
        price_ax.scatter([(target-reference)/60],[predicted],marker='*',s=135,color='#bd640f',edgecolor='white',linewidth=.7,zorder=4)
        price_ax.axvline((target-reference)/60,color='#b2b7bc',linewidth=.7,linestyle=':')
        first=datetime.fromtimestamp(reference,timezone.utc).strftime('%H:%M:%S')
        last=datetime.fromtimestamp(target,timezone.utc).strftime('%H:%M:%S')
        price_ax.set_title(f'Episode {index+1} · {first}–{last} UTC',loc='left',fontsize=11,fontweight='bold',pad=12)
        price_ax.set_xlabel('Minutes after original reference')
        final_cash={}
        for arm,color,marker,size in [('curve_hold_no_rotation','#845098','s',43),('usd_curve_manager','#087967','o',22)]:
            sx=[(row['arms'][arm]['position_state_known_epoch']-reference)/60 for row in rows]
            sy=[row['arms'][arm]['after']['side'] for row in rows]
            if not all(v in (-1,0,1) for v in sy):raise ValueError('position_side_shape')
            side_ax.scatter(sx,sy,s=size,marker=marker,facecolor='none' if arm=='curve_hold_no_rotation' else color,
                            edgecolor=color,linewidth=1.0,alpha=.85)
            final_cash[arm]=rows[-1]['arms'][arm]['after']['realized_usd']
        side_ax.set_yticks([-1,0,1],['Short','Flat','Long'])
        side_ax.set_ylim(-1.35,1.35)
        side_ax.set_xlabel('Minutes after original reference')
        side_ax.text(0,-.44,f'Realized virtual USD: manager {Decimal(final_cash["usd_curve_manager"]):+.2f} · hold {Decimal(final_cash["curve_hold_no_rotation"]):+.2f}',
                     transform=side_ax.transAxes,fontsize=9,color='#34414c')
        for ax in (price_ax,side_ax):
            ax.set_xlim(-1,62);ax.set_xticks([0,15,30,45,60]);ax.grid(axis='y',alpha=.18);ax.set_axisbelow(True)
        records.append(dict(episode_id=episode['episode_id'],observations=len(rows),reference_epoch=reference,target_epoch=target,
                            original_terminal_change_pips=str((terminal-ref)*10000),original_realized_usd=final_cash))
    axes[0,0].set_ylabel('Change from original reference (pips)')
    axes[1,0].set_ylabel('Recorded virtual position')
    handles=[Line2D([],[],color='#196691',marker='o',linestyle='none',markersize=5,label='Observed bid/ask midpoint; bars show spread'),
             Line2D([],[],color='#bd640f',linestyle='--',marker='*',markersize=8,label='Original H1 terminal value; not a price path'),
             Line2D([],[],color='#087967',marker='o',linestyle='none',label='Curve manager'),
             Line2D([],[],color='#845098',marker='s',markerfacecolor='none',linestyle='none',label='Hold original curve')]
    fig.legend(handles=handles,loc='upper left',bbox_to_anchor=(.06,.891),ncol=2,frameon=False,fontsize=10,columnspacing=2.5)
    fig.text(.065,.079,'Dots are placed when each quote or position state became available. No line fills the gaps between observations.',fontsize=10)
    fig.text(.065,.052,'Reference: official-M S5 close. Later dots: exact (bid + ask) / 2. Separate $2,500-notional paper arms; no broker fills.',fontsize=10,color='#45515d')
    fig.text(.065,.026,'All 178 scheduled steps are retained. These three overlapping-session scenarios do not establish an independent predictive edge.',fontsize=10,color='#45515d')
    OUTPUT.mkdir()
    outputs=[]
    for suffix in ('png','svg'):
        path=OUTPUT/('VERIFIED_PAPER_EPISODES_20260909.'+suffix)
        fig.savefig(path,dpi=155,facecolor='white',metadata={'Creator':'Verified Forex paper-episode renderer'})
        payload=path.read_bytes();outputs.append(dict(path=str(path),bytes=len(payload),sha256=sha(payload)))
    plt.close(fig)
    receipt=dict(schema_version='verified_paper_episode_figure_v1_20260909',rendered_epoch=time.time(),source_path=str(SOURCE),source_sha256=EXPECTED,
        renderer_sha256=sha(Path(__file__).read_bytes()),original_verifier=data['original_verifier'],episodes=records,outputs=outputs,
        scope='All three retained episodes; display only, no new scoring or interpolation. Availability/state-known time axes; fixed terminal annotation is not an implied forecast trajectory.',
        source_bytes_unchanged=SOURCE.read_bytes()==raw,broker_fills=False)
    path=OUTPUT/'FIGURE_RENDER_RECEIPT_20260909.json';path.write_text(json.dumps(receipt,sort_keys=True,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(dict(path=str(path),sha256=sha(path.read_bytes()),outputs=outputs)))


if __name__=='__main__':main()
