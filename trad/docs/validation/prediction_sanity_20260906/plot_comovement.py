"""Render descriptive currency and predictor correlations from frozen receipts."""
from pathlib import Path
import json
import os

OUT=Path(__file__).resolve().parent/'comovement'
os.environ['MPLCONFIGDIR']=str(OUT/'matplotlib_cache')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

curr=json.loads((OUT/'currency_comovement.json').read_text())
models=json.loads((OUT/'model_output_comovement.json').read_text())
currency=np.array(curr['matrices']['1']['correlation'])
model=np.eye(4)
for pair in models['pairs']:
    i,j=[models['families'].index(f) for f in pair['families']]
    model[i,j]=model[j,i]=pair['probability_correlation']
plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10})
fig,axes=plt.subplots(1,2,figsize=(12,6.8),gridspec_kw={'width_ratios':[1.3,1]})
fig.patch.set_facecolor('#f7f8fa')
panels=[(axes[0],currency,['EUR','GBP','AUD','NZD','JPY','CHF','CAD'],
         'Currencies move together','Minute returns against USD · 6,739 shared observations'),
        (axes[1],model,['Graph','Tabular','State','Ridge'],
         'Model predictions differ','Up-probability outputs · 368 shared decisions')]
for ax,data,labels,title,subtitle in panels:
    im=ax.imshow(data,vmin=-1,vmax=1,cmap='RdBu_r')
    ax.set_xticks(range(len(labels)),labels)
    ax.set_yticks(range(len(labels)),labels)
    ax.tick_params(length=0)
    ax.set_title(title+'\n'+subtitle,fontsize=11,pad=17,loc='left')
    for i in range(len(labels)):
        for j in range(len(labels)):
            ax.text(j,i,f'{data[i,j]:.2f}',ha='center',va='center',fontsize=9,
                    color='white' if abs(data[i,j])>.65 else '#17212b')
    for spine in ax.spines.values():spine.set_visible(False)
fig.suptitle('Co-movement is present; a useful forecast edge is still unproven',x=.04,ha='left',y=.98,fontsize=16,fontweight='bold')
fig.subplots_adjust(left=.06,right=.91,top=.78,bottom=.23,wspace=.35)
cbar_ax=fig.add_axes([.93,.28,.018,.42])
fig.colorbar(im,cax=cbar_ax)
cbar_ax.set_title('r',fontsize=10,pad=10)
fig.text(.04,.12,'Fixed archive: Aug 30–Sep 4, 2026. Returns are signed so positive means the non-USD currency strengthens.',fontsize=10)
fig.text(.04,.075,'Same-time correlation does not show which pair leads. The model comparison retains historical timing defects.',fontsize=10)
fig.text(.04,.032,'Descriptive diagnostics only. No model was fitted, selected, activated or authorized to trade.',fontsize=10,color='#505b66')
fig.savefig(OUT/'comovement_sanity.png',dpi=160,facecolor=fig.get_facecolor())
fig.savefig(OUT/'comovement_sanity.svg',facecolor=fig.get_facecolor())
print(OUT/'comovement_sanity.png')
