"""Authenticated retained-tier observations, not native conditional curves.

Reuses saved publications, original observer identities and the management quote
validator. No model loading, inference, training, live I/O or order authority.
The CLI replays a frozen capture clock, never represents it as available now.
"""
from pathlib import Path
import argparse
from collections import Counter
from copy import deepcopy
from decimal import Context, localcontext
import json
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'tools'),str(ROOT/'trad')]
import forex_retained_management_readiness as prior
import oanda_forecast_curve_contract_v1 as native
from oanda_curve_management_replay_v1 import quote_at

SCHEMA='retained_forecast_observation_receipt.v1'
CONSUMPTION='retained_forecast_observation_consumption.v1'
SOURCES=prior.SOURCES+['tools/forex_retained_forecast_receipts.py']
FLAGS={**native.AUTHORITY,'manager_activation':False,'models_fitted':0}


def sources():return {n:prior.sha(prior.read(ROOT/n)) for n in SOURCES}


def seal(body,key):return {**body,key:native.content_hash(body)}


class VerifiedCapture:
    """Authenticated registry/publication/observer context for receipt consumers."""
    @classmethod
    def load(cls,path,expected):
        m,data=prior.load_capture(Path(path),expected)
        return cls(m,data,expected)

    def __init__(self,m,data,expected):
        self.population,self.previous_report=prior.inspect(m,data)
        self.manifest=deepcopy(m);self.capture_sha256=expected
        self.registry=deepcopy(data['registry.json'])
        self.entries={e['id']:e for e in self.registry['connections']}
        self.now=m['capture_completed_epoch']
        self.receipts={};self.outcomes={}
        current=data['current.json']
        for anchor in data['anchors.json']:
            b=json.loads(anchor['body']);f=b['forecast']
            self._add('first_observed',f,b['publication_sha256'],b['publication_epoch'],anchor['observed'])
            # Later outcome state belongs to the evidence ledger, never a
            # candidate input or its identity; a label cannot influence pricing.
            self.outcomes[anchor['id']]={'state':anchor['state'],
                'outcome_sha256':prior.sha(prior.encoded(json.loads(anchor['outcome']))) if anchor['outcome'] else None}
        for f in current['forecasts']:
            self._add('capture_observed',f,current['payload_sha256'],current['generated_epoch'],self.now)

    def _add(self,kind,f,pub,generated,observed):
        entry=self.entries[f['connection']]
        prior.tracking.qualified_rows({'status':'current','generated_epoch':f['issued_epoch'],
            'registry_sha256':self.manifest['registry_sha256'],'connections':self.registry['connections'],
            'forecasts':[f]},f['issued_epoch'])
        prior.need(f['original_model_id']==entry.get('original_model_id'),'original_model_identity')
        prior.need(f['target_semantics'] in ('completed_M1_midpoint_elapsed_return','exact_completed_M1_midpoint_elapsed_return'),
                   'unsupported_target_semantics')
        for field in ('registry_sha256','feature_hash','input_hash'):
            native._hash_text(f[field])
        if f.get('panel_sha256') is not None:native._hash_text(f['panel_sha256'])
        prior.need(f['issued_epoch']<=generated<=observed<=self.now,'retained_observation_clock')
        fid=prior.tracking.prediction_key(f)
        body=dict(schema=SCHEMA,input_tier='authenticated_retained_publication_observation',
            capture_manifest_sha256=self.capture_sha256,registry_sha256=self.manifest['registry_sha256'],
            source_bindings=self.manifest['source_bindings'],observation_kind=kind,
            forecast_id=fid,forecast=deepcopy(f),model_definition_sha256=native.content_hash(entry),
            original_publication_sha256=pub,producer_generated_epoch=generated,
            original_observed_epoch=observed,
            publication_completion_epoch=None,publication_clock_scope='producer_generation_and_actual_observation_only',
            target_contract={'forecast_target_epoch':f['target_epoch'],'forecast_target_semantics':f['target_semantics'],
                'tracker_settlement':'first_completed_M1_close_at_or_after_elapsed_target',
                'tracker_settlement_max_delay_exclusive_sec':60,'broker_fill':False},**FLAGS)
        receipt=seal(body,'receipt_sha256')
        key=kind,f['connection'],f['instrument']
        prior.need(key not in self.receipts,'receipt_population_duplicate')
        self.receipts[key]=receipt

    def validate(self,receipt):
        prior.need(isinstance(receipt,dict),'receipt_shape')
        native._verify(receipt,'receipt_sha256')
        f=receipt['forecast'];key=receipt['observation_kind'],f['connection'],f['instrument']
        prior.need(self.receipts.get(key)==receipt,'receipt_differs_from_authenticated_source')
        return deepcopy(receipt)

    def consume(self,receipt,*,observed_epoch):
        r=self.validate(receipt);observed=native.epoch(observed_epoch)
        prior.need(observed>=r['original_observed_epoch'],'consumption_before_retained_observation')
        return seal(dict(schema=CONSUMPTION,receipt_sha256=r['receipt_sha256'],
            original_observed_epoch=r['original_observed_epoch'],observed_epoch=observed,
            available_epoch=observed,scope='observed_copy_not_policy_admission',**FLAGS),'consumption_sha256')

    def candidate(self,receipt,consumption,*,decision_epoch,target_epoch,quote=None,anchor=None):
        r=self.validate(receipt);c=self.consume(r,observed_epoch=consumption['observed_epoch'])
        prior.need(c==consumption,'consumption_semantic_mismatch')
        decision,target=map(native.epoch,(decision_epoch,target_epoch));f=r['forecast']
        prior.need(decision>=c['available_epoch'],'decision_precedes_consumption')
        result=dict(schema='retained_management_observation_candidate.v1',status='unavailable',
            receipt_sha256=r['receipt_sha256'],consumption_sha256=c['consumption_sha256'],
            forecast_id=r['forecast_id'],instrument=f['instrument'],connection=f['connection'],
            original_reference_epoch=f['reference_epoch'],original_target_epoch=f['target_epoch'],
            decision_epoch=decision,requested_target_epoch=target,conditional_value_qualified=False,
            action_eligible=False,**FLAGS)
        def refuse(reason):return seal({**result,'reason':reason},'candidate_sha256')
        if target!=f['target_epoch']:return refuse('exact_original_target_required')
        if decision>=target:return refuse('target_elapsed')
        if not 0<=decision-f['reference_epoch']<=180:return refuse('retained_reference_stale')
        if not 0<=decision-r['producer_generated_epoch']<=120:return refuse('retained_publication_stale')
        relation='new_entry_forecast_observation'
        if anchor is not None:
            a=self.validate(anchor);old=a['forecast']
            prior.need(a['original_observed_epoch']<=decision,'anchor_not_observed_at_decision')
            if old['instrument']!=f['instrument'] or old['target_epoch']!=target:return refuse('incumbent_terminal_mismatch')
            if not old['reference_epoch']<f['reference_epoch'] or old['input_hash']==f['input_hash']:
                return refuse('fresh_same_terminal_input_required')
            entry,previous=self.entries[f['connection']],self.entries[old['connection']]
            if not (entry.get('kind')==previous.get('kind')=='legacy26_matched'
                and entry.get('arm')==previous.get('arm')
                and entry.get('feature_names')==previous.get('feature_names')
                and old['target_semantics']==f['target_semantics']):return refuse('same_family_contract_required')
            relation='same_terminal_updated_forecast_observation'
        # Only translate original signed basis points to their original terminal
        # price. Do NOT subtract a realized move or call this remaining value.
        with localcontext(Context(prec=80)):
            reference=native.decimal_number(f['reference_mid'],positive=True)
            terminal=reference*(1+native.decimal_number(f['expected_return_bps'])/10000)
        if terminal<=0:return refuse('nonpositive_terminal_estimate')
        result.update(status='forecast_observation_available',reason=None,relation=relation,
            reference_mid=f['reference_mid'],original_expected_return_bps=f['expected_return_bps'],
            original_terminal_price_estimate=native.decimal_text(terminal),
            numeric_scope='decimal_translation_of_original_saved_numbers_not_original_quote_precision',
            input_hash=f['input_hash'],model_definition_sha256=r['model_definition_sha256'],
            target_contract=r['target_contract'],pricing_status='quote_missing')
        if quote is not None:
            try:
                validated=quote_at({f['instrument']:quote},f['instrument'],decision,30)
                result.update(pricing_status='quote_validated',quote_id=validated['quote_id'],
                    bid=native.decimal_text(validated['bid']),ask=native.decimal_text(validated['ask']))
            except (ValueError,TypeError,KeyError) as ex:result.update(pricing_status='quote_refused',quote_reason=str(ex))
        return seal(result,'candidate_sha256')


def inspect(context):
    rows=[];counts=Counter();anchors={(r['forecast']['connection'],r['forecast']['instrument']):r
        for r in context.receipts.values() if r['observation_kind']=='first_observed'}
    current={(r['forecast']['connection'],r['forecast']['instrument']):r
        for r in context.receipts.values() if r['observation_kind']=='capture_observed'}
    for base in context.population:
        k=base['connection'],base['instrument'];r=current.get(k);a=anchors.get(k)
        c=context.consume(r,observed_epoch=context.now) if r else None
        candidate=context.candidate(r,c,decision_epoch=context.now,target_epoch=r['forecast']['target_epoch']) if r else None
        if candidate:counts[candidate['status']]+=1
        alternatives=[]
        if a:
            for name in base['potential_same_family_terminal_matches']:
                alt=current[name,k[1]];ac=context.consume(alt,observed_epoch=context.now)
                alternatives.append(context.candidate(alt,ac,decision_epoch=context.now,target_epoch=a['forecast']['target_epoch'],anchor=a))
        rows.append(dict(connection=k[0],instrument=k[1],coverage_status=base['coverage_status'],
            original_observation_receipt=a,current_observation_receipt=r,consumption=c,candidate=candidate,
            same_terminal_observations=alternatives,
            outcome_evidence_not_decision_input=context.outcomes.get(a['forecast_id']) if a else None))
    report=dict(schema=SCHEMA,population=len(rows),receipts=len(context.receipts),first_observed=len(anchors),
        current_observed=len(current),candidate_statuses=dict(counts),
        same_terminal_observations=sum(len(r['same_terminal_observations']) for r in rows),
        replay_epoch=context.now,scope='frozen captured observation replay; not a new issuance or fresh current availability',
        native_gate_unchanged=context.previous_report['existing_native_policy_gate'],**FLAGS)
    return rows,report


def run(path,expected,output,run_id,*,resume=False,max_new=None):
    prior.need(max_new is None or type(max_new) is int and max_new>0,'positive_chunk_limit')
    context=VerifiedCapture.load(path,expected);rows,report=inspect(context)
    chunks=[rows[i:i+32] for i in range(0,len(rows),32)]
    required={f'rows_{i:04d}.json' for i in range(len(chunks))}|{'REPORT.json'}
    identity=prior.effective_run_identity(contract={'schema':SCHEMA,'capture_manifest':expected,
       'population':len(rows),'required_payloads':sorted(required),'rows_per_payload':32},dependency_hashes=sources())
    pub=prior.RunPublisher(output,run_id,identity)
    if (pub.root/'COMPLETION_MANIFEST.json').exists():
        prior.verify_completed_run(pub.root,identity);return {'status':'verified_completed'}
    pub.acquire(recover=resume);completed=False;payloads=[];added=0
    try:
        for index,chunk in enumerate(chunks):
            name=f'rows_{index:04d}.json'
            if pub.read_verified_payload(name) is None:
                if max_new is not None and added>=max_new:return {'status':'checkpointed','next_row':index*32}
                added+=1
            payloads.append(pub.write_or_validate_payload(name,prior.encoded(chunk)))
        payloads.append(pub.write_or_validate_payload('REPORT.json',prior.encoded(report)))
        pub.complete(payloads,required);completed=True
    finally:
        if not completed:pub.release()
    return {'status':'completed',**report}


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input',type=Path,required=True);p.add_argument('--sha256',required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--run-id',default='receipts')
    p.add_argument('--resume',action='store_true');p.add_argument('--max-new',type=int)
    a=p.parse_args();print(json.dumps(run(a.input,a.sha256,a.output,a.run_id,resume=a.resume,max_new=a.max_new)))
