"""Draft ready-only model prewarming; numerical caches still reset each origin."""
import time
from curve_capacity_predict_v2 import SharedCurvePredictor

class ReadyWarmPredictor(SharedCurvePredictor):
    def __init__(self,reader):
        super().__init__(reader);self.clock=None;self.cache_available={};self.failed=False

    def begin_origin(self,origin):
        if self.failed:raise ValueError('failed_prewarm_requires_new_verified_reader')
        if type(origin) is not int or self.clock is not None and origin<self.clock:raise ValueError('monotone_warm_cache_clock_required')
        self.clock=origin;super().begin_origin(origin)

    def prewarm(self,tasks,start_epoch,finish_epoch):
        if type(start_epoch) is not int or type(finish_epoch) is not int or finish_epoch-start_epoch!=120:raise ValueError('fixed_120second_prewarm_reservation_required')
        self.begin_origin(start_epoch);seen=set();rows=[];started=time.monotonic()
        try:
            for task in tasks:
                key=task['fit_id']
                if key in seen:continue
                seen.add(key)
                if task['joint_ready_epoch']>start_epoch:
                    rows.append({'fit_id':key,'status':'not_ready_at_prewarm_start','joint_ready_epoch':task['joint_ready_epoch'],'cache_available_epoch':None});continue
                existed=key in self.cache
                # Existing SharedCurvePredictor validates exact fitted transforms
                # even with no observation rows. No origin-dependent matrix is built.
                SharedCurvePredictor.predict(self,task,[],{})
                if not existed:self.cache_available[key]=finish_epoch
                rows.append({'fit_id':key,'status':'already_cached' if existed else 'prewarmed','joint_ready_epoch':task['joint_ready_epoch'],'cache_available_epoch':self.cache_available[key]})
            elapsed=time.monotonic()-started
            if elapsed>120:raise ValueError('prewarm_reservation_exceeded')
            self.clock=finish_epoch
            # Availability is the conservative reserved finish, never zero elapsed.
            return {'start_epoch':start_epoch,'reserved_finish_epoch':finish_epoch,'elapsed_seconds':elapsed,'worker_count':1,'tasks':rows,'scope':'modeled_reserved_prewarm_not_observed_historical_processing'}
        except BaseException:
            self.failed=True
            raise

    def predict(self,task,observations,views):
        if self.failed:raise ValueError('failed_prewarm_requires_new_verified_reader')
        if task['fit_id'] in self.cache_available and self.cache_available[task['fit_id']]>self.origin:raise ValueError('prewarmed_model_not_yet_available')
        existed=task['fit_id'] in self.cache
        result=super().predict(task,observations,views)
        if not existed:self.cache_available[task['fit_id']]=self.origin
        return result
