"""Pure scheduling bookkeeping for the fixed revision-news worker seam.

These values are scheduling hints, never trusted news/input/issue authority.
The worker must use the hard-bound I/O session and original opaque capture at
each input and ledger boundary. No threads, files, attempts or fits occur here.
"""
from dataclasses import dataclass,replace
import math,re

NEWS_CAPTURE_INTERVAL_SECONDS=60


def integer(value,label):
    if type(value) is not int or value<0:raise ValueError(label+'_nonnegative_integer_required')
    return value


def monotonic(value):
    if type(value) not in (int,float) or not math.isfinite(value) or value<0:raise ValueError('finite_nonnegative_monotonic_required')
    return float(value)


def sha(value,label):
    if type(value) is not str or re.fullmatch('[0-9a-f]{64}',value) is None:raise ValueError(label+'_sha256_required')
    return value


@dataclass(frozen=True,slots=True)
class SharedNewsSchedule:
    next_capture_monotonic:float=0.
    capture_in_flight:bool=False
    failure_serial:int=0
    accepted_capture_sha256:str|None=None
    accepted_capture_failure_serial:int|None=None


def _state(state):
    if type(state) is not SharedNewsSchedule:raise ValueError('typed_news_schedule_required')
    monotonic(state.next_capture_monotonic);integer(state.failure_serial,'failure_serial')
    if type(state.capture_in_flight) is not bool:raise ValueError('typed_in_flight_flag')
    if state.accepted_capture_sha256 is None:
        if state.accepted_capture_failure_serial is not None:raise ValueError('absent_capture_has_no_generation')
    else:
        sha(state.accepted_capture_sha256,'capture');integer(state.accepted_capture_failure_serial,'capture_failure_serial')
        if state.accepted_capture_failure_serial!=state.failure_serial:raise ValueError('cached_capture_generation_mismatch')
    return state


def dispatch_shared_capture(state,now_monotonic):
    """At most one outstanding capture and one dispatch per60 elapsed seconds."""
    state=_state(state);now=monotonic(now_monotonic)
    if state.capture_in_flight or now<state.next_capture_monotonic:return state,False
    return replace(state,capture_in_flight=True,next_capture_monotonic=now+NEWS_CAPTURE_INTERVAL_SECONDS),True


def observe_failure_generation(state,failure_serial):
    """A newly observed I/O failure clears hints once, without an attempt."""
    state=_state(state);serial=integer(failure_serial,'failure_serial')
    if serial<state.failure_serial:raise ValueError('io_failure_generation_regressed')
    if serial==state.failure_serial:return state,False
    return replace(state,failure_serial=serial,accepted_capture_sha256=None,accepted_capture_failure_serial=None),True


def complete_shared_capture(state,*,capture_sha256,captured_failure_serial,current_failure_serial,current_usable):
    """Bookkeep one future completion; real fixed-I/O eligibility is mandatory.

    The supplied generation must have been observed from the exact opaque
    capture in its original session, not inferred from current usability alone.
    """
    state=_state(state)
    if not state.capture_in_flight:raise ValueError('no_shared_capture_future_to_complete')
    digest=sha(capture_sha256,'capture');serial=integer(captured_failure_serial,'capture_failure_serial')
    state,wake=observe_failure_generation(state,current_failure_serial)
    if type(current_usable) is not bool:raise ValueError('typed_session_usability')
    if not current_usable or serial!=state.failure_serial:
        return replace(state,capture_in_flight=False,accepted_capture_sha256=None,accepted_capture_failure_serial=None),False,wake
    return replace(state,capture_in_flight=False,accepted_capture_sha256=digest,accepted_capture_failure_serial=serial),True,wake


def failed_shared_capture(state,*,current_failure_serial):
    """A failed capture does not create pair attempts or advance the cadence."""
    state=_state(state)
    if not state.capture_in_flight:raise ValueError('no_shared_capture_future_to_complete')
    state,wake=observe_failure_generation(state,current_failure_serial)
    return replace(state,capture_in_flight=False,accepted_capture_sha256=None,accepted_capture_failure_serial=None),wake


def failed_fit_basis(input_capture_sha256,cadence_bucket,news_failure_serial):
    """Quote polling alone cannot turn a failed model basis into new work."""
    return (sha(input_capture_sha256,'input_capture'),integer(cadence_bucket,'cadence_bucket'),integer(news_failure_serial,'news_failure_serial'))


def pair_capture_basis(price_signature,news_capture_sha256,news_failure_serial):
    """A changed price source or completed global capture can wake preparation."""
    return (sha(price_signature,'price_signature'),sha(news_capture_sha256,'news_capture'),integer(news_failure_serial,'news_failure_serial'))
