"""Isolated, exact-byte CAS and bounded streaming revision validation.

No network, credentials, production paths or historical availability synthesis.
Canonical envelope bytes are restored exactly; JSON-string lexical bytes are
never normalized. Immutable owned captures keep at most one expanded batch.
"""
import copy
import hashlib
import json
import math
import time
import weakref
import zlib
from collections.abc import Mapping, Sequence
from types import MappingProxyType
import projection_revision_reader_v1 as reader

SCHEMA = 'exact_projection_cas_v2_20260914'
MAX_OBJECT = 18 * 1024 * 1024
MAX_COMPRESSED = 192 * 1024 * 1024
MAX_EXPANDED_PREFIX = 2 * 1024 * 1024 * 1024
MAX_OBJECTS = 20000
MAX_INDEX_COMPRESSED = 32 * 1024 * 1024
MAX_INDEX_EXPANDED = 256 * 1024 * 1024
need = reader.require


def frame(body):
    need(type(body) is bytes and len(body) <= MAX_OBJECT, 'cas_object_bound')
    return len(body), hashlib.sha256(body).hexdigest(), zlib.compress(body, 6)


def expand(value):
    size, key, packed = value
    need(type(size) is int and 0 <= size <= MAX_OBJECT and type(packed) is bytes
         and len(packed) <= MAX_OBJECT, 'typed_cas_frame_required')
    decoder = zlib.decompressobj()
    raw = decoder.decompress(packed, size + 1)
    need(len(raw) == size and decoder.eof and not decoder.unconsumed_tail
         and not decoder.unused_data, 'exact_single_cas_frame_required')
    need(hashlib.sha256(raw).hexdigest() == key, 'cas_original_digest_invalid')
    return raw


def value_frame(value): return frame(reader.encoded(value))
def value_expand(value): return json.loads(expand(value))


class EvidenceStore(Mapping):
    """Compressed immutable mapping; returns newly decoded original objects."""
    __slots__ = ('__weakref__',)
    __hash__ = object.__hash__
    __eq__ = object.__eq__
    def __new__(cls, *a, **k): raise ValueError('owned_evidence_factory_required')
    def __getitem__(self, key): return value_expand(_evidence(self)[key])
    def __iter__(self): return iter(_evidence(self))
    def __len__(self): return len(_evidence(self))


class Batches(Sequence):
    __slots__ = ('__weakref__',)
    def __new__(cls, *a, **k): raise ValueError('owned_batches_factory_required')
    def __len__(self): return len(_batches(self)[0])
    def __getitem__(self, index):
        descriptors, evidence, profile = _batches(self)
        if isinstance(index, slice): return [self[i] for i in range(*index.indices(len(self)))]
        item = json.loads(descriptors[index]); snapshot = item['snapshot']
        source = json.loads(profile)
        need(item.pop('source_identity_sha256') == reader.sha([source['policy'],source['input_identity']]), 'batch_source_binding_invalid')
        snapshot.update(source)
        snapshot['entries'] = [evidence[key] for key in item.pop('evidence_sha256')]
        return item
    def __add__(self, other): return iter_chain(self, other)


class Published(Mapping):
    __slots__ = ('__weakref__',)
    __hash__ = object.__hash__
    __eq__ = object.__eq__
    def __new__(cls, *a, **k): raise ValueError('owned_publication_factory_required')
    def __getitem__(self, key):
        metadata, batches, *_ = _published(self)
        return batches if key == 'batches' else json.loads(metadata[key])
    def __iter__(self): return iter((*_published(self)[0], 'batches'))
    def __len__(self): return len(_published(self)[0]) + 1


def registry(cls):
    values = weakref.WeakKeyDictionary()
    def make(data):
        obj = object.__new__(cls); values[obj] = data; return obj
    def get(obj):
        need(type(obj) is cls and obj in values, 'exact_owned_capture_required')
        return values[obj]
    return make, get


_make_evidence, _evidence = registry(EvidenceStore)
_make_batches, _batches = registry(Batches)
_make_published, _published = registry(Published)


def owned_evidence(frames):
    need(len(frames) <= MAX_OBJECTS, 'cas_object_count_bound')
    need(sum(len(v[2]) for v in frames.values()) <= MAX_COMPRESSED, 'cas_compressed_byte_bound')
    need(sum(v[0] for v in frames.values()) <= MAX_EXPANDED_PREFIX, 'cas_expanded_work_bound')
    # Tuples and bytes are immutable. No supplied dict remains owned by callers.
    return _make_evidence(MappingProxyType(dict(frames)))


def iter_chain(first, second):
    yield from first
    yield from second


def compact_batch(envelope):
    original = envelope['snapshot']
    snapshot = {k: v for k, v in original.items() if k not in ('entries','policy','input_identity')}
    refs = [reader.sha(e) for e in envelope['snapshot']['entries']]
    return reader.encoded({'snapshot': snapshot, 'admission': envelope['admission'], 'evidence_sha256': refs,
                           'source_identity_sha256':reader.sha([original['policy'],original['input_identity']])})


def _seal_validated_publication(metadata, descriptors, frames, members):
    evidence = owned_evidence(frames)
    batches = _make_batches((tuple(descriptors), evidence, reader.encoded({k:metadata['profile'][k] for k in ('policy','input_identity')})))
    # Member payloads are independently compressed; evidence only via its hash.
    compressed = sum(len(record[2][2]) for record in members)
    expanded = sum(record[2][0] for record in members)
    need(compressed <= MAX_INDEX_COMPRESSED and expanded <= MAX_INDEX_EXPANDED, 'compact_member_index_bound')
    encoded = reader.encoded(metadata)
    fields = MappingProxyType({key:reader.encoded(value) for key,value in metadata.items()})
    return _make_published((fields, batches, tuple(members), hashlib.sha256(encoded).hexdigest(), len(encoded)))


def member_records(publication): return tuple(record[:3] for record in _published(publication)[2])
def complete_member_records(publication): return _published(publication)[2]
def header_records(publication): return tuple((event,sequence,header) for event,sequence,_,header in _published(publication)[2])
def batch_descriptor(publication, index): return _batches(_published(publication)[1])[0][index]
def evidence_frames(publication): return dict(_evidence(_batches(_published(publication)[1])[1]))
def evidence_store(publication): return _batches(_published(publication)[1])[1]


def manifest(publication):
    metadata, batches, members, metadata_sha, metadata_bytes = _published(publication)
    descriptors, evidence, _ = _batches(batches)
    frames = _evidence(evidence)
    value = {'schema_version': SCHEMA, 'metadata_sha256': metadata_sha,
             'batch_descriptor_sha256': [hashlib.sha256(b).hexdigest() for b in descriptors],
             'evidence': [(key, f[0], hashlib.sha256(f[2]).hexdigest()) for key, f in sorted(frames.items())],
             'member_frames': [(event, sequence, f[0], f[1],hashlib.sha256(header).hexdigest()) for event, sequence, f,header in members]}
    encoded = reader.encoded(value)
    return {'manifest_sha256': hashlib.sha256(encoded).hexdigest(), 'manifest_bytes': len(encoded),
            'retained_compressed_evidence_bytes': sum(len(f[2]) for f in frames.values()),
            'expanded_evidence_bytes_verified': sum(f[0] for f in frames.values()),
            'retained_compressed_member_bytes': sum(len(f[2]) for _, _, f,_ in members),
            'evidence_count': len(frames)}


class StreamValidator:
    """Reader v1's exact ordered checks, incrementally over unique durable rows.

    Unlike the pure reader's unordered supplier API, duplicate batch delivery
    is invalid here: SQLite publications have unique contiguous sequences.
    """
    def __init__(self, deadline):
        self.deadline = deadline; self.last_batch = 0; self.last_through = 0
        self.last_available = -math.inf; self.stream = None; self.policy_sha = None
        self.expanded = 0; self.seen = set()

    @classmethod
    def continue_after(cls, publication, deadline):
        """Extend an owned fully validated prefix; no arbitrary checkpoint input."""
        _published(publication)
        validator = cls(deadline)
        state = publication['validation_state']
        validator.last_batch = state['last_batch']; validator.last_through = state['last_through']
        validator.last_available = state['last_available'] if state['last_available'] is not None else -math.inf
        validator.stream = state['stream']; validator.policy_sha = state['policy_sha']
        validator.expanded = state['expanded']
        # Every new sequence must exceed the old through cursor, so the prior
        # set cannot intersect the strictly increasing new sequences.
        return validator

    def state(self):
        return {'last_batch':self.last_batch,'last_through':self.last_through,
                'last_available':self.last_available if math.isfinite(self.last_available) else None,
                'stream':self.stream,'policy_sha':self.policy_sha,'expanded':self.expanded}

    def add(self, envelope):
        need(time.monotonic() <= self.deadline, 'stream_validation_time_bound')
        snapshot = envelope['snapshot']; admission = envelope['admission']
        reader._validate_snapshot_shape(snapshot)
        need(snapshot.get('snapshot_sha256') == reader.sha({k:v for k,v in snapshot.items() if k != 'snapshot_sha256'})
             and snapshot.get('schema_version') == reader.SCHEMA and snapshot.get('complete_declared_prefix') is True
             and snapshot.get('admitted') is False, 'snapshot_binding_invalid')
        need(admission.get('schema_version') == reader.ADMISSION and admission.get('status') == 'admitted'
             and admission.get('availability_basis') == 'externally_supplied_durable_admission_readback', 'admission_required')
        need(admission.get('admission_sha256') == reader.sha({k:v for k,v in admission.items() if k != 'admission_sha256'})
             and admission.get('snapshot_sha256') == snapshot['snapshot_sha256'], 'admission_snapshot_mismatch')
        reader._integer(admission['through_seq'], 'admission_through_integer_required')
        if admission['first_projection_seq'] is not None:
            reader._integer(admission['first_projection_seq'], 'admission_first_integer_required', 1)
        sequence = admission['batch_sequence']
        need(type(sequence) is int and sequence == self.last_batch + 1 and sequence <= reader.MAX_BATCHES,
             'complete_admission_prefix_required')
        batchid = reader.sha([reader.ADMISSION, reader.sha(snapshot['input_identity']), sequence, snapshot['snapshot_sha256']])
        need(admission.get('batch_id') == batchid and admission.get('first_projection_seq') ==
             (snapshot['entries'][0]['projection']['projection_seq'] if snapshot['entries'] else None)
             and admission['through_seq'] == snapshot['through_seq'], 'admission_sequence_binding_mismatch')
        stream = reader.sha(snapshot['input_identity']); policy = reader.sha(snapshot['policy'])
        if self.stream is None: self.stream, self.policy_sha = stream, policy
        need((self.stream, self.policy_sha) == (stream, policy), 'cross_stream_or_policy_mix_refused')
        need(snapshot['after_seq'] == self.last_through, 'complete_admission_prefix_required')
        admitted = reader._number(admission['admitted_epoch']); available = reader._number(admission['admitted_available_epoch'])
        need(snapshot['read_started_epoch'] <= snapshot['read_completed_epoch'] <= admitted <= available
             and available >= self.last_available, 'admission_clock_order_invalid')
        for state, at in ((snapshot['initial_clock_state'], snapshot['read_started_epoch']),
                          (snapshot['initial_clock_state'], snapshot['read_completed_epoch']),
                          (snapshot['completion_clock_state'], snapshot['read_completed_epoch']),
                          (snapshot['initial_clock_state'], admitted), (snapshot['initial_clock_state'], available),
                          (admission['clock_state'], admitted), (admission['clock_state'], available)):
            reader.repair.validate_clock_state(state, at)
        need(snapshot['row_count'] == len(snapshot['entries']) <= reader.MAX_ROWS and
             snapshot['evidence_bytes'] == sum(len(reader.encoded(e)) for e in snapshot['entries']) <= reader.MAX_BYTES,
             'snapshot_size_mismatch')
        self.expanded += snapshot['evidence_bytes']
        need(self.expanded <= MAX_EXPANDED_PREFIX, 'stream_expanded_work_bound')
        prior = snapshot['after_seq']; members = []
        for evidence in snapshot['entries']:
            need(time.monotonic() <= self.deadline, 'stream_validation_time_bound')
            p = evidence['projection']; number = p['projection_seq']
            need(type(number) is int and prior < number <= snapshot['through_seq'] and number not in self.seen,
                 'duplicate_or_unordered_projection_sequence')
            self.seen.add(number); prior = number
            payload = reader.validate_projection(evidence, snapshot['policy'], snapshot['read_started_epoch'])
            member = {'canonical_event_id':p['canonical_event_id'], 'projection_seq':number,
                      'projection_id':p['projection_id'], 'payload':payload, 'evidence_sha256':reader.sha(evidence),
                      'payload_original_json_sha256':hashlib.sha256(p['payload_json'].encode()).hexdigest(),
                      'batch_id':batchid, 'snapshot_sha256':snapshot['snapshot_sha256'],
                      'admission_sha256':admission['admission_sha256'], 'admitted_available_epoch':available}
            member_sha = reader.sha(member)
            descriptor = {k:v for k,v in member.items() if k != 'payload'}
            descriptor.update(payload_canonical_sha256=reader.sha(payload), member_sha256=member_sha)
            member_frame = value_frame(descriptor)
            keys = ('event_id','relevant','structured_event','currency_scores','observed_available_utc',
                    'published_utc','first_seen_utc','causal_known_utc','detail_available_utc','numeric_causal_known_utc',
                    'publication_clock_known_utc','classification_observation_contract','classification_clock_status',
                    'classification_first_known_utc','classification_available_utc','source_evidence_contract',
                    'source_evidence_content_sha256','source_evidence_available_utc')
            header = {k:v for k,v in member.items() if k != 'payload'}
            header.update(payload={k:copy.deepcopy(payload.get(k)) for k in keys},
                          canonical_payload_sha256=reader.sha(payload), member_sha256=member_sha)
            members.append((p['canonical_event_id'], sequence, member_frame, reader.encoded(header)))
        need(prior == snapshot['through_seq'], 'through_sequence_mismatch')
        self.last_batch, self.last_through, self.last_available = sequence, snapshot['through_seq'], available
        return members


def validate_stream(batches, deadline):
    validator = StreamValidator(deadline)
    for batch in batches: validator.add(batch)
    return validator


def validate_extension(publication, envelope, deadline):
    need(reader.encoded(envelope['snapshot']['previous_checkpoint']) ==
         reader.encoded(publication['head']['checkpoint']), 'exact_previous_checkpoint_required')
    return StreamValidator.continue_after(publication, deadline).add(envelope)
