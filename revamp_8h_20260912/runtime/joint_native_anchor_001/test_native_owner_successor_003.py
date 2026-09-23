"""Use the exact hash-pinned production IO10 descriptor in owner fixtures."""
from pathlib import Path
import ast
import hashlib
import test_native_owner_successor_002 as prior
import oanda_causal_forecast_ledger_joint_news_v5 as ledger

def actual_descriptor():
    source=Path(__file__).parent.parent/'joint_revision_transport_001/flat_generation_001/source/revision_news_io_v10.py'
    raw=source.read_bytes()
    assert hashlib.sha256(raw).hexdigest()=='b904e0b9a62d09942e98eb92b9e784413849e60bbd5d5ad301d6fdbb8554f314'
    tree=ast.parse(raw)
    values=[ast.literal_eval(n.value) for n in tree.body if isinstance(n,ast.Assign)
        and len(n.targets)==1 and isinstance(n.targets[0],ast.Name) and n.targets[0].id=='DESCRIPTOR']
    assert len(values)==1 and type(values[0]) is str
    return values[0]

class SuccessorOwner(prior.SuccessorOwner):
    def setUp(self):
        prior.SuccessorOwner.setUp(self)
        self.io.DESCRIPTOR=actual_descriptor()
        self.c['news_capture_descriptor']['schema_version']=self.io.DESCRIPTOR
        self.c['source_capture_sha256']=ledger.digest({k:v for k,v in self.c.items() if k!='source_capture_sha256'})
        self.r['source_capture_sha256']=self.c['source_capture_sha256']
    def test_actual_io10_descriptor_is_accepted(self):
        self.assertEqual(self.io.DESCRIPTOR,'revision_news_fixed_io_descriptor_v2_20260913')
        self.issue();self.owner.consume_publications()
        evidence=self.owner.verified_native_publication()['forecasts'][0]['news_revision_evidence']
        self.assertEqual(evidence['news_capture_descriptor']['schema_version'],self.io.DESCRIPTOR)
    def test_old_descriptor_is_refused_by_fixed_new_generation(self):
        self.c['news_capture_descriptor']['schema_version']='revision_news_fixed_io_descriptor_v1_20260913'
        with self.assertRaisesRegex(ValueError,'descriptor'):self.issue()
        self.assertEqual(self.owner.counts()['forecasts'],0)
