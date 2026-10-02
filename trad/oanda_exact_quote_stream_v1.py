"""Opt-in exact receipt attachment to the existing no-order quote worker.

Use the staged operational profile only after its scoped rollout is authorized.
The original worker, float snapshot and raw PRICE hook remain unchanged.
"""
from pathlib import Path
import argparse
import hashlib
import sys

ROOT = Path(__file__).resolve().parent
EXPECTED = {
    'oanda_practice_quote_stream.py': 'f46f5aef1be8ea7a925b7b48cd8a138a06b260b23bccc4c3382c52cac3f04b9d',
    'oanda_practice_shadow_strategy_lab.py': '72d9632e44fec4b6db6a10b676e2fe0191a9e171393a96b8345aa3162142568b',
    'oanda_exact_quote_receipts_v1.py': 'd5d48ea688562e0b6a5e655b4ccbc8c3d4a9fd0a1ffca1f08cddc371eebceb3b',
    'oanda_quote_transport.py': '27ea11bc79a3a85cc09f00a88df697f0ecf8bba29b230c309cdf83e8967d8987',
}


def verify_sources(root=ROOT):
    for name, pin in EXPECTED.items():
        p=root/name
        if p.is_symlink() or hashlib.sha256(p.read_bytes()).hexdigest()!=pin:
            raise ValueError('unreviewed_quote_dependency:'+name)


def stream_class(base, publisher, path):
    """Attach before startup, expose transport health, drain after stream stop."""
    class ExactStream(base):
        def __init__(self, *args, **kwargs):
            if kwargs.get('raw_price_observer') is not None:
                raise ValueError('existing_raw_observer_must_not_be_replaced')
            legacy=kwargs.get('research_snapshot_path')
            if legacy is not None and Path(legacy).resolve()==Path(path).resolve():
                raise ValueError('separate_exact_receipt_path_required')
            instruments=kwargs.get('instruments') if 'instruments' in kwargs else args[1]
            self.exact_receipts=publisher(Path(path), instruments)
            try:
                super().__init__(*args, **dict(kwargs,raw_price_observer=self.exact_receipts))
            except BaseException:
                self.exact_receipts.close()
                raise

        def stats(self):
            return {**super().stats(), 'exact_receipts':dict(
                session_id=self.exact_receipts.session_id,
                connection_generation=self.exact_receipts.generation,
                quote_count=len(self.exact_receipts.quotes),
                refusal_count=len(self.exact_receipts.refusals),
                transport=self.exact_receipts.publisher.stats())}

        def stop(self):
            try:
                super().stop()
            finally:
                self.exact_receipts.close()
    return ExactStream


def main(argv=None):
    parser=argparse.ArgumentParser(add_help=False)
    parser.add_argument('--exact-quote-output',type=Path,required=True)
    args,remaining=parser.parse_known_args(argv)
    verify_sources()  # Check reviewed attachment targets before loading the worker.
    sys.path.insert(0,str(ROOT))
    import oanda_practice_quote_stream as original
    from oanda_exact_quote_receipts_v1 import ExactQuoteReceiptPublisher
    base=original.lab.MultiPriceStream
    original.lab.MultiPriceStream=stream_class(base,ExactQuoteReceiptPublisher,args.exact_quote_output)
    try:
        return original.main(remaining)
    finally:
        original.lab.MultiPriceStream=base


if __name__=='__main__':
    raise SystemExit(main())
