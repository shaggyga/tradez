import copy
import hashlib
from pathlib import Path
import tempfile
import unittest

from tools import resume_rolling_family_replication_v1 as resume


def complete_failed_header():
    ctx = {'variants': {n: {} for n in resume.EXPECTED_VARIANTS},
           'oof_models': [{}, {}, {}, {}],
           'meta_models': {n: {} for n in resume.frozen.META_ARMS}}
    return {'schema': resume.frozen.SCHEMA, 'status': 'failed',
            'failure': {'type': 'ValueError', 'message': 'window shape cannot be larger than input array shape'},
            'completed_base_bundles': 20, 'completed_variants': 20,
            'completed_comparator_fits': 8, 'completed_family_refits': 0,
            'completed_family_mean_variants': 2, 'can_place_orders': False, 'models_promoted': 0,
            'contexts': {n: copy.deepcopy(ctx) for n in resume.EXPECTED_CONTEXTS},
            'comparators': {str(i): {} for i in range(18)}}


class RecoveryContractTests(unittest.TestCase):
    def test_exact_known_failure_with_complete_main_grid(self):
        resume.validate_failed_report(complete_failed_header())

    def test_reject_other_failure_or_running_or_incomplete(self):
        changes = [('status', 'running'), ('failure', {'type': 'ValueError', 'message': 'other'}),
                   ('completed_variants', 19), ('completed_family_refits', 1),
                   ('completed_comparator_fits', 7), ('models_promoted', 1)]
        for key, value in changes:
            with self.subTest(key=key), self.assertRaises(ValueError):
                header = complete_failed_header(); header[key] = value
                resume.validate_failed_report(header)

    def test_reject_missing_model_or_variant(self):
        header = complete_failed_header()
        header['contexts']['compact50_60m']['oof_models'].pop()
        with self.assertRaises(ValueError):
            resume.validate_failed_report(header)
        header = complete_failed_header()
        header['contexts']['compact50_60m']['variants'].pop('mixture_raw')
        with self.assertRaises(ValueError):
            resume.validate_failed_report(header)

    def test_conflicting_artifact_digest(self):
        with self.assertRaises(ValueError):
            resume.referenced_files([{'path': 'models/a', 'sha256': 'a'}, {'path': 'models/a', 'sha256': 'b'}])

    def fixture(self, base):
        source = base / 'source'; source.mkdir()
        for name in resume.frozen.SUBDIRS:
            (source / name).mkdir()
        payload = b'exact preserved model and original forecast\x00\x01'
        path = source / 'models/model.bin'; path.write_bytes(payload)
        digest = hashlib.sha256(payload).hexdigest()
        report = {name: {} for name in resume.SECTIONS}
        report['contexts'] = {'nested': {'path': 'models/model.bin', 'sha256': digest}}
        return source, path, report, {'models/model.bin': digest}

    def test_reference_inventory_verified(self):
        with tempfile.TemporaryDirectory() as temp:
            source, path, report, expected = self.fixture(Path(temp))
            self.assertEqual(resume.verify_reuse(source, report), expected)

    def test_orphan_artifact_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            source, path, report, expected = self.fixture(Path(temp))
            (source / 'models/orphan.bin').write_bytes(b'orphan')
            with self.assertRaises(ValueError):
                resume.verify_reuse(source, report)

    def test_changed_source_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            source, path, report, expected = self.fixture(Path(temp))
            path.write_bytes(b'changed')
            with self.assertRaises(ValueError):
                resume.verify_reuse(source, report)

    def test_verified_copy_keeps_original_independent(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp); source, path, report, expected = self.fixture(base)
            output = base / 'destination'
            resume.copy_reuse(source, output, expected)
            self.assertEqual(path.read_bytes(), (output / 'models/model.bin').read_bytes())
            (output / 'models/model.bin').write_bytes(b'new copy changed')
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), expected['models/model.bin'])

    def test_existing_destination_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp); source, path, report, expected = self.fixture(base)
            with self.assertRaises(ValueError):
                resume.copy_reuse(source, source, expected)

    def test_changed_source_before_copy_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp); source, path, report, expected = self.fixture(base)
            path.write_bytes(b'changed after preflight')
            with self.assertRaises(ValueError):
                resume.copy_reuse(source, base / 'destination', expected)


if __name__ == '__main__':
    unittest.main()
