"""Publisher checks in isolated temporary trees, never the actual vault."""
import contextlib
import copy
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from tools import publish_rolling_period_checkpoint_v1 as publisher


def sample_plan(directory):
    project = Path(directory)/'trad'; project.mkdir()
    vault = Path(directory)/'vault'; vault.mkdir()
    original = b'\xef\xbb\xbf# Existing vault\r\nOld packages remain.\r\n'
    (vault/'README.md').write_bytes(original)
    acceptance = project/'evidence/ACCEPTANCE.json'; acceptance.parent.mkdir()
    acceptance.write_text('{"status":"accepted_research_checkpoint"}')
    module = project/'example.py'; module.write_text('VALUE = 7\n')
    guide = project/publisher.GUIDE; guide.parent.mkdir()
    guide.write_text('# Accepted study\n')
    files = []
    for p in (module, guide, acceptance):
        files.append({**publisher.checked(p), 'path': 'source/forex/trad/'+p.relative_to(project).as_posix(), 'category': 'fixture'})
    plan = {'project': str(project), 'vault': str(vault), 'package': publisher.PACKAGE,
            'files': files, 'payload_bytes': sum(r['bytes'] for r in files),
            'acceptance': publisher.checked(acceptance), 'summary_sha256': '0'*64,
            'windows': [], 'category_counts': {'fixture': 3}, 'exact_local_python_import_closure': ['example.py'],
            'predecessors': [], 'local_data_dependencies': [], 'vault_readme_before': publisher.checked(vault/'README.md')}
    return project, vault, acceptance, original, plan


class PeriodPublisherTests(unittest.TestCase):
    def test_input_chain_requires_same_base_overlay_period_and_all68(self):
        b = {'start': 0, 'train_end': 1, 'validation_end': 2, 'end': 3}
        common = {'status': 'complete', 'boundaries': b, 'pairs': {str(i): {} for i in range(68)}}
        im = {**common, 'base_sha256': 'base', 'endpoint_manifest_sha256': 'endpoint'}
        pm = {**common, 'base_sha256': 'base', 'overlay_sha256': 'endpoint'}
        qm = {**common, 'base_manifest_sha256': 'base', 'endpoint_manifest_sha256': 'endpoint'}
        em = {**common, 'base_manifest_sha256': 'base'}
        del em['boundaries']  # Endpoint schema has no own boundary field.
        publisher.validate_manifest_chain(im, pm, qm, common, em, b)
        for index, key, value, error in (
                (1, 'base_sha256', 'other', 'base_identity'),
                (0, 'endpoint_manifest_sha256', 'other', 'base_identity'),
                (4, 'status', 'building', 'window_identity'),
                (2, 'boundaries', {**b, 'end': 4}, 'window_identity'),
                (4, 'pairs', {'0': {}}, 'all68')):
            inputs = copy.deepcopy([im, pm, qm, common, em])
            inputs[index][key] = value
            with self.assertRaisesRegex(ValueError, error):
                publisher.validate_manifest_chain(*inputs, b)

    def test_default_cli_plan_does_not_read_acceptance_or_publish(self):
        with patch.object(publisher, 'preflight', side_effect=AssertionError('must not read results')):
            with patch('sys.argv', ['publisher.py']), contextlib.redirect_stdout(io.StringIO()) as out:
                publisher.main()
        value = json.loads(out.getvalue())
        self.assertEqual(value['status'], 'plan_only_no_run_results_read')
        self.assertFalse(value['writes_performed'])

    def test_ast_import_closure_is_exact_and_does_not_execute_modules(self):
        with tempfile.TemporaryDirectory() as d:
            project = Path(d)
            (project/'first.py').write_text('from helpers import second\nraise RuntimeError("never execute")\n')
            (project/'helpers').mkdir()
            (project/'helpers/second.py').write_text('import third\n')
            (project/'third.py').write_text('VALUE=1\n')
            closure = publisher.local_import_closure(project, ['first.py'])
            self.assertEqual(closure, {'first.py', 'helpers/second.py', 'third.py'})
            with self.assertRaises(ValueError):
                publisher.relative('../outside')

    def test_mock_accepted_publication_copies_exact_bytes_and_verifies_zip(self):
        with tempfile.TemporaryDirectory() as d:
            project, vault, accepted, original, plan = sample_plan(d)
            with patch.object(publisher, 'preflight', return_value=plan):
                receipt = publisher.publish(project, vault, accepted, publisher.digest(accepted))
            package = vault/publisher.PACKAGE
            self.assertEqual((package/'before/README.md').read_bytes(), original)
            after = (vault/'README.md').read_bytes()
            offset = receipt['vault_readme']['insertion_offset']
            count = receipt['vault_readme']['inserted_bytes']
            self.assertEqual(after[:offset]+after[offset+count:], original)
            self.assertEqual(offset, len(b'\xef\xbb\xbf# Existing vault\r\n'))
            self.assertIn(publisher.PACKAGE.encode(), after[offset:offset+count])
            self.assertTrue(receipt['vault_readme']['original_byte_reconstruction_verified'])
            self.assertTrue(receipt['zip']['all_entry_hashes_and_sizes_verified'])
            self.assertFalse(receipt['cloud_sync_verified'])
            self.assertFalse(receipt['models_loaded_or_fitted'])
            manifest = json.loads((package/'MANIFEST.json').read_bytes())
            publisher.verify_payload(package, manifest)
            with zipfile.ZipFile(package/publisher.ZIP_NAME) as z:
                self.assertEqual(set(z.namelist()), {r['path'] for r in manifest['files']} | {'MANIFEST.json'})
                self.assertNotIn('PUBLICATION.json', z.namelist())
                self.assertFalse(any(p.endswith(('.parquet', '.sqlite', '.csv')) for p in z.namelist()))
            self.assertEqual((accepted.parent/'VAULT_PUBLICATION.json').read_bytes(), (package/'PUBLICATION.json').read_bytes())

    def test_existing_receipt_and_source_mutation_prevent_readme_append(self):
        with tempfile.TemporaryDirectory() as d:
            project, vault, accepted, original, plan = sample_plan(d)
            marker = accepted.parent/'VAULT_PUBLICATION.json'; marker.write_text('existing')
            with patch.object(publisher, 'preflight', return_value=plan):
                with self.assertRaisesRegex(ValueError, 'receipt_already_exists'):
                    publisher.publish(project, vault, accepted, publisher.digest(accepted))
            self.assertFalse((vault/publisher.PACKAGE).exists())
            self.assertEqual((vault/'README.md').read_bytes(), original)
            (project/'example.py').write_text('CHANGED=1\n')
            with patch.object(publisher, 'preflight', return_value=plan):
                with self.assertRaisesRegex(ValueError, 'SHA256 mismatch'):
                    publisher.publish(project, vault, accepted, publisher.digest(accepted), accepted.parent/'new_receipt.json')
            self.assertEqual((vault/'README.md').read_bytes(), original)
            self.assertFalse((accepted.parent/'new_receipt.json').exists())

    def test_readme_append_refuses_a_concurrent_edit_without_overwriting_it(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d)/'README.md'; original = b'Original\r\n'
            current = original+b'Other writer\r\n'; path.write_bytes(current)
            with self.assertRaisesRegex(ValueError, 'changed_before_append'):
                publisher.append_readme(path, original, b'Our note\n')
            self.assertEqual(path.read_bytes(), current)
            receipt = publisher.append_readme(path, current, b'Our note\n')
            self.assertEqual(path.read_bytes(), current+b'Our note\n')
            self.assertEqual(receipt['original_bytes_preserved'], len(current))

    def test_title_insertion_preserves_bom_mixed_endings_and_original_bytes(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d)/'README.md'
            for original, prefix in (
                    (b'\xef\xbb\xbf# Vault\r\n\r\nFirst\nSecond\r\nTail', b'\xef\xbb\xbf# Vault\r\n'),
                    (b'# Vault\nOld\r\nOther\n', b'# Vault\n'),
                    (b'Preface\r\n# Vault\n\r\nOriginal', b'Preface\r\n# Vault\n'),
                    (b'\xef\xbb\xbf# Vault', b'\xef\xbb\xbf# Vault')):
                with self.subTest(original=original):
                    path.write_bytes(original)
                    addition = b'\n## New checkpoint\n\nPointer\n'
                    record = publisher.insert_readme_after_title(path, original, addition)
                    after = path.read_bytes(); offset = record['insertion_offset']
                    self.assertEqual(after, prefix+addition+original[len(prefix):])
                    self.assertEqual(after[:offset]+after[offset+len(addition):], original)
                    self.assertTrue(record['original_byte_reconstruction_verified'])
                    self.assertEqual(record['before_sha256'], hashlib.sha256(original).hexdigest())
                    self.assertEqual(record['after_sha256'], hashlib.sha256(after).hexdigest())

    def test_title_insertion_refuses_concurrent_edit_and_requires_title(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d)/'README.md'; original = b'# Vault\r\nOriginal\n'
            current = original+b'Other writer\r\n'; path.write_bytes(current)
            with self.assertRaisesRegex(ValueError, 'changed_before_insert'):
                publisher.insert_readme_after_title(path, original, b'\nOur pointer\n')
            self.assertEqual(path.read_bytes(), current)
            path.write_bytes(original)
            def concurrent_append(_fd):
                with path.open('ab') as other:
                    other.write(b'Concurrent suffix\n')
            with patch.object(publisher.os, 'fsync', side_effect=concurrent_append):
                with self.assertRaisesRegex(ValueError, 'concurrent_change_or_insert_mismatch'):
                    publisher.insert_readme_after_title(path, original, b'\nOur pointer\n')
            self.assertTrue(path.read_bytes().endswith(b'Concurrent suffix\n'))
            untitled = b'Original without title\n'; path.write_bytes(untitled)
            with self.assertRaisesRegex(ValueError, 'first_title_required'):
                publisher.insert_readme_after_title(path, untitled, b'\nOur pointer\n')
            self.assertEqual(path.read_bytes(), untitled)

    def test_payload_checker_rejects_corruption_and_unmanifested_files(self):
        with tempfile.TemporaryDirectory() as d:
            package = Path(d); source = package/'module.py'; source.write_bytes(b'X=1\n')
            m = {'files': [{'path': 'module.py', **publisher.checked(source)}]}
            publisher.verify_payload(package, m)
            source.write_bytes(b'X=2\n')
            with self.assertRaisesRegex(ValueError, 'SHA256 mismatch'):
                publisher.verify_payload(package, m)
            source.write_bytes(b'X=1\n'); (package/'unexpected.db').write_bytes(b'no')
            with self.assertRaisesRegex(ValueError, 'unmanifested'):
                publisher.verify_payload(package, m)

    def test_nonaccepted_receipt_fails_before_results_or_package_creation(self):
        with tempfile.TemporaryDirectory() as d:
            project, vault, accepted, _, _ = sample_plan(d)
            accepted.write_text('{"status":"pending","can_place_orders":false,"models_promoted":0}')
            with self.assertRaisesRegex(ValueError, 'final_unpromoted'):
                publisher.preflight(project, vault, accepted, publisher.digest(accepted))
            self.assertFalse((vault/publisher.PACKAGE).exists())


if __name__ == '__main__':
    unittest.main()
