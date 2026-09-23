"""Offline safety/closure tests; synthetic directories only, no services or vault."""
from contextlib import ExitStack
import json
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from tools import publish_operational_checkpoint_v1 as pub


class PublisherTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='operational_checkpoint_test_')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / 'source'
        self.root.mkdir()
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for name, value in [('ROOT', self.root), ('EXTRA_FILES', set()), ('TEST_PATTERNS', ())]:
            self.stack.enter_context(patch.object(pub, name, value))
        self.write('worker.py', 'VALUE = 1\n')
        self.profile({'services': [{'script': 'worker.py', 'source_sha256': pub.sha(b'VALUE = 1\n')}]})

    def write(self, name, raw):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw.encode() if isinstance(raw, str) else raw)
        return path

    def profile(self, value):
        return self.write('config/profile.json', json.dumps(value))

    def build(self):
        return pub.build('config/profile.json')

    def test_service_and_sibling_config_pins_verified(self):
        child = self.write('config/child.json', '{}')
        self.profile({'script': 'worker.py', 'source_sha256': pub.sha(b'VALUE = 1\n'),
                      'child_path': str(child), 'child_sha256': pub.sha(b'{}')})
        result = self.build()
        self.assertEqual(result['config_pins_verified'], 2)
        self.assertEqual([x['binding'] for x in result['files'] if x['path'] == 'worker.py'], ['config_pinned'])
        child.write_text('{"changed":true}')
        with self.assertRaisesRegex(ValueError, 'pin'):
            self.build()

    def test_missing_pinned_source_and_conflict_rejected(self):
        self.profile({'script': 'missing.py', 'source_sha256': '0' * 64})
        with self.assertRaisesRegex(ValueError, 'missing'):
            self.build()
        self.profile({'script': 'worker.py', 'source_sha256': '0' * 64,
                      'source_bindings': {'worker.py': '1' * 64}})
        with self.assertRaisesRegex(ValueError, 'conflicting'):
            self.build()

    def test_pin_cannot_change_after_initial_check(self):
        original = pub.read_stable
        reads = 0
        def changing(name):
            nonlocal reads
            raw = original(name)
            if name == 'worker.py':
                reads += 1
                if reads == 1:
                    self.write('worker.py', 'VALUE = 2\n')
            return raw
        with patch.object(pub, 'read_stable', side_effect=changing):
            with self.assertRaisesRegex(ValueError, 'changed'):
                self.build()

    def test_parent_package_initializers_and_relative_imports_are_closed(self):
        self.write('worker.py', 'import pkg.nested.child\n')
        self.profile({'script': 'worker.py'})
        self.write('pkg/__init__.py', 'from . import helper\n')
        self.write('pkg/helper.py', 'HELPER = 1\n')
        self.write('pkg/nested/__init__.py', '')
        self.write('pkg/nested/child.py', 'CHILD = 2\n')
        names = {x['path'] for x in self.build()['files']}
        self.assertTrue({'pkg/__init__.py', 'pkg/helper.py', 'pkg/nested/__init__.py', 'pkg/nested/child.py'} <= names)

    def test_redirects_raw_cache_database_and_private_paths_excluded(self):
        folder = pub.EVIDENCE_ROOTS[0]
        for name in ('worker.stdout.log', 'worker.stderr.log', 'worker.out.log',
                     'worker.err.log', 'archive/raw.json', 'validation_cache/items.json',
                     'raw/items.txt', 'state.sqlite', 'secret.private.json'):
            self.write(folder + '/' + name, '{}')
        self.write(folder + '/TEST_RUN.log', '12 passed\n')
        self.write(folder + '/receipt.json', '{}')
        names = {x['path'] for x in self.build()['files']}
        self.assertEqual({x for x in names if x.startswith(folder)},
                         {folder + '/TEST_RUN.log', folder + '/receipt.json'})

    def test_runtime_references_external_but_runtime_pins_refused(self):
        runtime = self.write('data/status.json', '{}')
        self.profile({'heartbeat': str(runtime)})
        self.assertNotIn('data/status.json', {x['path'] for x in self.build()['files']})
        self.profile({'source_bindings': {'data/status.json': pub.sha(b'{}')}})
        with self.assertRaisesRegex(ValueError, 'runtime'):
            self.build()
        self.profile({'source_bindings': {'../escape.py': '0' * 64}})
        with self.assertRaises(RuntimeError):
            self.build()

    def test_known_private_account_identifier_is_screened_without_disclosure(self):
        account = b'-'.join((b'123', b'456', b'12345678', b'999'))
        self.write('creds', account)
        self.profile({'note': account.decode()})
        with self.assertRaisesRegex(RuntimeError, 'known private value') as caught:
            self.build()
        self.assertNotIn(account.decode(), str(caught.exception))

    def test_only_explicit_private_dependency_is_omitted_and_hash_bound(self):
        account = b'-'.join((b'123', b'456', b'12345678', b'999'))
        self.write('creds', account)
        raw = b'PRIVATE_IDENTIFIER = "' + account + b'"\n'
        self.write('oanda_arima_canary_executor.py', raw)
        self.write('worker.py', 'import oanda_arima_canary_executor\n')
        self.profile({'script': 'worker.py'})
        value = self.build()
        self.assertNotIn('oanda_arima_canary_executor.py', {x['path'] for x in value['files']})
        self.assertEqual(len(value['private_external_sources']), 1)
        self.assertEqual(value['private_external_sources'][0]['sha256'], pub.sha(raw))
        self.assertEqual(value['private_external_sources'][0]['bytes'], len(raw))
        self.assertNotIn(account.decode(), pub.encode(value).decode())
        self.write('worker.py', 'import oanda_arima_canary_executor\nimport other_private\n')
        self.write('other_private.py', raw)
        with self.assertRaisesRegex(RuntimeError, 'other_private.py'):
            self.build()

    def test_evidence_link_entries_never_traversed(self):
        folder = pub.EVIDENCE_ROOTS[0]
        self.write(folder + '/receipt.json', '{}')
        class Entry:
            name = 'junction'
            path = str(self.root / folder / 'junction')
            def stat(self, *, follow_symlinks):
                self_test.assertFalse(follow_symlinks)
                return type('Info', (), {'st_mode': stat.S_IFLNK, 'st_reparse_tag': 0})()
        self_test = self
        class Scan:
            def __enter__(self): return iter([Entry()])
            def __exit__(self, *args): pass
        with patch.object(pub.os, 'scandir', return_value=Scan()) as scan:
            self.assertEqual(list(pub.evidence_files(folder)), [])
            self.assertEqual(scan.call_count, 1)

    def test_file_bound_and_case_aliases_fail_closed(self):
        with patch.object(pub, 'MAX_FILE_BYTES', 1):
            with self.assertRaisesRegex(ValueError, 'oversized'):
                self.build()
        self.profile({'sources': ['worker.py', 'WORKER.py']})
        # Windows resolves both spellings; explicit mixed names must not be archived.
        if (self.root / 'WORKER.py').exists():
            with self.assertRaisesRegex(ValueError, 'case-aliased'):
                self.build()

    def test_exact_acceptance_required_and_zip_readback_complete(self):
        inventory = self.build()
        raw = pub.encode(inventory)
        target = Path(self.temp.name) / 'checkpoint'
        with self.assertRaisesRegex(ValueError, 'accepted'):
            pub.publish(inventory, raw, '0' * 64, target)
        self.assertFalse(target.exists())
        result = pub.publish(inventory, raw, pub.sha(raw), target)
        self.assertTrue(result['local_readback_passed'])
        with zipfile.ZipFile(target / 'operational_source_20260916.zip') as z:
            self.assertEqual(set(z.namelist()), {'MANIFEST.json', 'README.md'} |
                             {'source/' + x['path'] for x in inventory['files']})
        with self.assertRaises(FileExistsError):
            pub.publish(inventory, raw, pub.sha(raw), target)

    def test_changed_copy_and_project_destination_refused(self):
        inventory = self.build()
        raw = pub.encode(inventory)
        with self.assertRaisesRegex(ValueError, 'outside'):
            pub.publish(inventory, raw, pub.sha(raw), self.root / 'checkpoint')
        self.write('worker.py', 'VALUE = 3\n')
        target = Path(self.temp.name) / 'changed'
        with self.assertRaisesRegex(ValueError, 'changed during copy'):
            pub.publish(inventory, raw, pub.sha(raw), target)
        self.assertFalse((target / 'PUBLISH_RECEIPT.json').exists())


if __name__ == '__main__':
    unittest.main()
