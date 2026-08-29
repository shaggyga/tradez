import tempfile
import unittest
from pathlib import Path

from trad.forex_guarded_deploy import (
    DeploymentConflict,
    deploy_files,
    initialize_manifest,
    verify_manifest,
)


class GuardedDeployTests(unittest.TestCase):
    def test_deploys_when_manifest_matches_and_rejects_external_drift(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            target = root / "target"
            source.mkdir()
            target.mkdir()
            (source / "worker.py").write_text("version = 2\n", encoding="utf-8")
            (target / "worker.py").write_text("version = 1\n", encoding="utf-8")
            manifest = root / "manifest.json"
            initialize_manifest(
                target,
                manifest,
                ["worker.py"],
                owner="test",
            )
            result = deploy_files(
                source,
                target,
                manifest,
                ["worker.py"],
                owner="test-deploy",
            )
            self.assertEqual(result["changed_files"], ["worker.py"])
            self.assertTrue(verify_manifest(target, manifest)["ok"])

            (target / "worker.py").write_text("external change\n", encoding="utf-8")
            with self.assertRaises(DeploymentConflict):
                deploy_files(
                    source,
                    target,
                    manifest,
                    ["worker.py"],
                    owner="conflicting-chat",
                )


if __name__ == "__main__":
    unittest.main()
