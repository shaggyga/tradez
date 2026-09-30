#!/usr/bin/env python3
"""Read-only offline startup checks. A pass does not authorize research or trading."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import importlib.metadata
import importlib.util
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import zipfile

# Load only our stdlib byte-verification helper, including under python -I.
_SPEC = importlib.util.spec_from_file_location("forex_workspace", Path(__file__).with_name("forex_workspace.py"))
workspace = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(workspace)
SHA = re.compile(r"[0-9a-f]{64}")
COMMIT = re.compile(r"[0-9a-f]{40}")
TERMINAL = {"DONE", "ABANDONED"}
READY = {"complete", "accepted_within_scope"}


def git(root, *arguments, allowed=(0,)):
    env = {**os.environ, "GIT_OPTIONAL_LOCKS": "0", "GIT_TERMINAL_PROMPT": "0"}
    result = subprocess.run(["git", "-C", str(root), *arguments], env=env,
                            capture_output=True, timeout=30)
    workspace.require(result.returncode in allowed, "Local Git query failed: " + arguments[0])
    return result.stdout.decode("utf-8", "replace").strip()


class Snapshot:
    """Bounded no-hydration reads with end-of-check race detection."""
    def __init__(self):
        self.reads = {}

    def read(self, path, limit=workspace.MAX_JSON_BYTES):
        path = Path(path)
        value = workspace.read_bytes(path, limit, allow_cloud=True)
        old = self.reads.get(path)
        digest = workspace.sha256(value)
        workspace.require(old is None or old == (digest, len(value)), "File changed during preflight: " + str(path))
        self.reads[path] = (digest, len(value))
        return value

    def json(self, path):
        return workspace.json_bytes(self.read(path))

    def check(self, root, record):
        path = workspace.under(root, record["path"], allow_cloud=True)
        data = self.read(path, workspace.MAX_BYTES)
        workspace.checked_record(data, record, record["path"])

    def unchanged(self):
        for path, (digest, size) in self.reads.items():
            data = workspace.read_bytes(path, max(size, 1), allow_cloud=True)
            workspace.require(len(data) == size and workspace.sha256(data) == digest,
                              "File changed during preflight: " + str(path))


def checked_package(vault, pointer, snapshot):
    name = pointer["package"]
    workspace.safe_name(name)
    workspace.require("/" not in name and pointer["manifest"] == name + "/MANIFEST.json",
                      "Pointer package/manifest mismatch")
    raw = snapshot.read(workspace.under(vault, pointer["manifest"], allow_cloud=True))
    workspace.require(workspace.sha256(raw) == pointer["manifest_sha256"], "Pointer manifest SHA256 mismatch")
    manifest = workspace.json_bytes(raw)
    records = workspace.inventory(manifest["files"])
    workspace.require(records and len(records) <= 8192 and
                      sum(r["bytes"] for r in records.values()) <= workspace.MAX_BYTES,
                      "Sealed package exceeds bounded inspection; review its declared scope")
    root = workspace.under(vault, name, allow_cloud=True)
    for record in records.values():
        snapshot.check(root, record)
    # Pointer navigation must refer to the same verified package member.
    for key in ("review", "path_forward", "run_status", "design", "design_coverage",
                "current_acceptance_addendum", "ops_status"):
        if key in pointer:
            target = pointer[key]
            workspace.require(target.startswith(name + "/") and target[len(name) + 1:] in records,
                              "Pointer target lacks manifest coverage: " + key)
    return manifest


def active_claims(board):
    """Use both board table layouts; terminal rows are history regardless of section."""
    result, columns, seen = [], None, set()
    for line in board.splitlines():
        if not line.startswith("|"):
            continue
        cells = [cell.strip().strip("`").strip() for cell in line.strip().strip("|").split("|")]
        if cells and cells[0] == "Task ID":
            columns = cells
            continue
        if not columns or not cells or set(cells[0]) <= {"-", ":", " "}:
            continue
        if len(cells) != len(columns):
            workspace.require(False, "Malformed coordination row; reconcile board before work")
        row = dict(zip(columns, cells))
        task = row["Task ID"]
        workspace.require(task not in seen, "Duplicate coordination task ID: " + task)
        seen.add(task)
        status = row.get("Status", row.get("Final status", ""))
        if status not in TERMINAL:
            result.append({"task_id": task, "status": status,
                           "owner": row.get("Chat / owner"),
                           "scope": row.get("Reserved scope / files", row.get("Files changed")),
                           "handoff": row.get("Handoff / next action", row.get("Remaining action"))})
    workspace.require(columns is not None, "Coordination table is unavailable")
    return result


def environment_versions(expected):
    workspace.require(isinstance(expected, dict) and expected and
                      all(isinstance(k, str) and isinstance(v, str) and v for k, v in expected.items()),
                      "Exact environment metadata is missing")
    installed = {}
    for package in expected:
        if package == "python":
            installed[package] = platform.python_version()
        else:
            try:
                installed[package] = importlib.metadata.version(package)
            except importlib.metadata.PackageNotFoundError:
                installed[package] = None
    return installed


def preflight(root, vault, *, expected_revision=None, claim_id=None, work_item=None,
              recipe=None, recipe_sha256=None, profile="engineering", artifacts=()):
    checks, snapshot, state = [], Snapshot(), {}

    def check(name, operation):
        try:
            detail = operation()
            checks.append({"check": name, "status": "pass", "detail": detail})
            return detail
        except (workspace.ReviewRequired, OSError, ValueError, KeyError, TypeError,
                AttributeError, zipfile.BadZipFile, subprocess.SubprocessError) as exc:
            checks.append({"check": name, "status": "blocked", "reason": str(exc)})
            return None

    def roots():
        state["root"] = workspace.root_path(root, allow_cloud=True)
        state["vault"] = workspace.root_path(vault, allow_cloud=True)
        return {"workspace": str(state["root"]), "vault": str(state["vault"])}

    def repository():
        repo = state["root"]
        workspace.require(Path(git(repo, "rev-parse", "--show-toplevel")).resolve() == repo.resolve(),
                          "Supply the repository root, not a nested or unrelated checkout")
        state["head"] = git(repo, "rev-parse", "HEAD")
        state["branch"] = git(repo, "branch", "--show-current")
        state["git_status"] = git(repo, "status", "--porcelain=v1", "--untracked-files=all")
        workspace.require(not state["git_status"], "Working tree contains tracked or untracked changes; preserve and checkpoint them first")
        return {"head": state["head"], "branch": state["branch"], "clean": True}

    def pointers():
        base = state["vault"]
        for key, name in (("design", "DESIGN_ALIGNMENT_LATEST.json"), ("review", "CHECKPOINT_REVIEW_LATEST.json")):
            pointer = snapshot.json(base / name)
            state[key] = pointer
            state[key + "_manifest"] = checked_package(base, pointer, snapshot)
        state["queue"] = snapshot.json(base / "REVIEW_QUEUE.json")
        state["board"] = snapshot.read(base / "CHAT_COORDINATION_BOARD.md").decode("utf-8-sig")
        snapshot.read(base / "VAULT_FIRST_REUSE.md")
        return {"design_package": state["design"]["package"], "review_package": state["review"]["package"],
                "design_manifest_sha256": state["design"]["manifest_sha256"],
                "review_manifest_sha256": state["review"]["manifest_sha256"],
                "exact_next_item": state["queue"].get("exact_next_item")}

    def documents():
        manifest = state["review_manifest"]
        for key, base in (("external_documents", state["vault"]), ("project_documents", state["root"])):
            records = workspace.inventory(manifest[key])
            workspace.require(records, "Current-document manifest is empty: " + key)
            if key == "external_documents":
                workspace.require("REVIEW_QUEUE.json" in records and "VAULT_FIRST_REUSE.md" in records,
                                  "Current manifest does not cover queue and reuse policy")
            for record in records.values():
                snapshot.check(base, record)
        return {"vault_documents": len(manifest["external_documents"]),
                "project_documents": len(manifest["project_documents"])}

    def source():
        records = [dict(r, path=r["path"][len("source_snapshot/"):])
                   for r in state["design_manifest"]["files"] if r["path"].startswith("source_snapshot/")]
        expected = workspace.inventory(records)
        workspace.require(expected, "Design package lacks a verified source snapshot")
        folder = state["root"] / "stage_c_alignment_integrity_v2"
        # Current engineering snapshot deliberately covers root files; nested runs/evidence stay local.
        workspace.require(all("/" not in name for name in expected), "Nested source snapshot needs explicit coverage mapping")
        actual = {p.name for p in folder.iterdir() if p.is_file()}
        workspace.require(actual == set(expected), "Engineering source inventory differs from current design snapshot")
        for record in expected.values():
            snapshot.check(folder, record)
        return {"engineering_files_verified": len(expected), "scope": "current engineering root source snapshot"}

    def revision():
        receipt = snapshot.json(state["vault"] / "SHARED_GIT_REMOTE_LATEST.json")
        state["receipt"] = receipt
        supplied = expected_revision
        wanted = supplied or receipt.get("handoff_document_commit")
        workspace.require(isinstance(wanted, str) and COMMIT.fullmatch(wanted), "Expected revision must be an exact full commit SHA")
        workspace.require(state["head"] == wanted, "Checkout revision differs from " + ("explicit expected commit" if supplied else "Vault final handoff commit"))
        required_branch = receipt.get("branch")
        workspace.require(bool(required_branch) and state["branch"] == required_branch,
                          "Checkout branch differs from Vault shared branch; reconcile before work")
        return {"expected_commit": wanted, "identity_source": "explicit" if supplied else "Vault final handoff receipt",
                "branch": state["branch"], "live_remote_freshness": "not_checked_offline"}

    def ownership():
        claims = active_claims(state["board"])
        state["claims"] = claims
        others = [c for c in claims if c["task_id"] != claim_id]
        # A sealed, work-specific manual scope review can establish non-overlap.
        # Keep the other owner's claim intact; never infer release from age/status.
        reconciled = []
        for review in state["review_manifest"].get("coordination_reconciliations", []):
            workspace.require(review.get("work_item") == (work_item or state["queue"].get("exact_next_item"))
                              and "claim_id" in review and review["claim_id"] == claim_id,
                              "Coordination reconciliation belongs to a different work item/owner")
            workspace.require(review.get("decision") == "nonoverlapping" and
                              isinstance(review.get("reason"), str) and review["reason"].strip(),
                              "Coordination reconciliation lacks a substantive scope decision")
            other = review.get("other_claim")
            workspace.require(other in others, "Reconciled claim changed or is no longer uniquely active")
            others.remove(other)
            reconciled.append(review)
        workspace.require(not others, "Unresolved coordination owners require scope reconciliation: " + ", ".join(c["task_id"] for c in others))
        if claim_id:
            own = [c for c in claims if c["task_id"] == claim_id]
            workspace.require(len(own) == 1 and own[0]["status"] in {"CLAIMED", "IN_PROGRESS"},
                              "Supplied claim is absent, terminal or not active")
        return {"active_claims": claims, "claim_id": claim_id, "claim_created": False,
                "sealed_scope_reconciliations": reconciled,
                "atomic_cross_machine_reservation": False}

    def gate():
        declaration = state["queue"].get("pre_research_gate")
        workspace.require(isinstance(declaration, dict) and declaration.get("required") is True and
                          declaration.get("pointer") == "OPERATIONAL_READINESS_LATEST.json",
                          "Queue does not declare the operational prerequisite")
        step_id = declaration.get("active_operational_step")
        workspace.require(isinstance(step_id, str) and step_id,
                          "Operational prerequisite lacks its exact active step identity")
        entries = state["queue"].get("operational_steps")
        workspace.require(isinstance(entries, list), "Operational queue entries are unavailable")
        required = [entry for entry in entries if entry.get("step_id") == step_id]
        workspace.require(len(required) == 1, "Required operational queue entry is missing or ambiguous")
        entry = required[0]
        workspace.require(entry.get("implementation_status") == "complete" and
                          entry.get("review_status") == "accepted_within_scope",
                          "Required operational queue entry is unfinished or awaiting accepted review")
        pointer = snapshot.json(state["vault"] / "OPERATIONAL_READINESS_LATEST.json")
        manifest = checked_package(state["vault"], pointer, snapshot)
        target = pointer.get("ops_status", pointer.get("run_status"))
        workspace.require(target is not None, "Operational gate lacks manifest-covered OPS_STATUS.json")
        status = snapshot.json(workspace.under(state["vault"], target, allow_cloud=True))
        workspace.require(pointer.get("step_id") == step_id and status.get("step_id") == step_id and
                          status.get("package") == pointer["package"],
                          "Operational gate identity differs from required queue step/package")
        packet = entry.get("packet", "")
        prefix = pointer["package"] + "/"
        records = workspace.inventory(manifest["files"])
        member = packet[len(prefix):] if packet.startswith(prefix) else None
        workspace.require(member in records and entry.get("packet_sha256") == records[member]["sha256"],
                          "Operational queue packet is not bound to this exact verified gate package")
        workspace.require(pointer.get("status") in READY and status.get("status") in READY,
                          "Operational checkpoint remains incomplete, blocked or awaiting review")
        workspace.require(pointer.get("research_authorization") is False and status.get("research_authorization") is False,
                          "Operational gate must keep research authorization separate")
        selected = work_item or state["queue"].get("exact_next_item")
        workspace.require(bool(selected), "No selected work item")
        # Selecting an arbitrary historical item must not bypass the current queue.
        workspace.require(selected == state["queue"].get("exact_next_item"), "Selected work differs from the verified next queue item")
        active = [step for step in state["queue"].get("steps", [])
                  if step.get("step_id") == state["queue"].get("active_step_id")]
        workspace.require(len(active) == 1, "Current engineering prerequisite is missing or ambiguous")
        workspace.require(active[0].get("review_status") == "accepted_within_scope",
                          "Current engineering prerequisite is awaiting review or correction")
        return {"status": pointer["status"], "selected_work_item": selected, "research_authorization": False,
                "step_id": step_id, "queue_packet": packet,
                "package": pointer["package"], "manifest_sha256": pointer["manifest_sha256"]}

    def environment():
        workspace.require(recipe is not None or recipe_sha256 is None,
                          "A recipe hash without a recipe path is ambiguous")
        if recipe:
            workspace.require(isinstance(recipe_sha256, str) and SHA.fullmatch(recipe_sha256),
                              "Selecting a recipe requires its independently recorded exact SHA256")
            raw = snapshot.read(Path(recipe))
            workspace.require(workspace.sha256(raw) == recipe_sha256, "Selected recipe SHA256 mismatch")
            selected = workspace.json_bytes(raw)
            expected = selected["environment"]
            sources = selected.get("sources")
            workspace.require(isinstance(sources, dict) and sources, "Selected recipe has no source closure")
            for name, digest in sources.items():
                workspace.require(isinstance(digest, str) and SHA.fullmatch(digest), "Invalid recipe source hash")
                data = snapshot.read(workspace.under(state["root"] / "stage_c_alignment_integrity_v2", name, allow_cloud=True))
                workspace.require(workspace.sha256(data) == digest, "Recipe source mismatch: " + name)
            selected_profile = "recipe:" + str(recipe)
        elif profile == "stdlib":
            workspace.require(sys.version_info >= (3, 10), "Python 3.10+ required for offline tooling")
            return {"profile": "stdlib", "python": platform.python_version(), "numerical_environment_checked": False}
        else:
            lock = snapshot.read(state["root"] / "requirements-engineering.lock.txt").decode("utf-8-sig")
            version = re.search(r"Observed Python (\d+\.\d+\.\d+)", lock)
            workspace.require(version is not None, "Engineering profile lacks exact Python metadata")
            expected = {"python": version[1]}
            for line in lock.splitlines():
                if not line.strip() or line.lstrip().startswith("#"):
                    continue
                match = re.fullmatch(r"([A-Za-z0-9_.-]+)==([^\s;]+)", line.strip())
                workspace.require(match is not None and match[1] not in expected, "Profile must contain unique exact package pins")
                expected[match[1]] = match[2]
            selected_profile = "engineering"
        installed = environment_versions(expected)
        differences = {name: {"expected": version, "installed": installed[name]}
                       for name, version in expected.items() if installed[name] != version}
        workspace.require(not differences, "Environment metadata mismatch: " + json.dumps(differences, sort_keys=True))
        return {"profile": selected_profile, "expected": expected, "installed": installed,
                "model_runtime_qualified": False}

    def archive(artifact_id):
        registry = snapshot.json(state["root"] / "artifacts" / "registry.json")
        workspace.require(artifact_id in registry["artifacts"], "Artifact is not registered: " + artifact_id)
        record = registry["artifacts"][artifact_id]
        data = snapshot.read(workspace.under(state["vault"], record["archive_vault_relative_path"], allow_cloud=True), workspace.MAX_BYTES)
        handle, members = workspace.inspect_archive(data, record)
        handle.close()
        return {"artifact": artifact_id, "verified_members": len(members), "archive_sha256": record["archive_sha256"],
                "original_run_identity_fingerprint": record["original_run_identity_fingerprint"],
                "models_loaded": 0, "models_fitted": 0}

    check("roots", roots)
    check("repository", repository)
    check("vault_packages", pointers)
    check("current_documents", documents)
    check("engineering_source", source)
    check("revision", revision)
    check("coordination", ownership)
    check("operational_gate", gate)
    check("environment", environment)
    for artifact_id in artifacts:
        check("artifact:" + artifact_id, lambda value=artifact_id: archive(value))

    def stable():
        snapshot.unchanged()
        workspace.require(git(state["root"], "rev-parse", "HEAD") == state["head"] and
                          git(state["root"], "branch", "--show-current") == state["branch"] and
                          git(state["root"], "status", "--porcelain=v1", "--untracked-files=all") == state["git_status"],
                          "Git changed during preflight; reconcile and retry")
        return {"files_read": len(snapshot.reads)}

    check("snapshot_stability", stable)
    return {"schema": "forex_offline_preflight.v1", "status": "blocked" if any(c["status"] == "blocked" for c in checks) else "pass",
            "utc": datetime.now(timezone.utc).isoformat(), "checks": checks,
            "code_commit": state.get("head"), "branch": state.get("branch"),
            "unresolved_claims": state.get("claims", []), "research_authorization": False,
            "model_runtime_qualified": False, "models_loaded": 0, "models_fitted": 0,
            "network_calls": 0, "filesystem_mutations": 0,
            "limitations": ["Local immutable/current-document identity checks only; no live GitHub or OneDrive freshness proof.",
                            "Passing is a setup observation, not a research, model run, broker or service authorization.",
                            "The board is advisory, not an atomic distributed lock; establish shared freshness and claim work before edits/runs.",
                            "Selected recipe input bytes and full historical artifact graphs are checked only by their original operator; this tool never launches it.",
                            "Historical missing approvals unrelated to the selected next work are not interpreted as startup blockers."]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--vault", type=Path, required=True)
    parser.add_argument("--expected-revision")
    parser.add_argument("--claim-id")
    parser.add_argument("--work-item")
    parser.add_argument("--recipe", type=Path)
    parser.add_argument("--recipe-sha256")
    parser.add_argument("--profile", choices=("engineering", "stdlib"), default="engineering")
    parser.add_argument("--artifact", action="append", default=[])
    args = parser.parse_args(argv)
    result = preflight(args.workspace, args.vault, expected_revision=args.expected_revision,
                       claim_id=args.claim_id, work_item=args.work_item, recipe=args.recipe,
                       recipe_sha256=args.recipe_sha256, profile=args.profile, artifacts=args.artifact)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] == "pass" else 2


if __name__ == "__main__":
    raise SystemExit(main())
