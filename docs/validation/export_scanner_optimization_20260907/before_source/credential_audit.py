#!/usr/bin/env python3
"""Credential/privacy audit for a Git candidate, index, or committed tree.

Findings never echo the suspected value; only a one-way fingerprint is
reported.  Synthetic credential-handling fixtures are explicitly classified
instead of being mistaken for production credentials.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import subprocess
import tokenize
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Iterable


FIXTURE_PATHS = {
    "test_oanda_latest_moves.py",
    "test_oanda_macro_consensus_access_audit.py",
    "test_oanda_practice_top_signal_executor.py",
    "test_oanda_wait_for_private_creds.py",
}
BANNED_NAMES = {
    "creds", ".env", "credentials.json", "secrets.json", "auth.json",
    "token.json", "accounts_registry.json", ".netrc", ".npmrc", ".pypirc",
}
BANNED_DIRECTORY_NAMES = {"creds", "credentials", "secrets"}
BANNED_STEMS = {"creds", "credentials", "secrets", "auth", "token", "accounts_registry"}
BANNED_CONFIG_SUFFIXES = {".env", ".ini", ".json", ".toml", ".yaml", ".yml"}
PATTERNS = {
    "private_key_header": re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "known_token_prefix": re.compile(rb"\b(?:sk-[A-Za-z0-9_-]{20,}|gh[pousr]_[A-Za-z0-9]{20,}|AKIA[0-9A-Z]{16})\b"),
    "credential_assignment": re.compile(
        rb"(?i)[\"']?(?:[a-z0-9_-]*(?:api[_-]?key|api[_-]?token|access[_-]?token)"
        rb"|oanda[_-]?(?:token|access[_-]?token)|secret|password)[\"']?"
        rb"\s*[=:]\s*(?:[rubf]{0,2}(?:\"{3}|'{3}|[\"'`])\s*)?"
        rb"([A-Za-z0-9._~+/=-]{16,})(?:\"{3}|'{3}|[\"'`])?"
    ),
    "bearer_token": re.compile(rb"(?i)\bBearer\s+([A-Za-z0-9._~+/=-]{16,})"),
    "credential_query": re.compile(
        rb"(?i)[?&](?:api[_-]?key|api[_-]?token|access[_-]?token|token|secret|password)="
        rb"([A-Za-z0-9._~+/=-]{16,})"
    ),
    "credential_uri": re.compile(rb"(?i)\b(?:https?|postgres(?:ql)?|mysql)://[^\s:/]+:[^\s/@]+@"),
}
ACCOUNT_ID = re.compile(rb"\b\d{3}-\d{3}-\d{8}-\d{3}\b")
SYNTHETIC_MARKERS = (
    b"practice-token", b"sensitive-value", b"not-heartbeat-telemetry",
    b"must-not-be-persisted", b"long-enough", b"example.invalid",
)


def path_is_banned(path: str) -> bool:
    parts = Path(path).parts
    lowered = [part.lower() for part in parts]
    if any(part in BANNED_DIRECTORY_NAMES for part in lowered[:-1]):
        return True
    name = lowered[-1]
    if name in BANNED_NAMES or name == ".env" or name.startswith(".env."):
        return True
    candidate = Path(name)
    if candidate.stem in BANNED_STEMS and candidate.suffix in BANNED_CONFIG_SUFFIXES:
        return True
    if name.startswith(("creds.", "credentials.", "secrets.")):
        return True
    return False


def synthetic_fixture_value(path: str, material: bytes) -> bool:
    material_lower = material.lower()
    return path in FIXTURE_PATHS and any(
        marker in material_lower for marker in SYNTHETIC_MARKERS
    )


def code_reference_value(material: bytes) -> bool:
    """Recognize expression syntax only; this does not classify a literal secret."""
    lowered = material.lower()
    if re.fullmatch(rb"[a-z_][a-z_]*", lowered):
        return True
    return bool(re.fullmatch(rb"[a-z_][a-z0-9_.]*", lowered)) and lowered.startswith(
        (b"args.", b"config.", b"env.", b"os.", b"self.", b"settings.")
    )


@lru_cache(maxsize=1)
def _python_reference_spans(payload: bytes) -> frozenset[tuple[int, int]]:
    """Prove candidate value spans are NAME/attribute tokens, not literal text.

    Cache only the last source member so repeated matches do not repeatedly
    tokenize large modules or retain a repository's contents in memory.
    """
    candidates = {
        match.span(1) for match in PATTERNS["credential_assignment"].finditer(payload)
        if code_reference_value(match.group(1))
    }
    if not candidates:
        return frozenset()
    try:
        encoding, _ = tokenize.detect_encoding(io.BytesIO(payload).readline)
        text = payload.decode(encoding)
        byte_encoding = "utf-8" if encoding.lower() == "utf-8-sig" else encoding
        lines = text.split("\n")
        offsets = []
        offset = 3 if payload.startswith(b"\xef\xbb\xbf") else 0
        for line in lines:
            offsets.append(offset)
            offset += len((line + "\n").encode(byte_encoding))

        def byte_offset(position: tuple[int, int]) -> int:
            row, column = position
            return offsets[row - 1] + len(lines[row - 1][:column].encode(byte_encoding))

        accepted = set()
        chain_start = chain_end = None
        after_dot = False
        for token in tokenize.generate_tokens(io.StringIO(text).readline):
            if token.type == tokenize.ERRORTOKEN:
                return frozenset()
            if token.type == tokenize.NAME:
                start, end = byte_offset(token.start), byte_offset(token.end)
                if not after_dot or start != chain_end:
                    chain_start = start
                chain_end = end
                after_dot = False
                if (chain_start, end) in candidates:
                    accepted.add((chain_start, end))
            elif token.type == tokenize.OP and token.string == "." and chain_end is not None:
                start, end = byte_offset(token.start), byte_offset(token.end)
                if not after_dot and start == chain_end:
                    chain_end = end
                    after_dot = True
                else:
                    chain_start = chain_end = None
                    after_dot = False
            else:
                chain_start = chain_end = None
                after_dot = False
        return frozenset(accepted)
    except (SyntaxError, UnicodeError, LookupError, tokenize.TokenError, ValueError, IndexError):
        # Malformed/unknown source is never evidence for a reference exemption.
        return frozenset()


def credential_assignment_is_reference(path: str, match: re.Match[bytes]) -> bool:
    """Exempt only token-proven Python references; other languages fail closed."""
    if Path(path).suffix.lower() not in {".py", ".pyi"}:
        return False
    return match.span(1) in _python_reference_spans(match.string)


def run_git(root: Path, *args: str, binary: bool = False):
    result = subprocess.run(
        ["git", "-C", str(root), *args], check=True, capture_output=True
    )
    return result.stdout if binary else result.stdout.decode("utf-8").strip()


def paths_for_mode(root: Path, mode: str, revision: str | None = None) -> list[str]:
    if mode == "candidate":
        raw = run_git(root, "ls-files", "--cached", "--others", "--exclude-standard", "-z", binary=True)
    elif mode == "staged":
        # Audit the complete proposed index, not merely paths changed from HEAD.
        raw = run_git(root, "ls-files", "--cached", "-z", binary=True)
    else:
        raw = run_git(
            root, "ls-tree", "-r", "--name-only", "-z", revision or "HEAD", binary=True
        )
    return sorted(part.decode("utf-8") for part in raw.split(b"\0") if part)


def bytes_for_path(
    root: Path, path: str, mode: str, revision: str | None = None
) -> bytes:
    if mode == "candidate":
        return (root / path).read_bytes()
    object_name = f":{path}" if mode == "staged" else f"{revision or 'HEAD'}:{path}"
    return run_git(root, "show", object_name, binary=True)


def batch_bytes_for_paths(
    root: Path, paths: list[str], mode: str, revision: str | None = None
) -> dict[str, bytes | None]:
    if mode == "candidate":
        return {}
    specs = [f":{path}" if mode == "staged" else f"{revision or 'HEAD'}:{path}" for path in paths]
    result = subprocess.run(
        ["git", "-C", str(root), "cat-file", "--batch"],
        input=("\n".join(specs) + "\n").encode("utf-8"),
        check=True,
        capture_output=True,
    )
    stream = io.BytesIO(result.stdout)
    payloads: dict[str, bytes | None] = {}
    for path in paths:
        header = stream.readline().rstrip(b"\n")
        if header.endswith(b" missing"):
            payloads[path] = None
            continue
        fields = header.rsplit(b" ", 2)
        if len(fields) != 3 or fields[1] != b"blob":
            payloads[path] = None
            continue
        size = int(fields[2])
        payloads[path] = stream.read(size)
        if stream.read(1) != b"\n":
            raise RuntimeError("malformed git cat-file batch response")
    return payloads


def audit(root: Path, mode: str, revision: str | None = None) -> dict:
    if revision is not None and mode != "commit":
        raise ValueError("revision is supported only for commit-mode audits")
    audited_revision = None
    if mode == "commit":
        audited_revision = run_git(
            root, "rev-parse", "--verify", f"{revision or 'HEAD'}^{{commit}}"
        )
    findings = []
    account_files = account_occurrences = 0
    paths = paths_for_mode(root, mode, audited_revision)
    indexed_payloads = batch_bytes_for_paths(root, paths, mode, audited_revision)
    for path in paths:
        if path_is_banned(path):
            findings.append({"path": path, "rule": "banned_private_path"})
            continue
        try:
            if mode == "candidate":
                payload = bytes_for_path(root, path, mode, audited_revision)
            else:
                payload = indexed_payloads.get(path)
                if payload is None:
                    raise OSError("Git object is unavailable")
        except (OSError, subprocess.CalledProcessError):
            findings.append({"path": path, "rule": "unreadable_candidate"})
            continue
        accounts = ACCOUNT_ID.findall(payload)
        if accounts:
            account_files += 1
            account_occurrences += len(accounts)
        for rule, pattern in PATTERNS.items():
            for match in pattern.finditer(payload):
                material = match.group(1) if match.lastindex else match.group(0)
                if rule == "credential_assignment" and synthetic_fixture_value(path, material):
                    continue
                if rule == "credential_assignment" and credential_assignment_is_reference(path, match):
                    continue
                line = payload.count(b"\n", 0, match.start()) + 1
                findings.append(
                    {
                        "path": path,
                        "line": line,
                        "rule": rule,
                        "value_sha256_prefix": hashlib.sha256(material).hexdigest()[:16],
                    }
                )
    policy = {
        "reference_contract": "token_proven_python_name_or_attribute_only_v3_20260905",
        "fixture_paths": sorted(FIXTURE_PATHS),
        "rules": sorted(PATTERNS),
        "banned_names": sorted(BANNED_NAMES),
        "banned_directory_names": sorted(BANNED_DIRECTORY_NAMES),
        "banned_stems": sorted(BANNED_STEMS),
    }
    return {
        "schema_version": "git_credential_audit_v1",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "audited_revision": audited_revision,
        "files_scanned": len(paths),
        "finding_count": len(findings),
        "passed": not findings,
        "findings": findings,
        "privacy": {
            "account_id_files": account_files,
            "account_id_occurrences": account_occurrences,
            "classification": "private_source_metadata_not_bearer_credentials",
        },
        "policy_sha256": hashlib.sha256(
            json.dumps(policy, sort_keys=True).encode("utf-8")
        ).hexdigest(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--mode", choices=("candidate", "staged", "commit"), default="candidate")
    parser.add_argument(
        "--revision",
        help="Exact commit/ref to audit; valid only with --mode commit",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.revision and args.mode != "commit":
        parser.error("--revision requires --mode commit")
    result = audit(args.root.resolve(), args.mode, args.revision)
    text = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    print(text, end="")
    return 0 if result["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
