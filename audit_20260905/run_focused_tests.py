"""Run existing focused tests with network, child processes and runtime DBs blocked."""
from __future__ import annotations
import os
from pathlib import Path
import sys

ROOT = Path(r"C:\Users\zmoor\Documents\forex\trad")
OUT = Path(__file__).resolve().parent
sys.dont_write_bytecode = True
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
os.environ["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
os.environ["TEMP"] = os.environ["TMP"] = str(OUT)
sys.path[:0] = [str(ROOT), str(ROOT.parent)]

def guard(event, args):
    if event in {"socket.connect", "socket.connect_ex", "socket.bind", "subprocess.Popen", "os.system", "os.startfile"}:
        raise RuntimeError(f"Audit blocked external action: {event}")
    if event == "sqlite3.connect":
        database = str(args[0])
        if database == ":memory:":
            return
        if database.startswith("file:"):
            database = database[5:].split("?", 1)[0]
        if not Path(database).resolve().is_relative_to(OUT):
            raise RuntimeError("Audit blocked SQLite access outside disposable fixture directory")
    if event == "open" and isinstance(args[0], (str, bytes, os.PathLike)):
        path, mode, flags = args
        writing = (isinstance(mode, str) and any(c in mode for c in "wax+")) or (
            isinstance(flags, int) and flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND)
        )
        if writing and not Path(os.fsdecode(path)).resolve().is_relative_to(OUT):
            raise RuntimeError("Audit blocked file write outside evidence directory")

sys.addaudithook(guard)
import pytest
raise SystemExit(pytest.main([
    "-q", "-p", "no:cacheprovider", "--log-file", str(OUT / "pytest_root.log"),
    "--basetemp", str(OUT / "pytest_root_fixtures"),
    str(ROOT / "test_oanda_source_governance_news_fast_lane.py"),
    str(ROOT / "test_oanda_issue_register_validator.py"),
]))
