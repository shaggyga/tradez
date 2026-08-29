#!/usr/bin/env python3
"""Quote-safe launcher for the OANDA technical live manager."""

from __future__ import annotations

import runpy
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SITE_PACKAGES = ROOT / "..venv" / "Lib" / "site-packages"
TARGET = ROOT / "oanda_tech_prod_live_account_manager.py"

sys.path.insert(0, str(ROOT))
sys.path.append(str(SITE_PACKAGES))
sys.argv = [str(TARGET)] + sys.argv[1:]
runpy.run_path(str(TARGET), run_name="__main__")
