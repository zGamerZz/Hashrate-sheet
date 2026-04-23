from __future__ import annotations

import errno
import hashlib
import json
import logging
import os
import random
import re
import sqlite3
import threading
import time
import weakref
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import requests

from ..config import ABILITY_DIM_STATIC, ABILITY_HEADER_ORDER

def canonical_ability_header(ability_name: str) -> Optional[str]:
    """
    Map DB ability names to the fixed output header set.
    Only selected abilities are mapped; everything else is ignored.
    """
    n = re.sub(r"\s+", " ", (ability_name or "").strip().lower())
    if not n:
        return None

    if n in {"power up boost", "power-up boost"}:
        return "Power Up Boost"
    if n in {"clan power up boost", "clan powerup boost", "clan powerup"}:
        return "Clan Power Up Boost"

    m = re.match(r"^(rocket|boost)\s*\(x(1|10|100)\)$", n)
    if m:
        return f"Rocket (x{m.group(2)})"

    m = re.match(r"^instant boost\s*\(x(1|10|100)\)$", n)
    if m:
        return f"Instant Boost (x{m.group(1)})"

    m = re.match(r"^echo boost\s*\(x(1|10|100)\)$", n)
    if m:
        return f"Echo Boost (x{m.group(1)})"

    m = re.match(r"^focus boost\s*\(x(1|10|100)\)$", n)
    if m:
        return f"Focus Boost (x{m.group(1)})"

    # Optional tolerance for old style names like "Boost X10", "Echo Boost X1", etc.
    m = re.match(r"^(rocket|boost)\s*x(1|10|100)$", n)
    if m:
        return f"Rocket (x{m.group(2)})"
    m = re.match(r"^instant boost\s*x(1|10|100)$", n)
    if m:
        return f"Instant Boost (x{m.group(1)})"
    m = re.match(r"^echo boost\s*x(1|10|100)$", n)
    if m:
        return f"Echo Boost (x{m.group(1)})"
    m = re.match(r"^focus boost\s*x(1|10|100)$", n)
    if m:
        return f"Focus Boost (x{m.group(1)})"
    return None

def build_ability_id_to_header(catalog: Sequence[Tuple[str, str, int]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for ability_id, ability_name, _sort_order in catalog:
        hdr = canonical_ability_header(ability_name)
        if hdr is None:
            continue
        out[str(ability_id)] = hdr
    return out
