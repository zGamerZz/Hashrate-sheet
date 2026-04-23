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

try:
    from gspread.exceptions import APIError
except Exception:  # pragma: no cover
    class APIError(Exception):
        pass

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        s = str(v).strip()
        if s == "":
            return None
        return int(float(s))
    except Exception:
        return None

def safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        s = str(v).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None

def to_iso_utc(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc).replace(microsecond=0).isoformat()
    s = str(v).strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()
    except Exception:
        return s

def row_checksum(row: List[Any]) -> str:
    raw = json.dumps(row, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def parse_row_range(updated_range: str) -> Optional[Tuple[int, int]]:
    if not updated_range:
        return None
    m = re.search(r"![A-Z]+(\d+):[A-Z]+(\d+)$", updated_range)
    if not m:
        return None
    try:
        a = int(m.group(1))
        b = int(m.group(2))
        return (a, b) if a <= b else (b, a)
    except Exception:
        return None

def classify_sheet_error(err: Exception) -> Tuple[bool, Optional[float], Optional[int]]:
    retryable = False
    retry_after = None
    status = None
    if isinstance(err, APIError):
        resp = getattr(err, "response", None)
        status = getattr(resp, "status_code", None)
        if status in {429, 500, 502, 503, 504}:
            retryable = True
        try:
            ra = resp.headers.get("Retry-After") if resp is not None else None
            if ra:
                retry_after = float(ra)
        except Exception:
            retry_after = None
    msg = str(err)
    if any(x in msg for x in ["429", "500", "502", "503", "504", "timeout", "timed out"]):
        retryable = True
    return retryable, retry_after, status
