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
    import gspread
    from gspread.exceptions import APIError
except Exception:  # pragma: no cover
    gspread = None  # type: ignore

    class APIError(Exception):
        pass

try:
    from google.oauth2.service_account import Credentials
except Exception:  # pragma: no cover
    Credentials = None  # type: ignore

from ..config import *
from ..logging_utils import log_debug, log_warn
from ..runtime.rate_limit import TokenBucket
from ..utils import safe_int

@dataclass
class SheetContext:
    ws_id: int
    title: str
    ws: Any
    league_id: int
    kind: str = "main"
    next_row: int = LOG_START_ROW
    expected_cols: int = 0
    round_col_idx: Optional[int] = 6

def open_spreadsheet() -> Any:
    if gspread is None or Credentials is None:
        raise RuntimeError("Missing dependency: install gspread and google-auth.")
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID)

def _normalize_sheet_marker(raw: Any) -> str:
    s = str(raw or "").strip().lower()
    if s == MAIN_SHEET_MARKER.lower():
        return MAIN_SHEET_MARKER
    if s == CLAN_SHEET_MARKER.lower() or s == f"{CLAN_SHEET_MARKER.lower()}_shield":
        return CLAN_SHEET_MARKER
    return ""

def _single_cell_from_values(v: Any) -> str:
    if not isinstance(v, list) or not v:
        return ""
    first = v[0]
    if not isinstance(first, list) or not first:
        return ""
    return str(first[0]).strip()

def read_sheet_selectors(sh: Any, worksheets: Sequence[Any], read_limiter: TokenBucket) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
    if not worksheets:
        return out
    ranges: List[str] = []
    for ws in worksheets:
        ranges.extend([f"'{ws.title}'!A1", f"'{ws.title}'!{CONFIG_CELL}"])

    try:
        read_limiter.wait_for_token(1)
        if hasattr(sh, "values_batch_get"):
            resp = sh.values_batch_get(ranges)
            if isinstance(resp, dict):
                vrs = resp.get("valueRanges") or []
                for idx, ws in enumerate(worksheets):
                    a_idx = idx * 2
                    b_idx = a_idx + 1
                    marker_raw = _single_cell_from_values((vrs[a_idx] or {}).get("values") if a_idx < len(vrs) and isinstance(vrs[a_idx], dict) else None)
                    league_raw = _single_cell_from_values((vrs[b_idx] or {}).get("values") if b_idx < len(vrs) and isinstance(vrs[b_idx], dict) else None)
                    out[ws.id] = {
                        "marker": _normalize_sheet_marker(marker_raw),
                        "league_id": safe_int(league_raw),
                    }
        elif hasattr(sh, "batch_get"):
            arr = sh.batch_get(ranges)
            for idx, ws in enumerate(worksheets):
                a_idx = idx * 2
                b_idx = a_idx + 1
                marker_raw = _single_cell_from_values(arr[a_idx] if a_idx < len(arr) else None)
                league_raw = _single_cell_from_values(arr[b_idx] if b_idx < len(arr) else None)
                out[ws.id] = {
                    "marker": _normalize_sheet_marker(marker_raw),
                    "league_id": safe_int(league_raw),
                }
    except Exception as e:
        log_warn("sheet.batch_selector_read_failed", err=repr(e))

    for ws in worksheets:
        if ws.id in out:
            continue
        try:
            read_limiter.wait_for_token(1)
            marker_raw = (ws.acell("A1").value or "").strip()
            league_raw = (ws.acell(CONFIG_CELL).value or "").strip()
            out[ws.id] = {
                "marker": _normalize_sheet_marker(marker_raw),
                "league_id": safe_int(league_raw),
            }
        except Exception:
            out[ws.id] = {"marker": "", "league_id": None}
    if DEBUG_VERBOSE:
        configured = [v.get("league_id") for v in out.values() if v.get("league_id") is not None]
        main_count = sum(1 for v in out.values() if v.get("marker") == MAIN_SHEET_MARKER)
        clan_count = sum(1 for v in out.values() if v.get("marker") == CLAN_SHEET_MARKER)
        log_debug(
            "sheet.read_selectors",
            total=len(out),
            configured=len(configured),
            main_tabs=main_count,
            clan_tabs=clan_count,
            leagues=sorted(set(configured))[:200],
        )
    return out
