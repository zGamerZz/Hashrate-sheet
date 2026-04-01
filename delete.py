#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import os
import random
import time
from typing import Any, Dict, List, Optional

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

try:
    import dotenv
except Exception:  # pragma: no cover
    dotenv = None  # type: ignore

if dotenv is not None:
    dotenv.load_dotenv()


SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "service_acc.json")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1bq5Sy2pV35x33Q12G5_EJ2S0Kb1iXo1Y2FCxL7QIxGg")
CLEAR_RANGE = os.getenv("DELETE_RANGE", "H3:W840")


def with_backoff(fn, what: str, tries: int = 6) -> Any:
    last_err = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            last_err = e
            retry = 0.6 * (2**i) * (1 + random.uniform(-0.2, 0.2))
            if isinstance(e, APIError):
                resp = getattr(e, "response", None)
                code = getattr(resp, "status_code", None)
                if code in {429, 500, 502, 503, 504}:
                    print(f"[WARN] {what} HTTP {code}, retry in {retry:.2f}s")
                    time.sleep(retry)
                    continue
            print(f"[WARN] {what} failed, retry in {retry:.2f}s: {repr(e)}")
            time.sleep(retry)
    raise last_err  # type: ignore


def open_spreadsheet() -> Any:
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID)


def safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        s = str(v).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


def read_selector_cells(sh: Any, worksheets: List[Any]) -> Dict[int, Dict[str, str]]:
    """
    Read A1 and B5 for all worksheets in one batched request (fallback to per-sheet reads).
    """
    out: Dict[int, Dict[str, str]] = {}
    ranges: List[str] = []
    for ws in worksheets:
        ranges.append(f"'{ws.title}'!A1")
        ranges.append(f"'{ws.title}'!B5")

    # Batch read first.
    try:
        if hasattr(sh, "values_batch_get"):
            resp = sh.values_batch_get(ranges)
            value_ranges = (resp or {}).get("valueRanges") or []
            i = 0
            for ws in worksheets:
                a1 = ""
                b5 = ""
                if i < len(value_ranges):
                    vals = value_ranges[i].get("values") if isinstance(value_ranges[i], dict) else None
                    if vals and vals[0]:
                        a1 = str(vals[0][0]).strip()
                i += 1
                if i < len(value_ranges):
                    vals = value_ranges[i].get("values") if isinstance(value_ranges[i], dict) else None
                    if vals and vals[0]:
                        b5 = str(vals[0][0]).strip()
                i += 1
                out[ws.id] = {"A1": a1, "B5": b5}
            return out
    except Exception as e:
        print(f"[WARN] batch selector read failed, falling back to per-sheet reads: {repr(e)}")

    # Fallback per sheet.
    for ws in worksheets:
        a1 = ""
        b5 = ""
        try:
            a1 = (ws.acell("A1").value or "").strip()
        except Exception:
            pass
        try:
            b5 = (ws.acell("B5").value or "").strip()
        except Exception:
            pass
        out[ws.id] = {"A1": a1, "B5": b5}
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="One-time cleanup: clear only H3:S839 in sheets where A1 or B5 contains a league id."
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Perform deletion. Without this flag, script runs as dry-run only.",
    )
    args = parser.parse_args()

    sh = with_backoff(open_spreadsheet, "open_spreadsheet")
    worksheets = with_backoff(sh.worksheets, "worksheets")
    print(f"[OK] Spreadsheet: {sh.title} | tabs={len(worksheets)}")
    selector_values = with_backoff(lambda: read_selector_cells(sh, worksheets), "read_selector_cells")

    actions: List[tuple[Any, str, str]] = []

    for ws in worksheets:
        vals = selector_values.get(ws.id, {"A1": "", "B5": ""})
        a1_raw = vals.get("A1", "")
        b5_raw = vals.get("B5", "")
        league_a1 = safe_int(a1_raw)
        league_b5 = safe_int(b5_raw)
        if league_a1 is None and league_b5 is None:
            print(f"[SKIP] sheet='{ws.title}' A1='{a1_raw}' B5='{b5_raw}' (no league id)")
            continue
        source = "A1" if league_a1 is not None else "B5"
        league_id = league_a1 if league_a1 is not None else league_b5
        actions.append((ws, source, str(league_id)))
        print(f"[PLAN] sheet='{ws.title}' league={league_id} via {source} clear={CLEAR_RANGE}")

    if not args.execute:
        print(f"[DRY] No changes applied. matched_sheets={len(actions)}. Run with --execute to perform deletion.")
        return

    total = 0
    for ws, source, league_id in actions:
        with_backoff(lambda: ws.batch_clear([CLEAR_RANGE]), f"batch_clear({ws.title})")
        total += 1
        print(f"[DONE] cleared sheet='{ws.title}' league={league_id} via {source} range={CLEAR_RANGE}")

    print(f"[OK] Cleanup complete. sheets_cleared={total} range={CLEAR_RANGE}")


if __name__ == "__main__":
    main()
