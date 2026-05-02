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
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import requests

from ..config import *
from ..logging_utils import log_debug, log_info, log_warn
from ..runtime.rate_limit import TokenBucket
from ..storage.state import StateStore
from ..utils import row_checksum, safe_float, safe_int
from .context import SheetContext, read_sheet_selectors

def _trim_header(values: Sequence[Any]) -> List[str]:
    return [(v or "").strip() for v in values]

def _read_header_row(ws: Any, read_limiter: TokenBucket) -> List[str]:
    try:
        read_limiter.wait_for_token(1)
        row_vals = ws.row_values(3)
    except Exception:
        return []
    return _trim_header(row_vals)

def _header_matches_existing(existing: Sequence[str], expected_header: Sequence[str]) -> bool:
    expected = _trim_header(expected_header)
    if len(existing) < len(expected):
        return False
    for i, v in enumerate(expected):
        if existing[i] != v:
            return False
    return True

def _expected_without_league_th(expected_header: Sequence[str]) -> List[str]:
    out: List[str] = []
    removed = False
    for h in expected_header:
        if not removed and h == LEAGUE_TH_HEADER:
            removed = True
            continue
        out.append(h)
    return out

def _expected_with_legacy_clan_gmt(expected_header: Sequence[str]) -> List[str]:
    out = list(expected_header)
    if "calc_mode" in out and "clan_shield_gmt" not in out:
        out.insert(out.index("calc_mode"), "clan_shield_gmt")
    return out

def _looks_like_iso_ts(value: Any) -> bool:
    s = str(value or "").strip()
    if not s:
        return False
    return re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", s) is not None

def _history_shift_detected(ws: Any, read_limiter: TokenBucket) -> bool:
    try:
        read_limiter.wait_for_token(1)
        values = ws.get(f"H{LOG_START_ROW}:I{LOG_START_ROW + 10}")
    except Exception as e:
        log_warn("sheet.history_shift_check_failed", sheet=getattr(ws, "title", "?"), err=repr(e))
        return False
    for row in values or []:
        h = row[0] if len(row) > 0 else ""
        i = row[1] if len(row) > 1 else ""
        if _looks_like_iso_ts(h) and safe_float(i) is not None:
            return True
    return False

def _detect_next_row(ws: Any, read_limiter: TokenBucket, round_col_index: int = 7) -> int:
    """
    Determine next writable row from roundId column.
    This avoids relying only on local row_map state after manual sheet edits.
    """
    last_data_row = LOG_START_ROW - 1
    try:
        read_limiter.wait_for_token(1)
        values = ws.col_values(round_col_index)
        for idx, val in enumerate(values, start=1):
            if idx < LOG_START_ROW:
                continue
            if safe_int(val) is not None:
                last_data_row = idx
    except Exception as e:
        log_warn("sheet.detect_next_row_failed", sheet=getattr(ws, "title", "?"), err=repr(e))
    return max(LOG_START_ROW, last_data_row + 1)

def _read_round_row_index(ws: Any, read_limiter: TokenBucket, round_col_index: int = 7) -> Dict[int, int]:
    """
    Build round_id -> row_idx index directly from the sheet roundId column.
    Used to heal missing local row_map entries and detect actual holes.
    """
    out: Dict[int, int] = {}
    try:
        read_limiter.wait_for_token(1)
        values = ws.col_values(round_col_index)
    except Exception as e:
        log_warn("sheet.read_round_index_failed", sheet=getattr(ws, "title", "?"), err=repr(e))
        return out
    for idx, val in enumerate(values, start=1):
        if idx < LOG_START_ROW:
            continue
        rid = safe_int(val)
        if rid is None:
            continue
        out[rid] = idx
    return out

def _insert_league_th_column(ctx: SheetContext, write_limiter: TokenBucket) -> bool:
    try:
        write_limiter.wait_for_token(1)
        ctx.ws.spreadsheet.batch_update(
            {
                "requests": [
                    {
                        "insertDimension": {
                            "range": {
                                "sheetId": ctx.ws.id,
                                "dimension": "COLUMNS",
                                "startIndex": LEAGUE_TH_COL_INDEX,
                                "endIndex": LEAGUE_TH_COL_INDEX + 1,
                            },
                            "inheritFromBefore": True,
                        }
                    }
                ]
            }
        )
        log_info("sheet.history_shift_fixed", sheet=ctx.title, action="insert_col", col_index=LEAGUE_TH_COL_INDEX)
        return True
    except Exception as e:
        log_warn("sheet.history_shift_insert_failed", sheet=ctx.title, err=repr(e))
        return False

def _remove_clan_gmt_column(ctx: SheetContext, write_limiter: TokenBucket) -> bool:
    try:
        write_limiter.wait_for_token(1)
        ctx.ws.spreadsheet.batch_update(
            {
                "requests": [
                    {
                        "deleteDimension": {
                            "range": {
                                "sheetId": ctx.ws.id,
                                "dimension": "COLUMNS",
                                "startIndex": CLAN_LEGACY_GMT_COL_INDEX,
                                "endIndex": CLAN_LEGACY_GMT_COL_INDEX + 1,
                            }
                        }
                    }
                ]
            }
        )
        log_info("sheet.legacy_clan_gmt_removed", sheet=ctx.title, col_index=CLAN_LEGACY_GMT_COL_INDEX)
        return True
    except Exception as e:
        log_warn("sheet.legacy_clan_gmt_remove_failed", sheet=ctx.title, err=repr(e))
        return False

def sync_layout_if_needed(
    ctx: SheetContext,
    expected_header: List[str],
    layout_sig: str,
    state: StateStore,
    write_limiter: TokenBucket,
    read_limiter: TokenBucket,
    marker: str,
    enable_history_shift: bool,
) -> None:
    meta = state.get_sheet_state(ctx.ws_id)
    cached_ok = (meta or {}).get("layout_sig") == layout_sig
    existing_header = _read_header_row(ctx.ws, read_limiter)
    header_ok = _header_matches_existing(existing_header, expected_header)

    needs_history_shift = False
    shift_reason = None
    needs_clan_gmt_column_drop = False
    checked_history = enable_history_shift and (ctx.ws_id in MIGRATION_CHECKED_SHEETS)
    if enable_history_shift and LEAGUE_TH_HEADER in expected_header:
        old_expected = _expected_without_league_th(expected_header)
        if not header_ok and existing_header and _header_matches_existing(existing_header, old_expected):
            needs_history_shift = True
            shift_reason = "missing_league_th_header"
        elif header_ok and (not checked_history) and _history_shift_detected(ctx.ws, read_limiter):
            needs_history_shift = True
            shift_reason = "shifted_history_detected"
    if marker == CLAN_SHEET_MARKER and not header_ok and existing_header:
        legacy_expected = _expected_with_legacy_clan_gmt(expected_header)
        if _header_matches_existing(existing_header, legacy_expected):
            needs_clan_gmt_column_drop = True

    if cached_ok and header_ok and not needs_history_shift and not needs_clan_gmt_column_drop:
        return
    if DRY_RUN:
        reason_parts: List[str] = []
        if not cached_ok:
            reason_parts.append("cache_mismatch")
        if not header_ok:
            reason_parts.append("header_mismatch")
        if needs_history_shift:
            reason_parts.append("history_shift")
        if needs_clan_gmt_column_drop:
            reason_parts.append("legacy_clan_gmt_column")
        reason = "+".join(reason_parts) if reason_parts else "no_change"
        log_info("sheet.layout_dry_skip", sheet=ctx.title, reason=reason)
        state.upsert_sheet_meta(ctx.ws_id, ctx.title, ctx.league_id, layout_sig=layout_sig)
        return
    if needs_history_shift:
        if _insert_league_th_column(ctx, write_limiter):
            MIGRATION_CHECKED_SHEETS.add(ctx.ws_id)
        else:
            log_warn("sheet.history_shift_skipped", sheet=ctx.title, reason=shift_reason or "unknown")
            return
    elif enable_history_shift and not checked_history:
        MIGRATION_CHECKED_SHEETS.add(ctx.ws_id)
    if needs_clan_gmt_column_drop:
        if not _remove_clan_gmt_column(ctx, write_limiter):
            return
    write_limiter.wait_for_token(1)
    updates = [
        {"range": "A1", "values": [[marker]]},
        {"range": "B1", "values": [[ctx.league_id]]},
        {"range": "A3", "values": [expected_header]},
    ]
    if marker == MAIN_SHEET_MARKER:
        updates.extend(
            [
                {"range": "G1", "values": [["last_roundId"]]},
                {"range": "I1", "values": [["last_abilities_logged_roundId"]]},
            ]
        )
    elif marker == CLAN_SHEET_MARKER:
        updates.append({"range": "C1", "values": [["last_roundId"]]})
    ctx.ws.batch_update(updates, value_input_option="RAW")
    state.upsert_sheet_meta(ctx.ws_id, ctx.title, ctx.league_id, layout_sig=layout_sig)
    log_info("sheet.layout_synced", sheet=ctx.title, action="header_refreshed")

def _clan_tab_title(main_title: str) -> str:
    title = f"{main_title}{CLAN_TAB_SUFFIX}"
    return title[:100]

def refresh_sheet_contexts(
    sh: Any,
    state: StateStore,
    main_expected_header: List[str],
    clan_expected_header: List[str],
    write_limiter: TokenBucket,
    read_limiter: TokenBucket,
    enable_clan_sync: bool = ENABLE_CLAN_SYNC,
    odyssey_sentinel_header: Optional[List[str]] = None,
    return_odyssey_sentinel: bool = False,
) -> Union[
    Tuple[Dict[int, SheetContext], Dict[int, SheetContext]],
    Tuple[Dict[int, SheetContext], Dict[int, SheetContext], Dict[int, SheetContext]],
]:
    read_limiter.wait_for_token(1)
    worksheets = sh.worksheets()
    selectors = read_sheet_selectors(sh, worksheets, read_limiter)
    main_contexts: Dict[int, SheetContext] = {}
    clan_contexts: Dict[int, SheetContext] = {}
    odyssey_sentinel_contexts: Dict[int, SheetContext] = {}
    pending_odyssey_sentinel: List[Any] = []
    main_layout_sig = row_checksum(main_expected_header)
    clan_layout_sig = row_checksum(clan_expected_header)
    odyssey_sentinel_header = list(odyssey_sentinel_header or ODYSSEY_SENTINEL_HEADERS)
    odyssey_sentinel_layout_sig = row_checksum(odyssey_sentinel_header)
    if DEBUG_VERBOSE:
        log_debug(
            "sheet.refresh_begin",
            worksheets=len(worksheets),
            main_header_cols=len(main_expected_header),
            clan_header_cols=len(clan_expected_header),
            clan_sync_enabled=enable_clan_sync,
        )

    for ws in worksheets:
        sel = selectors.get(ws.id, {})
        marker = str(sel.get("marker") or "")
        league_id = safe_int(sel.get("league_id"))
        title_is_odyssey_sentinel = str(getattr(ws, "title", "") or "") == ODYSSEY_SENTINEL_SHEET_TITLE
        if marker == ODYSSEY_SENTINEL_SHEET_MARKER or title_is_odyssey_sentinel:
            if league_id is None:
                pending_odyssey_sentinel.append(ws)
                continue
            odyssey_sentinel_contexts[ws.id] = SheetContext(
                ws_id=ws.id,
                title=ws.title,
                ws=ws,
                league_id=league_id,
                kind="odyssey_sentinel",
                expected_cols=len(odyssey_sentinel_header),
                round_col_idx=2,
            )
            continue
        if league_id is None:
            if DEBUG_VERBOSE:
                log_debug("sheet.refresh_skip_no_league", sheet=ws.title, ws_id=ws.id)
            continue
        if marker == MAIN_SHEET_MARKER:
            main_contexts[ws.id] = SheetContext(
                ws_id=ws.id,
                title=ws.title,
                ws=ws,
                league_id=league_id,
                kind="main",
                expected_cols=len(main_expected_header),
                round_col_idx=6,
            )
        elif marker == CLAN_SHEET_MARKER and enable_clan_sync:
            clan_contexts[ws.id] = SheetContext(
                ws_id=ws.id,
                title=ws.title,
                ws=ws,
                league_id=league_id,
                kind="clan",
                expected_cols=len(clan_expected_header),
                round_col_idx=2,
            )
        elif marker == CLAN_SHEET_MARKER and DEBUG_VERBOSE:
            log_debug("sheet.refresh_skip_clan_disabled", sheet=ws.title, ws_id=ws.id)
        elif DEBUG_VERBOSE:
            log_debug("sheet.refresh_skip_unknown_marker", sheet=ws.title, ws_id=ws.id, marker=marker)

    odyssey_main_candidates = [
        ctx for ctx in main_contexts.values() if "odyssey" in str(ctx.title or "").strip().lower()
    ]
    inferred_odyssey_league_id = odyssey_main_candidates[0].league_id if len(odyssey_main_candidates) == 1 else None
    for ws in pending_odyssey_sentinel:
        if inferred_odyssey_league_id is None:
            log_warn(
                "sheet.odyssey_sentinel_league_unresolved",
                sheet=ws.title,
                candidates=len(odyssey_main_candidates),
            )
            continue
        odyssey_sentinel_contexts[ws.id] = SheetContext(
            ws_id=ws.id,
            title=ws.title,
            ws=ws,
            league_id=inferred_odyssey_league_id,
            kind="odyssey_sentinel",
            expected_cols=len(odyssey_sentinel_header),
            round_col_idx=2,
        )

    if enable_clan_sync:
        clan_by_league = {ctx.league_id: ctx for ctx in clan_contexts.values()}
        for main_ctx in list(main_contexts.values()):
            if main_ctx.league_id in clan_by_league:
                continue
            clan_title = _clan_tab_title(main_ctx.title)
            if DRY_RUN:
                log_info(
                    "sheet.clan_tab_create_dry_skip",
                    league_id=main_ctx.league_id,
                    source_sheet=main_ctx.title,
                    target_title=clan_title,
                )
                continue
            try:
                write_limiter.wait_for_token(1)
                ws_new = sh.add_worksheet(
                    title=clan_title,
                    rows=max(1000, LOG_START_ROW + 50),
                    cols=max(26, len(clan_expected_header) + 2),
                )
                ctx = SheetContext(
                    ws_id=ws_new.id,
                    title=ws_new.title,
                    ws=ws_new,
                    league_id=main_ctx.league_id,
                    kind="clan",
                    expected_cols=len(clan_expected_header),
                    round_col_idx=2,
                )
                clan_contexts[ws_new.id] = ctx
                clan_by_league[ctx.league_id] = ctx
                log_info(
                    "sheet.clan_tab_created",
                    league_id=ctx.league_id,
                    sheet=ctx.title,
                    source_sheet=main_ctx.title,
                )
            except Exception as e:
                log_warn(
                    "sheet.clan_tab_create_failed",
                    league_id=main_ctx.league_id,
                    source_sheet=main_ctx.title,
                    target_title=clan_title,
                    err=repr(e),
                )

    for ctx in main_contexts.values():
        state.upsert_sheet_meta(ctx.ws_id, ctx.title, ctx.league_id, layout_sig=None)
        sync_layout_if_needed(
            ctx,
            main_expected_header,
            main_layout_sig,
            state,
            write_limiter,
            read_limiter,
            marker=MAIN_SHEET_MARKER,
            enable_history_shift=True,
        )
        ctx.next_row = _detect_next_row(ctx.ws, read_limiter, round_col_index=(ctx.round_col_idx or 6) + 1)
        if DEBUG_VERBOSE:
            log_debug("sheet.next_row_detected", sheet=ctx.title, ws_id=ctx.ws_id, kind=ctx.kind, next_row=ctx.next_row)

    for ctx in clan_contexts.values():
        state.upsert_sheet_meta(ctx.ws_id, ctx.title, ctx.league_id, layout_sig=None)
        sync_layout_if_needed(
            ctx,
            clan_expected_header,
            clan_layout_sig,
            state,
            write_limiter,
            read_limiter,
            marker=CLAN_SHEET_MARKER,
            enable_history_shift=False,
        )
        ctx.next_row = _detect_next_row(ctx.ws, read_limiter, round_col_index=(ctx.round_col_idx or 2) + 1)
        if DEBUG_VERBOSE:
            log_debug("sheet.next_row_detected", sheet=ctx.title, ws_id=ctx.ws_id, kind=ctx.kind, next_row=ctx.next_row)

    for ctx in odyssey_sentinel_contexts.values():
        state.upsert_sheet_meta(ctx.ws_id, ctx.title, ctx.league_id, layout_sig=None)
        sync_layout_if_needed(
            ctx,
            odyssey_sentinel_header,
            odyssey_sentinel_layout_sig,
            state,
            write_limiter,
            read_limiter,
            marker=ODYSSEY_SENTINEL_SHEET_MARKER,
            enable_history_shift=False,
        )
        ctx.next_row = _detect_next_row(ctx.ws, read_limiter, round_col_index=(ctx.round_col_idx or 2) + 1)
        if DEBUG_VERBOSE:
            log_debug("sheet.next_row_detected", sheet=ctx.title, ws_id=ctx.ws_id, kind=ctx.kind, next_row=ctx.next_row)

    if DEBUG_VERBOSE:
        log_debug(
            "sheet.refresh_done",
            main_contexts=len(main_contexts),
            clan_contexts=len(clan_contexts),
            odyssey_sentinel_contexts=len(odyssey_sentinel_contexts),
        )

    if return_odyssey_sentinel:
        return main_contexts, clan_contexts, odyssey_sentinel_contexts
    return main_contexts, clan_contexts
