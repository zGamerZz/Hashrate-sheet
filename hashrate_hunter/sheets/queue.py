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

from ..config import *
from ..domain.rows import _normalize_payload_row
from ..logging_utils import log_debug, log_info, log_warn
from ..runtime.rate_limit import TokenBucket
from ..storage.state import StateStore
from ..utils import classify_sheet_error, safe_int
from .context import SheetContext

def _reschedule(state: StateStore, op_id: int, retry_count: int, retry_after: Optional[float] = None) -> None:
    delay = min(300.0, float(2 ** min(retry_count, 8))) + random.uniform(0.0, 1.0)
    if retry_after is not None:
        delay = max(delay, retry_after)
    if DEBUG_VERBOSE:
        log_debug("queue.reschedule_calc", op_id=op_id, retry_count=retry_count, retry_after=retry_after, delay_s=round(delay, 3))
    state.reschedule_op(op_id, retry_count, time.time() + delay)

def _handle_missing_sheet_op(state: StateStore, op: Dict[str, Any], retry_after: float = 30.0) -> None:
    op_id = int(op["id"])
    ws_id = int(op["sheet_id"])
    next_retry = int(op["retry_count"]) + 1
    if next_retry >= MAX_MISSING_SHEET_RETRIES:
        log_warn(
            "queue.drop_missing_sheet",
            sheet_id=ws_id,
            op_id=op_id,
            op_type=op.get("op_type"),
            round_id=op.get("round_id"),
            retries=next_retry,
        )
        state.delete_op(op_id)
        return
    _reschedule(state, op_id, next_retry, retry_after=retry_after)

def _ensure_sheet_row_capacity(ctx: SheetContext, required_last_row: int, write_limiter: TokenBucket) -> bool:
    current_rows = safe_int(getattr(ctx.ws, "row_count", None)) or 0
    if required_last_row <= current_rows:
        return True
    if not AUTO_EXPAND_SHEET_ROWS:
        return False

    add_rows = required_last_row - current_rows
    try:
        write_limiter.wait_for_token(1)
        ctx.ws.add_rows(add_rows)
        log_info(
            "sheet.rows_expanded",
            sheet=ctx.title,
            ws_id=ctx.ws_id,
            old_rows=current_rows,
            added_rows=add_rows,
            new_rows=current_rows + add_rows,
        )
        return True
    except Exception as e:
        log_warn(
            "sheet.rows_expand_failed",
            sheet=ctx.title,
            ws_id=ctx.ws_id,
            required_last_row=required_last_row,
            current_rows=current_rows,
            err=repr(e),
        )
        return False

def purge_stale_queue_ops(state: StateStore, contexts: Dict[int, SheetContext]) -> None:
    if not PURGE_QUEUE_FOR_MISSING_SHEETS or not contexts:
        return
    deleted = state.purge_ops_for_missing_sheets(contexts.keys())
    if deleted > 0:
        log_warn(
            "queue.purged_missing_sheets",
            deleted_ops=deleted,
            active_sheets=len(contexts),
        )

def flush_sheet_queue_with_rate_limit(state: StateStore, contexts: Dict[int, SheetContext], write_limiter: TokenBucket) -> int:
    ops = state.fetch_due_ops(QUEUE_FETCH_LIMIT)
    if not ops:
        return 0

    if DEBUG_VERBOSE:
        by_type: Dict[str, int] = {}
        for op in ops:
            t = str(op.get("op_type"))
            by_type[t] = by_type.get(t, 0) + 1
        log_debug("queue.flush_start", due=len(ops), by_type=by_type, write_limiter=write_limiter.snapshot())

    processed = 0
    i = 0
    while i < len(ops):
        op = ops[i]
        op_type = str(op["op_type"])

        if op_type == "append_round":
            batch = [op]
            j = i + 1
            while j < len(ops) and len(batch) < APPEND_BATCH_SIZE:
                n = ops[j]
                if str(n["op_type"]) == "append_round" and int(n["sheet_id"]) == int(op["sheet_id"]):
                    batch.append(n)
                    j += 1
                else:
                    break
            i = j

            ws_id = int(op["sheet_id"])
            ctx = contexts.get(ws_id)
            if not ctx:
                for b in batch:
                    _handle_missing_sheet_op(state, b, retry_after=30.0)
                continue

            try:
                payloads = [json.loads(str(b["payload_json"])) for b in batch]
                rows: List[List[Any]] = []
                valid_batch: List[Dict[str, Any]] = []
                valid_payloads: List[Dict[str, Any]] = []
                for b, p in zip(batch, payloads):
                    rid = int(b["round_id"])
                    norm = _normalize_payload_row(
                        p.get("row"),
                        ctx.expected_cols,
                        rid,
                        round_col_idx=ctx.round_col_idx,
                    )
                    if norm is None:
                        log_warn("queue.append_drop_invalid_payload", sheet=ctx.title, op_id=b["id"], round_id=rid)
                        state.delete_op(int(b["id"]))
                        continue
                    p["row"] = norm
                    rows.append(norm)
                    valid_batch.append(b)
                    valid_payloads.append(p)
                if not valid_batch:
                    continue
                if DRY_RUN:
                    log_info("queue.append_dry_skip", sheet=ctx.title, count=len(rows))
                    continue

                # Deterministic contiguous write under sheet-detected next row.
                start_row = max(LOG_START_ROW, ctx.next_row)
                end_row = start_row + len(valid_batch) - 1
                if not _ensure_sheet_row_capacity(ctx, end_row, write_limiter):
                    for b in valid_batch:
                        _reschedule(state, int(b["id"]), int(b["retry_count"]) + 1, retry_after=30.0)
                    log_warn(
                        "queue.append_capacity_pending",
                        sheet=ctx.title,
                        ws_id=ws_id,
                        required_last_row=end_row,
                        size=len(valid_batch),
                    )
                    continue
                write_limiter.wait_for_token(1)
                ctx.ws.update(range_name=f"A{start_row}", values=rows, value_input_option="RAW")

                for k, b in enumerate(valid_batch):
                    rid = int(b["round_id"])
                    checksum = str(b["checksum"])
                    finalized = int(valid_payloads[k].get("finalized", 0))
                    row_idx = start_row + k
                    state.upsert_row_map(ws_id, rid, row_idx, checksum, finalized)
                    state.set_last_synced_round(ws_id, rid)
                    state.upsert_round_processing_state(
                        ctx.league_id,
                        rid,
                        metrics_status="ok",
                        abilities_status="ok",
                        sheet_status="ok",
                        last_error=None,
                        retry_after_ts=0.0,
                        attempt_count=0,
                    )
                    state.delete_op(int(b["id"]))
                    processed += 1
                    if DEBUG_VERBOSE:
                        log_debug(
                            "queue.append_row_mapped",
                            sheet=ctx.title,
                            round_id=rid,
                            row_idx=row_idx,
                            finalized=finalized,
                            checksum=checksum[:12],
                        )

                ctx.next_row = end_row + 1
                state.prune_row_map(ws_id, ROW_MAP_KEEP_PER_SHEET)
                log_info(
                    "queue.append_applied",
                    sheet=ctx.title,
                    rows=len(valid_batch),
                    start_row=start_row,
                    end_row=end_row,
                    first_round=valid_batch[0]["round_id"],
                    last_round=valid_batch[-1]["round_id"],
                    next_row=ctx.next_row,
                )

            except Exception as e:
                retryable, retry_after, status = classify_sheet_error(e)
                for b in batch:
                    op_id = int(b["id"])
                    next_retry = int(b["retry_count"]) + 1
                    rid = safe_int(b.get("round_id"))
                    if rid is not None:
                        state.upsert_round_processing_state(
                            ctx.league_id,
                            rid,
                            sheet_status="failed",
                            last_error=repr(e),
                            retry_after_ts=time.time() + min(ROUND_RETRY_BACKOFF_MAX_SECONDS, ROUND_RETRY_BACKOFF_MIN_SECONDS * (2 ** max(0, min(next_retry, 8) - 1))),
                            attempt_count=min(ROUND_SOFT_FAIL_MAX_ATTEMPTS, next_retry),
                        )
                    if DROP_NON_RETRYABLE_SHEET_ERRORS and not retryable:
                        log_warn(
                            "queue.append_drop_non_retryable",
                            sheet=ctx.title,
                            ws_id=ws_id,
                            op_id=op_id,
                            round_id=b.get("round_id"),
                            status=status,
                            err=repr(e),
                        )
                        state.delete_op(op_id)
                    else:
                        _reschedule(state, op_id, next_retry, retry_after)
                log_warn(
                    "queue.append_failed",
                    sheet=ctx.title,
                    size=len(batch),
                    status=status,
                    retryable=retryable,
                    err=repr(e),
                )

        elif op_type == "update_round":
            i += 1
            ws_id = int(op["sheet_id"])
            ctx = contexts.get(ws_id)
            if not ctx:
                _handle_missing_sheet_op(state, op, retry_after=30.0)
                continue
            try:
                payload = json.loads(str(op["payload_json"]))
                rid = int(op["round_id"])
                row_idx = int(payload["row_idx"])
                row = _normalize_payload_row(
                    payload.get("row"),
                    ctx.expected_cols,
                    rid,
                    round_col_idx=ctx.round_col_idx,
                )
                if row is None:
                    log_warn("queue.update_drop_invalid_payload", sheet=ctx.title, op_id=op["id"], round_id=rid)
                    state.delete_op(int(op["id"]))
                    continue
                finalized = int(payload.get("finalized", 0))
                if DRY_RUN:
                    log_info("queue.update_dry_skip", sheet=ctx.title, round_id=rid, row_idx=row_idx)
                    continue

                if not _ensure_sheet_row_capacity(ctx, row_idx, write_limiter):
                    _reschedule(state, int(op["id"]), int(op["retry_count"]) + 1, retry_after=30.0)
                    log_warn(
                        "queue.update_capacity_pending",
                        sheet=ctx.title,
                        ws_id=ws_id,
                        round_id=rid,
                        row_idx=row_idx,
                    )
                    continue
                write_limiter.wait_for_token(1)
                ctx.ws.update(range_name=f"A{row_idx}", values=[row], value_input_option="RAW")
                state.upsert_row_map(ws_id, rid, row_idx, str(op["checksum"]), finalized)
                state.set_last_synced_round(ws_id, rid)
                state.upsert_round_processing_state(
                    ctx.league_id,
                    rid,
                    metrics_status="ok",
                    abilities_status="ok",
                    sheet_status="ok",
                    last_error=None,
                    retry_after_ts=0.0,
                    attempt_count=0,
                )
                state.delete_op(int(op["id"]))
                processed += 1
                if DEBUG_VERBOSE:
                    log_debug(
                        "queue.update_applied",
                        sheet=ctx.title,
                        round_id=rid,
                        row_idx=row_idx,
                        finalized=finalized,
                        checksum=str(op["checksum"])[:12],
                    )
            except Exception as e:
                retryable, retry_after, status = classify_sheet_error(e)
                op_id = int(op["id"])
                next_retry = int(op["retry_count"]) + 1
                rid_for_state = safe_int(op.get("round_id"))
                if rid_for_state is not None:
                    state.upsert_round_processing_state(
                        ctx.league_id,
                        rid_for_state,
                        sheet_status="failed",
                        last_error=repr(e),
                        retry_after_ts=time.time() + min(ROUND_RETRY_BACKOFF_MAX_SECONDS, ROUND_RETRY_BACKOFF_MIN_SECONDS * (2 ** max(0, min(next_retry, 8) - 1))),
                        attempt_count=min(ROUND_SOFT_FAIL_MAX_ATTEMPTS, next_retry),
                    )
                if DROP_NON_RETRYABLE_SHEET_ERRORS and not retryable:
                    log_warn(
                        "queue.update_drop_non_retryable",
                        sheet=ctx.title,
                        ws_id=ws_id,
                        op_id=op_id,
                        round_id=op.get("round_id"),
                        status=status,
                        err=repr(e),
                    )
                    state.delete_op(op_id)
                else:
                    _reschedule(state, op_id, next_retry, retry_after)
                log_warn(
                    "queue.update_failed",
                    sheet=ctx.title,
                    op_id=op["id"],
                    status=status,
                    retryable=retryable,
                    err=repr(e),
                )

        elif op_type == "append_clan_round":
            i += 1
            ws_id = int(op["sheet_id"])
            ctx = contexts.get(ws_id)
            if not ctx:
                _handle_missing_sheet_op(state, op, retry_after=30.0)
                continue
            try:
                payload = json.loads(str(op["payload_json"]))
                rid = int(op["round_id"])
                rows_payload = payload.get("rows")
                if not isinstance(rows_payload, list) or not rows_payload:
                    log_warn("queue.clan_append_drop_invalid_payload", sheet=ctx.title, op_id=op["id"], round_id=rid)
                    state.delete_op(int(op["id"]))
                    continue
                rows: List[List[Any]] = []
                for row in rows_payload:
                    norm = _normalize_payload_row(
                        row,
                        ctx.expected_cols,
                        rid,
                        round_col_idx=ctx.round_col_idx,
                    )
                    if norm is None:
                        rows = []
                        break
                    rows.append(norm)
                if not rows:
                    log_warn("queue.clan_append_drop_invalid_rows", sheet=ctx.title, op_id=op["id"], round_id=rid)
                    state.delete_op(int(op["id"]))
                    continue
                if DRY_RUN:
                    log_info("queue.clan_append_dry_skip", sheet=ctx.title, round_id=rid, rows=len(rows))
                    continue

                start_row = max(LOG_START_ROW, ctx.next_row)
                end_row = start_row + len(rows) - 1
                if not _ensure_sheet_row_capacity(ctx, end_row, write_limiter):
                    _reschedule(state, int(op["id"]), int(op["retry_count"]) + 1, retry_after=30.0)
                    log_warn(
                        "queue.clan_append_capacity_pending",
                        sheet=ctx.title,
                        ws_id=ws_id,
                        round_id=rid,
                        required_last_row=end_row,
                        rows=len(rows),
                    )
                    continue

                write_limiter.wait_for_token(1)
                ctx.ws.update(range_name=f"A{start_row}", values=rows, value_input_option="RAW")
                state.upsert_row_map(ws_id, rid, start_row, str(op["checksum"]), 1)
                state.set_last_synced_round(ws_id, rid)
                state.delete_op(int(op["id"]))
                ctx.next_row = end_row + 1
                state.prune_row_map(ws_id, ROW_MAP_KEEP_PER_SHEET)
                processed += 1
                log_info(
                    "queue.clan_append_applied",
                    sheet=ctx.title,
                    round_id=rid,
                    rows=len(rows),
                    start_row=start_row,
                    end_row=end_row,
                    next_row=ctx.next_row,
                )
            except Exception as e:
                retryable, retry_after, status = classify_sheet_error(e)
                op_id = int(op["id"])
                next_retry = int(op["retry_count"]) + 1
                if DROP_NON_RETRYABLE_SHEET_ERRORS and not retryable:
                    log_warn(
                        "queue.clan_append_drop_non_retryable",
                        sheet=ctx.title,
                        ws_id=ws_id,
                        op_id=op_id,
                        round_id=op.get("round_id"),
                        status=status,
                        err=repr(e),
                    )
                    state.delete_op(op_id)
                else:
                    _reschedule(state, op_id, next_retry, retry_after)
                log_warn(
                    "queue.clan_append_failed",
                    sheet=ctx.title,
                    op_id=op_id,
                    status=status,
                    retryable=retryable,
                    err=repr(e),
                )

        elif op_type == "append_odyssey_sentinel_round":
            i += 1
            ws_id = int(op["sheet_id"])
            ctx = contexts.get(ws_id)
            if not ctx:
                _handle_missing_sheet_op(state, op, retry_after=30.0)
                continue
            try:
                payload = json.loads(str(op["payload_json"]))
                rid = int(op["round_id"])
                rows_payload = payload.get("rows")
                if not isinstance(rows_payload, list) or not rows_payload:
                    log_warn("queue.odyssey_sentinel_append_drop_invalid_payload", sheet=ctx.title, op_id=op["id"], round_id=rid)
                    state.delete_op(int(op["id"]))
                    continue
                rows: List[List[Any]] = []
                for row in rows_payload:
                    norm = _normalize_payload_row(
                        row,
                        ctx.expected_cols,
                        rid,
                        round_col_idx=ctx.round_col_idx,
                    )
                    if norm is None:
                        rows = []
                        break
                    rows.append(norm)
                if not rows:
                    log_warn("queue.odyssey_sentinel_append_drop_invalid_rows", sheet=ctx.title, op_id=op["id"], round_id=rid)
                    state.delete_op(int(op["id"]))
                    continue
                if DRY_RUN:
                    log_info("queue.odyssey_sentinel_append_dry_skip", sheet=ctx.title, round_id=rid, rows=len(rows))
                    continue

                start_row = max(LOG_START_ROW, ctx.next_row)
                end_row = start_row + len(rows) - 1
                if not _ensure_sheet_row_capacity(ctx, end_row, write_limiter):
                    _reschedule(state, int(op["id"]), int(op["retry_count"]) + 1, retry_after=30.0)
                    log_warn(
                        "queue.odyssey_sentinel_append_capacity_pending",
                        sheet=ctx.title,
                        ws_id=ws_id,
                        round_id=rid,
                        required_last_row=end_row,
                        rows=len(rows),
                    )
                    continue

                write_limiter.wait_for_token(1)
                ctx.ws.update(range_name=f"A{start_row}", values=rows, value_input_option="RAW")
                state.upsert_row_map(ws_id, rid, start_row, str(op["checksum"]), 1)
                state.set_last_synced_round(ws_id, rid)
                state.delete_op(int(op["id"]))
                ctx.next_row = end_row + 1
                state.prune_row_map(ws_id, ROW_MAP_KEEP_PER_SHEET)
                processed += 1
                log_info(
                    "queue.odyssey_sentinel_append_applied",
                    sheet=ctx.title,
                    round_id=rid,
                    rows=len(rows),
                    start_row=start_row,
                    end_row=end_row,
                    next_row=ctx.next_row,
                )
            except Exception as e:
                retryable, retry_after, status = classify_sheet_error(e)
                op_id = int(op["id"])
                next_retry = int(op["retry_count"]) + 1
                if DROP_NON_RETRYABLE_SHEET_ERRORS and not retryable:
                    log_warn(
                        "queue.odyssey_sentinel_append_drop_non_retryable",
                        sheet=ctx.title,
                        ws_id=ws_id,
                        op_id=op_id,
                        round_id=op.get("round_id"),
                        status=status,
                        err=repr(e),
                    )
                    state.delete_op(op_id)
                else:
                    _reschedule(state, op_id, next_retry, retry_after)
                log_warn(
                    "queue.odyssey_sentinel_append_failed",
                    sheet=ctx.title,
                    op_id=op_id,
                    status=status,
                    retryable=retryable,
                    err=repr(e),
                )

        else:
            i += 1
            log_warn("queue.unknown_op_dropped", op_type=op_type, op_id=op["id"])
            state.delete_op(int(op["id"]))

    return processed
