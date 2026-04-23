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

from ..api.clan import GoMiningClanApiClient
from ..api.round_ability import GoMiningRoundAbilityApiClient
from ..config import GAP_SCAN_LOOKBACK_ROUNDS, MAX_ROUNDS_PER_POLL, RECONCILE_DEEP_MISSING_PER_PASS
from ..logging_utils import log_info
from ..runtime.rate_limit import TokenBucket
from ..sheets.context import SheetContext
from ..storage.state import StateStore
from ..utils import safe_int, to_iso_utc
from .enqueue import _enqueue_specific_main_rounds

def run_reconcile_pass(
    state: StateStore,
    main_contexts: Dict[int, SheetContext],
    ability_id_to_name: Dict[str, str],
    ability_headers: List[str],
    round_ability_api: GoMiningRoundAbilityApiClient,
    read_limiter: Optional[TokenBucket],
    power_up_clan_api: Optional[GoMiningClanApiClient],
) -> Dict[str, int]:
    repaired_rounds = 0
    queued_ops = 0
    sheets_with_gaps = 0
    for ws_id, ctx in main_contexts.items():
        latest_round = state.fetch_latest_api_completed_round_id(ctx.league_id)
        if latest_round is None:
            continue
        from_round = max(0, int(latest_round) - int(GAP_SCAN_LOOKBACK_ROUNDS))
        price_cutover = state.get_price_cutover_round(ws_id)
        deep_from_round = max(0, int(price_cutover)) if price_cutover is not None else 0
        records = state.fetch_api_completed_rounds(
            ctx.league_id,
            since_round=max(0, from_round - 1),
            limit=max(GAP_SCAN_LOOKBACK_ROUNDS * 2, MAX_ROUNDS_PER_POLL),
        )
        if not records:
            continue
        records = [r for r in records if to_iso_utc(r.get("ended_at"))]
        if not records:
            continue
        round_ids = sorted({int(safe_int(r.get("round_id")) or -1) for r in records if safe_int(r.get("round_id")) is not None})
        if not round_ids:
            continue
        row_map = state.get_round_row_map_bulk(ws_id, round_ids)
        retry_states = state.list_retryable_round_states(ctx.league_id, limit=MAX_ROUNDS_PER_POLL)
        retry_ids = {int(safe_int(x.get("round_id")) or -1) for x in retry_states if safe_int(x.get("round_id")) is not None}
        missing_ids = [rid for rid in round_ids if rid >= from_round and rid not in row_map]
        deep_missing_ids: List[int] = []
        if RECONCILE_DEEP_MISSING_PER_PASS > 0:
            deep_missing_ids = state.list_missing_row_map_round_ids(
                ws_id,
                ctx.league_id,
                from_round=deep_from_round,
                limit=RECONCILE_DEEP_MISSING_PER_PASS,
            )
        target_ids = sorted(
            {rid for rid in missing_ids if rid > 0}
            | {rid for rid in retry_ids if rid > 0}
            | {rid for rid in deep_missing_ids if rid > 0}
        )
        if not target_ids:
            continue
        sheets_with_gaps += 1
        rec_by_rid = {int(safe_int(r.get("round_id")) or -1): r for r in records if safe_int(r.get("round_id")) is not None}
        missing_record_ids = [rid for rid in target_ids if rid not in rec_by_rid]
        if missing_record_ids:
            extra_records = state.fetch_api_round_records_by_ids(ctx.league_id, missing_record_ids)
            for rec in extra_records:
                rid = safe_int(rec.get("round_id"))
                if rid is None:
                    continue
                rec_by_rid[int(rid)] = rec
        target_records = [rec_by_rid[rid] for rid in target_ids if rid in rec_by_rid]
        for rid in target_ids:
            state.upsert_round_processing_state(
                ctx.league_id,
                rid,
                metrics_status="ok",
                abilities_status="pending",
                sheet_status="pending",
                retry_after_ts=0.0,
            )
        enq = _enqueue_specific_main_rounds(
            state=state,
            ctx=ctx,
            records=target_records,
            ability_id_to_name=ability_id_to_name,
            ability_headers=ability_headers,
            round_ability_api=round_ability_api,
            read_limiter=read_limiter,
            power_up_clan_api=power_up_clan_api,
        )
        if enq > 0:
            queued_ops += enq
            repaired_rounds += len(target_records)
        log_info(
            "sync.reconcile_sheet",
            sheet=ctx.title,
            league_id=ctx.league_id,
            latest_seen=latest_round,
            lookback_from=from_round,
            deep_from=deep_from_round,
            deep_targets=len(deep_missing_ids),
            targets=len(target_records),
            queued=enq,
        )
    return {
        "sheets_with_gaps": sheets_with_gaps,
        "queued_ops": queued_ops,
        "repaired_rounds": repaired_rounds,
    }

def log_main_sheet_summaries(state: StateStore, main_contexts: Dict[int, SheetContext]) -> None:
    for ws_id, ctx in main_contexts.items():
        latest_seen = state.fetch_latest_api_completed_round_id(ctx.league_id)
        latest_written = state.get_last_synced_round(ws_id)
        lag_rounds = None
        if latest_seen is not None and latest_written is not None:
            lag_rounds = max(0, int(latest_seen) - int(latest_written))
        from_round = 0
        to_round = 0
        gap_count = 0
        if latest_seen is not None:
            from_round = max(0, int(latest_seen) - int(GAP_SCAN_LOOKBACK_ROUNDS))
            to_round = int(latest_seen)
            gap_count = state.count_missing_rows_for_range(ws_id, ctx.league_id, from_round, to_round)
        retry_backlog = len(state.list_retryable_round_states(ctx.league_id, limit=500))
        log_info(
            "sync.sheet_summary",
            sheet=ctx.title,
            league_id=ctx.league_id,
            latest_seen=latest_seen,
            latest_written=latest_written,
            lag_rounds=lag_rounds,
            gap_count=gap_count,
            retry_queue_depth=retry_backlog,
            lookback_from=from_round,
            lookback_to=to_round,
        )
