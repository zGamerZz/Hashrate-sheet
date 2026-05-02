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
import traceback
import weakref
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import requests

from ..api.clan import GoMiningClanApiClient
from ..api.league_index import _to_api_calculated_at
from ..api.round_ability import GoMiningRoundAbilityApiClient
from ..config import *
from ..domain.pricing import (
    calc_boost_gmt_from_api_round,
    calc_clan_power_up_gmt_pair_from_boost_users_api,
    calc_power_up_gmt_triplet_from_power_up_users_api,
)
from ..domain.rows import build_canonical_row, build_clan_round_rows
from ..logging_utils import log_debug, log_info, log_warn
from ..runtime.rate_limit import TokenBucket
from ..sheets.context import SheetContext
from ..sheets.layout import _read_round_row_index
from ..storage.legacy_db import DBClient
from ..storage.state import StateStore
from ..utils import row_checksum, safe_float, safe_int, to_iso_utc, utc_now_iso
from .cache import fetch_completed_rounds_prefer_api, fetch_latest_completed_round_id_prefer_api

def _maybe_flush_during_enqueue(
    flush_tick: Optional[Callable[[], int]],
    last_flush_monotonic: float,
    flush_every_seconds: float,
    *,
    scope: str,
    sheet: str,
    round_id: Optional[int],
) -> float:
    if flush_tick is None:
        return last_flush_monotonic
    now_mono = time.monotonic()
    if flush_every_seconds > 0 and (now_mono - last_flush_monotonic) < flush_every_seconds:
        return last_flush_monotonic
    try:
        processed = int(flush_tick() or 0)
        if processed > 0:
            log_info(
                "queue.flushed_during_enqueue",
                scope=scope,
                sheet=sheet,
                round_id=round_id,
                processed=processed,
            )
    except Exception as e:
        log_warn(
            "queue.flush_during_enqueue_failed",
            scope=scope,
            sheet=sheet,
            round_id=round_id,
            err=repr(e),
        )
    return now_mono

def enqueue_main_sheet_ops(
    db: Optional[DBClient],
    state: StateStore,
    contexts: Dict[int, SheetContext],
    ability_id_to_name: Dict[str, str],
    ability_headers: List[str],
    round_ability_api: GoMiningRoundAbilityApiClient,
    read_limiter: Optional[TokenBucket] = None,
    power_up_clan_api: Optional[GoMiningClanApiClient] = None,
    flush_tick: Optional[Callable[[], int]] = None,
    flush_every_seconds: float = ENQUEUE_FLUSH_EVERY_SECONDS,
) -> int:
    enqueued = 0
    ability_header_order = {name: idx for idx, name in enumerate(ability_headers)}
    for ws_id, ctx in contexts.items():
        last_flush_monotonic = time.monotonic() - max(0.0, flush_every_seconds)

        def _progress_tick(round_id: Optional[int]) -> None:
            nonlocal last_flush_monotonic
            last_flush_monotonic = _maybe_flush_during_enqueue(
                flush_tick,
                last_flush_monotonic,
                flush_every_seconds,
                scope="main",
                sheet=ctx.title,
                round_id=round_id,
            )

        state.upsert_sheet_meta(ws_id, ctx.title, ctx.league_id, layout_sig=None)
        last_synced = state.get_last_synced_round(ws_id)

        if last_synced is None:
            cutover = fetch_latest_completed_round_id_prefer_api(
                db,
                state,
                ctx.league_id,
            )
            if cutover is None:
                cutover = 0
            state.set_last_synced_round(ws_id, cutover)
            state.set_price_cutover_round(ws_id, cutover)
            log_info("sync.init_cutover", sheet=ctx.title, league_id=ctx.league_id, cutover_round=cutover)
            continue

        price_cutover = state.get_price_cutover_round(ws_id)
        if price_cutover is None:
            state.set_price_cutover_round(ws_id, last_synced)
            price_cutover = last_synced
            log_info("sync.price_cutover_initialized", sheet=ctx.title, league_id=ctx.league_id, cutover_round=price_cutover)

        since_round = max(0, last_synced - STABILIZATION_ROUNDS)
        rounds = fetch_completed_rounds_prefer_api(
            db,
            state,
            ctx.league_id,
            since_round,
            MAX_ROUNDS_PER_POLL,
        )
        if DEBUG_VERBOSE:
            log_debug(
                "sync.sheet_poll",
                sheet=ctx.title,
                ws_id=ws_id,
                league_id=ctx.league_id,
                last_synced=last_synced,
                since_round=since_round,
                fetched_rounds=len(rounds),
            )
        if not rounds:
            continue

        rounds_sorted = sorted(rounds, key=lambda r: safe_int(r.get("round_id")) or -1)
        round_ids = [safe_int(r.get("round_id")) for r in rounds_sorted]
        round_ids = [x for x in round_ids if x is not None]
        if not round_ids:
            continue

        max_round = max(round_ids)
        row_maps = state.get_round_row_map_bulk(ws_id, round_ids)
        sheet_round_index: Dict[int, int] = {}
        if ctx.ws is not None and read_limiter is not None:
            sheet_round_index = _read_round_row_index(
                ctx.ws,
                read_limiter,
                round_col_index=(ctx.round_col_idx or 6) + 1,
            )
        counts_by_round: Dict[int, Optional[Dict[str, int]]] = {}

        for rec in rounds_sorted:
            rid = safe_int(rec.get("round_id"))
            if rid is None:
                continue
            rstate = state.get_round_processing_state(ctx.league_id, rid) or {}
            retry_after_ts = float(rstate.get("retry_after_ts") or 0.0)
            if retry_after_ts > time.time():
                if DEBUG_VERBOSE:
                    log_debug(
                        "sync.round_retry_wait",
                        sheet=ctx.title,
                        league_id=ctx.league_id,
                        round_id=rid,
                        retry_after_s=round(retry_after_ts - time.time(), 2),
                    )
                continue
            _progress_tick(rid)
            latest_last_synced = state.get_last_synced_round(ws_id)
            if latest_last_synced is not None and latest_last_synced > last_synced:
                last_synced = latest_last_synced
            mapped = row_maps.get(rid)
            if mapped is None:
                fresh_map = state.get_round_row_map_bulk(ws_id, [rid]).get(rid)
                if fresh_map is not None:
                    if isinstance(fresh_map, dict):
                        mapped = fresh_map
                        row_maps[rid] = fresh_map
                    else:
                        log_warn(
                            "sync.round_row_map_invalid_shape",
                            sheet=ctx.title,
                            round_id=rid,
                            typ=type(fresh_map).__name__,
                        )
            if mapped is None:
                existing_row_idx = sheet_round_index.get(rid)
                if existing_row_idx is not None:
                    # Reconstruct missing local mapping for rows already present in sheet.
                    state.upsert_row_map(ws_id, rid, existing_row_idx, "", 0)
                    mapped = {"row_idx": existing_row_idx, "checksum": "", "finalized": 0}
                    row_maps[rid] = mapped
                    if DEBUG_VERBOSE:
                        log_debug(
                            "sync.round_row_map_recovered_from_sheet",
                            sheet=ctx.title,
                            round_id=rid,
                            row_idx=existing_row_idx,
                        )
                elif rid <= last_synced:
                    # Real hole: round is <= last_synced but missing in both row_map and sheet.
                    # Keep processing so it can be appended/backfilled.
                    log_warn(
                        "sync.round_hole_detected",
                        sheet=ctx.title,
                        round_id=rid,
                        last_synced=last_synced,
                    )
            elif not isinstance(mapped, dict):
                log_warn(
                    "sync.round_row_map_invalid_shape",
                    sheet=ctx.title,
                    round_id=rid,
                    typ=type(mapped).__name__,
                )
                mapped = None

            counts_source = "cache"
            if rid not in counts_by_round:
                # API is the only source for ability counts to guarantee user-specific exclusions.
                api_counts = round_ability_api.fetch_round_ability_counts(
                    rid,
                    expected_league_id=ctx.league_id,
                    progress_hook=lambda rid=rid: _progress_tick(rid),
                )
                counts_by_round[rid] = api_counts
                counts_source = "api"
            counts_by_id = counts_by_round.get(rid)
            if DEBUG_VERBOSE and counts_by_id is not None:
                log_debug(
                    "sync.round_ability_source",
                    sheet=ctx.title,
                    league_id=ctx.league_id,
                    round_id=rid,
                    source=counts_source,
                    abilities=len(counts_by_id),
                )
            if counts_by_id is None:
                lag_to_latest = max_round - rid
                prev_attempt = int(rstate.get("attempt_count") or 0)
                next_attempt = min(ROUND_SOFT_FAIL_MAX_ATTEMPTS, prev_attempt + 1)
                delay_s = min(
                    ROUND_RETRY_BACKOFF_MAX_SECONDS,
                    ROUND_RETRY_BACKOFF_MIN_SECONDS * (2 ** max(0, next_attempt - 1)),
                )
                state.upsert_round_processing_state(
                    ctx.league_id,
                    rid,
                    metrics_status="ok",
                    abilities_status="failed",
                    sheet_status="pending",
                    last_error="abilities_api_unavailable",
                    retry_after_ts=time.time() + delay_s,
                    attempt_count=next_attempt,
                )
                log_warn(
                    "sync.round_abilities_soft_fail",
                    sheet=ctx.title,
                    league_id=ctx.league_id,
                    round_id=rid,
                    last_synced=last_synced,
                    max_round=max_round,
                    lag_to_latest=lag_to_latest,
                    retry_in_s=round(delay_s, 1),
                    attempt=next_attempt,
                )
                continue

            counts_by_name: Dict[str, int] = {}
            unknown_ability_ids: List[str] = []
            for aid, cnt in counts_by_id.items():
                aname = ability_id_to_name.get(aid)
                if not aname:
                    unknown_ability_ids.append(aid)
                    continue
                counts_by_name[aname] = counts_by_name.get(aname, 0) + int(cnt)

            if unknown_ability_ids:
                unknown_unique = sorted(set(unknown_ability_ids))
                log_warn(
                    "sync.round_unknown_ability_ids",
                    sheet=ctx.title,
                    league_id=ctx.league_id,
                    round_id=rid,
                    unknown_count=len(unknown_unique),
                    unknown_ids=unknown_unique[:10],
                )

            excluded_user_boosts_audit = ""
            get_cached_excluded_boosts = getattr(
                round_ability_api,
                "get_cached_excluded_user_boosts_for_round",
                None,
            )
            if callable(get_cached_excluded_boosts):
                try:
                    excluded_counts_by_id = get_cached_excluded_boosts(rid)
                except Exception as e:
                    excluded_counts_by_id = {}
                    log_warn(
                        "sync.round_excluded_user_audit_cache_read_failed",
                        sheet=ctx.title,
                        league_id=ctx.league_id,
                        round_id=rid,
                        user_id=EXCLUDED_BOOST_USER_ID,
                        err=repr(e),
                    )
                if isinstance(excluded_counts_by_id, dict) and excluded_counts_by_id:
                    known_counts: Dict[str, int] = {}
                    unknown_counts: Dict[str, int] = {}
                    for ability_id_raw, cnt_raw in excluded_counts_by_id.items():
                        aid = str(ability_id_raw or "").strip()
                        cnt = safe_int(cnt_raw) or 0
                        if not aid or cnt <= 0:
                            continue
                        header_name = ability_id_to_name.get(aid)
                        if header_name:
                            known_counts[header_name] = known_counts.get(header_name, 0) + cnt
                        else:
                            unknown_counts[aid] = unknown_counts.get(aid, 0) + cnt
                    parts: List[str] = []
                    for name, cnt in sorted(
                        known_counts.items(),
                        key=lambda x: (ability_header_order.get(x[0], 9999), x[0]),
                    ):
                        parts.append(f"{name}={cnt}")
                    for aid, cnt in sorted(unknown_counts.items(), key=lambda x: x[0]):
                        parts.append(f"ability_id={aid}:{cnt}")
                    excluded_user_boosts_audit = "; ".join(parts)

            power_up_gmt_value: Optional[float] = None
            power_up_gmt_sentinel_value: Optional[float] = None
            clan_power_up_gmt_value: Optional[float] = None
            clan_power_up_gmt_sentinel_value: Optional[float] = None
            power_up_missing_aliases: List[str] = []

            get_cached_users = getattr(round_ability_api, "get_cached_ability_users_for_round", None)
            get_tracked_user_blocks_mined = getattr(round_ability_api, "get_cached_tracked_user_blocks_mined_for_round", None)
            refetched_users_for_round = False

            if callable(get_cached_users):
                power_up_count = int(counts_by_name.get("Power Up Boost", 0) or 0)
                power_up_users: List[Dict[str, Any]] = []
                if power_up_count <= 0:
                    power_up_gmt_value = 0.0
                    power_up_gmt_sentinel_value = 0.0
                    power_up_missing_aliases = []
                else:
                    try:
                        cached_users = get_cached_users(rid, POWER_UP_ABILITY_ID)
                        if isinstance(cached_users, list):
                            power_up_users = [u for u in cached_users if isinstance(u, dict)]
                    except Exception as e:
                        log_warn(
                            "sync.round_power_up_cache_read_failed",
                            sheet=ctx.title,
                            league_id=ctx.league_id,
                            round_id=rid,
                            err=repr(e),
                        )
                    if not power_up_users and not refetched_users_for_round:
                        # Pull API user-leaderboard once to populate exact per-user boost users for pricing.
                        _ = round_ability_api.fetch_round_ability_counts(
                            rid,
                            expected_league_id=ctx.league_id,
                            progress_hook=lambda rid=rid: _progress_tick(rid),
                        )
                        refetched_users_for_round = True
                    if not power_up_users:
                        try:
                            cached_users = get_cached_users(rid, POWER_UP_ABILITY_ID)
                            if isinstance(cached_users, list):
                                power_up_users = [u for u in cached_users if isinstance(u, dict)]
                        except Exception as e:
                            log_warn(
                                "sync.round_power_up_cache_read_failed_after_refetch",
                                sheet=ctx.title,
                                league_id=ctx.league_id,
                                round_id=rid,
                                err=repr(e),
                            )
                    if power_up_users:
                        power_up_resolution_stats: Dict[str, int] = {}
                        try:
                            exact_power_up_gmt, exact_power_up_gmt_sentinel, exact_missing = calc_power_up_gmt_triplet_from_power_up_users_api(
                                power_up_users,
                                power_up_clan_api,
                                resolution_stats=power_up_resolution_stats,
                            )
                        except Exception as e:
                            exact_power_up_gmt, exact_power_up_gmt_sentinel, exact_missing = None, None, []
                            log_warn(
                                "sync.round_power_up_exact_calc_failed",
                                sheet=ctx.title,
                                league_id=ctx.league_id,
                                round_id=rid,
                                users=len(power_up_users),
                                boost_count=power_up_count,
                                err=repr(e),
                            )
                        resolved_api = int(power_up_resolution_stats.get("resolved_api", 0))
                        missing_count = int(power_up_resolution_stats.get("missing", 0))
                        if resolved_api <= 0:
                            power_up_resolution_source = "unresolved"
                        else:
                            power_up_resolution_source = "api_only"
                        log_debug(
                            "sync.round_power_up_resolution",
                            sheet=ctx.title,
                            league_id=ctx.league_id,
                            round_id=rid,
                            power_up_resolution_source=power_up_resolution_source,
                            resolved_api=resolved_api,
                            missing=missing_count,
                            users=len(power_up_users),
                            boost_count=power_up_count,
                        )
                        power_up_missing_aliases = [str(x).strip() for x in (exact_missing or []) if str(x).strip()]
                        if exact_power_up_gmt is not None and exact_power_up_gmt_sentinel is not None:
                            power_up_gmt_value = exact_power_up_gmt
                            power_up_gmt_sentinel_value = exact_power_up_gmt_sentinel
                        else:
                            power_up_gmt_value = None
                            power_up_gmt_sentinel_value = None
                            log_warn(
                                "sync.round_power_up_exact_unavailable",
                                sheet=ctx.title,
                                league_id=ctx.league_id,
                                round_id=rid,
                                users=len(power_up_users),
                                missing=len(power_up_missing_aliases),
                                missing_aliases=power_up_missing_aliases[:20],
                                boost_count=power_up_count,
                            )
                    else:
                        power_up_gmt_value = None
                        power_up_gmt_sentinel_value = None
                        power_up_missing_aliases = []
                        log_warn(
                            "sync.round_power_up_users_missing",
                            sheet=ctx.title,
                            league_id=ctx.league_id,
                            round_id=rid,
                            boost_count=power_up_count,
                            counts_source=counts_source,
                        )

                clan_power_up_count = int(counts_by_name.get("Clan Power Up Boost", 0) or 0)
                clan_power_up_users: List[Dict[str, Any]] = []
                if clan_power_up_count <= 0:
                    clan_power_up_gmt_value = 0.0
                    clan_power_up_gmt_sentinel_value = 0.0
                else:
                    try:
                        cached_users = get_cached_users(rid, CLAN_POWER_UP_ABILITY_ID)
                        if isinstance(cached_users, list):
                            clan_power_up_users = [u for u in cached_users if isinstance(u, dict)]
                    except Exception as e:
                        log_warn(
                            "sync.round_clan_power_up_cache_read_failed",
                            sheet=ctx.title,
                            league_id=ctx.league_id,
                            round_id=rid,
                            err=repr(e),
                        )
                    if not clan_power_up_users and not refetched_users_for_round:
                        _ = round_ability_api.fetch_round_ability_counts(
                            rid,
                            expected_league_id=ctx.league_id,
                            progress_hook=lambda rid=rid: _progress_tick(rid),
                        )
                        refetched_users_for_round = True
                    if not clan_power_up_users:
                        try:
                            cached_users = get_cached_users(rid, CLAN_POWER_UP_ABILITY_ID)
                            if isinstance(cached_users, list):
                                clan_power_up_users = [u for u in cached_users if isinstance(u, dict)]
                        except Exception as e:
                            log_warn(
                                "sync.round_clan_power_up_cache_read_failed_after_refetch",
                                sheet=ctx.title,
                                league_id=ctx.league_id,
                                round_id=rid,
                                err=repr(e),
                            )
                    if clan_power_up_users:
                        try:
                            exact_clan_power_up_gmt, exact_clan_power_up_gmt_sentinel = calc_clan_power_up_gmt_pair_from_boost_users_api(
                                clan_power_up_users,
                                power_up_clan_api,
                            )
                        except Exception as e:
                            exact_clan_power_up_gmt, exact_clan_power_up_gmt_sentinel = None, None
                            log_warn(
                                "sync.round_clan_power_up_exact_calc_failed",
                                sheet=ctx.title,
                                league_id=ctx.league_id,
                                round_id=rid,
                                users=len(clan_power_up_users),
                                boost_count=clan_power_up_count,
                                err=repr(e),
                            )
                        if exact_clan_power_up_gmt is not None and exact_clan_power_up_gmt_sentinel is not None:
                            clan_power_up_gmt_value = exact_clan_power_up_gmt
                            clan_power_up_gmt_sentinel_value = exact_clan_power_up_gmt_sentinel
                        else:
                            clan_power_up_gmt_value = None
                            clan_power_up_gmt_sentinel_value = None
                            log_warn(
                            "sync.round_clan_power_up_exact_unavailable",
                            sheet=ctx.title,
                            league_id=ctx.league_id,
                            round_id=rid,
                            users=len(clan_power_up_users),
                            boost_count=clan_power_up_count,
                            fallback="api_only_no_external_db",
                        )
                    else:
                        clan_power_up_gmt_value = None
                        clan_power_up_gmt_sentinel_value = None
                        log_warn(
                            "sync.round_clan_power_up_users_missing",
                            sheet=ctx.title,
                            league_id=ctx.league_id,
                            round_id=rid,
                            boost_count=clan_power_up_count,
                            counts_source=counts_source,
                            fallback="api_only_no_external_db",
                        )
            else:
                # Backward compatibility for stubs/tests that do not expose per-user cache.
                power_up_count = int(counts_by_name.get("Power Up Boost", 0) or 0)
                if power_up_count <= 0:
                    power_up_gmt_value = 0.0
                    power_up_gmt_sentinel_value = 0.0
                    power_up_missing_aliases = []
                else:
                    approx = calc_boost_gmt_from_api_round(rec, counts_by_name, "Power Up Boost")
                    power_up_gmt_value = approx
                    power_up_gmt_sentinel_value = approx
                    power_up_missing_aliases = []
                clan_power_up_count = int(counts_by_name.get("Clan Power Up Boost", 0) or 0)
                if clan_power_up_count <= 0:
                    clan_power_up_gmt_value = 0.0
                    clan_power_up_gmt_sentinel_value = 0.0
                else:
                    # Clan Power Up must be clan-based via clan/get-by-id users.
                    # Without round API user cache we cannot resolve exact clan membership/counts here.
                    clan_power_up_gmt_value = None
                    clan_power_up_gmt_sentinel_value = None

            tracked_user_blocks_mined = None
            fetch_tracked_user_blocks_mined = getattr(round_ability_api, "fetch_tracked_user_blocks_mined_for_round", None)
            if callable(fetch_tracked_user_blocks_mined):
                try:
                    calculated_at = _to_api_calculated_at(rec.get("ended_at") or rec.get("snapshot_ts"))
                    tracked_user_blocks_mined = safe_float(
                        fetch_tracked_user_blocks_mined(
                            ctx.league_id,
                            rid,
                            calculated_at,
                            progress_hook=lambda rid=rid: _progress_tick(rid),
                        )
                    )
                except Exception as e:
                    tracked_user_blocks_mined = None
                    log_warn(
                        "sync.round_tracked_user_blocks_mined_fetch_failed",
                        sheet=ctx.title,
                        league_id=ctx.league_id,
                        round_id=rid,
                        user_id=EXCLUDED_BOOST_USER_ID,
                        err=repr(e),
                    )
            if callable(get_tracked_user_blocks_mined):
                if tracked_user_blocks_mined is None:
                    try:
                        tracked_user_blocks_mined = safe_float(get_tracked_user_blocks_mined(rid))
                    except Exception as e:
                        tracked_user_blocks_mined = None
                        log_warn(
                            "sync.round_tracked_user_blocks_mined_cache_read_failed",
                            sheet=ctx.title,
                            league_id=ctx.league_id,
                            round_id=rid,
                            user_id=EXCLUDED_BOOST_USER_ID,
                            err=repr(e),
                        )

            try:
                stage = "build_row"
                row = build_canonical_row(
                    rec,
                    ability_headers,
                    counts_by_name,
                    price_cutover_round=price_cutover,
                    power_up_gmt_value=power_up_gmt_value,
                    power_up_gmt_sentinel_value=power_up_gmt_sentinel_value,
                    clan_power_up_gmt_value=clan_power_up_gmt_value,
                    clan_power_up_gmt_sentinel_value=clan_power_up_gmt_sentinel_value,
                    power_up_missing_aliases=power_up_missing_aliases,
                    excluded_user_boosts_audit=excluded_user_boosts_audit,
                    tracked_user_blocks_mined=tracked_user_blocks_mined,
                )
                stage = "checksum"
                checksum = row_checksum(row)
                finalized = 1 if rid <= (max_round - STABILIZATION_ROUNDS) else 0

                if mapped is None:
                    stage = "enqueue_append"
                    state.enqueue_op(ws_id, "append_round", rid, checksum, {"row": row, "finalized": finalized})
                    state.upsert_round_processing_state(
                        ctx.league_id,
                        rid,
                        metrics_status="ok",
                        abilities_status="ok",
                        sheet_status="pending",
                        last_error=None,
                        retry_after_ts=0.0,
                        attempt_count=0,
                    )
                    enqueued += 1
                    if DEBUG_VERBOSE:
                        log_debug(
                            "sync.round_enqueued_append",
                            sheet=ctx.title,
                            round_id=rid,
                            finalized=finalized,
                            checksum=checksum[:12],
                            row_preview=row[:12],
                        )
                    continue

                needs_update = (str(mapped.get("checksum")) != checksum) or (int(mapped.get("finalized") or 0) != finalized)
                if needs_update:
                    stage = "resolve_row_idx"
                    row_idx = safe_int(mapped.get("row_idx"))
                    if row_idx is None:
                        if DEBUG_VERBOSE:
                            log_debug("sync.round_skip_update_missing_row_idx", sheet=ctx.title, round_id=rid)
                        continue
                    stage = "enqueue_update"
                    state.enqueue_op(
                        ws_id,
                        "update_round",
                        rid,
                        checksum,
                        {"row_idx": row_idx, "row": row, "finalized": finalized},
                    )
                    state.upsert_round_processing_state(
                        ctx.league_id,
                        rid,
                        metrics_status="ok",
                        abilities_status="ok",
                        sheet_status="pending",
                        last_error=None,
                        retry_after_ts=0.0,
                        attempt_count=0,
                    )
                    enqueued += 1
                    if DEBUG_VERBOSE:
                        log_debug(
                            "sync.round_enqueued_update",
                            sheet=ctx.title,
                            round_id=rid,
                            row_idx=row_idx,
                            finalized=finalized,
                            old_checksum=str(mapped.get("checksum"))[:12],
                            new_checksum=checksum[:12],
                            row_preview=row[:12],
                        )
                elif DEBUG_VERBOSE:
                    log_debug("sync.round_skip_no_change", sheet=ctx.title, round_id=rid, row_idx=mapped.get("row_idx"), finalized=finalized)
            except Exception as e:
                log_warn(
                    "sync.round_process_failed",
                    sheet=ctx.title,
                    league_id=ctx.league_id,
                    round_id=rid,
                    stage=locals().get("stage", "unknown"),
                    err=repr(e),
                    traceback=traceback.format_exc()[:2000],
                )
                prev_attempt = int(rstate.get("attempt_count") or 0)
                next_attempt = min(ROUND_SOFT_FAIL_MAX_ATTEMPTS, prev_attempt + 1)
                delay_s = min(
                    ROUND_RETRY_BACKOFF_MAX_SECONDS,
                    ROUND_RETRY_BACKOFF_MIN_SECONDS * (2 ** max(0, next_attempt - 1)),
                )
                state.upsert_round_processing_state(
                    ctx.league_id,
                    rid,
                    metrics_status="ok",
                    abilities_status="ok",
                    sheet_status="failed",
                    last_error=repr(e),
                    retry_after_ts=time.time() + delay_s,
                    attempt_count=next_attempt,
                )
                continue

    return enqueued

def enqueue_clan_sheet_ops(
    db: Optional[DBClient],
    state: StateStore,
    clan_contexts: Dict[int, SheetContext],
    clan_api: GoMiningClanApiClient,
    flush_tick: Optional[Callable[[], int]] = None,
    flush_every_seconds: float = ENQUEUE_FLUSH_EVERY_SECONDS,
) -> int:
    enqueued = 0
    for ws_id, ctx in clan_contexts.items():
        last_flush_monotonic = time.monotonic() - max(0.0, flush_every_seconds)

        def _progress_tick(round_id: Optional[int]) -> None:
            nonlocal last_flush_monotonic
            last_flush_monotonic = _maybe_flush_during_enqueue(
                flush_tick,
                last_flush_monotonic,
                flush_every_seconds,
                scope="clan",
                sheet=ctx.title,
                round_id=round_id,
            )

        state.upsert_sheet_meta(ws_id, ctx.title, ctx.league_id, layout_sig=None)
        last_synced = state.get_last_synced_round(ws_id)

        if last_synced is None:
            cutover = fetch_latest_completed_round_id_prefer_api(
                db,
                state,
                ctx.league_id,
            )
            if cutover is None:
                cutover = 0
            state.set_last_synced_round(ws_id, cutover)
            log_info("sync.clan_init_cutover", sheet=ctx.title, league_id=ctx.league_id, cutover_round=cutover)
            continue

        rounds = fetch_completed_rounds_prefer_api(
            db,
            state,
            ctx.league_id,
            max(0, last_synced),
            MAX_ROUNDS_PER_POLL,
        )
        if DEBUG_VERBOSE:
            log_debug(
                "sync.clan_sheet_poll",
                sheet=ctx.title,
                ws_id=ws_id,
                league_id=ctx.league_id,
                last_synced=last_synced,
                fetched_rounds=len(rounds),
            )
        if not rounds:
            continue

        rounds_sorted = sorted(rounds, key=lambda r: safe_int(r.get("round_id")) or -1)
        round_ids = [safe_int(r.get("round_id")) for r in rounds_sorted]
        round_ids = [x for x in round_ids if x is not None]
        if not round_ids:
            continue

        row_maps = state.get_round_row_map_bulk(ws_id, round_ids)

        for rec in rounds_sorted:
            rid = safe_int(rec.get("round_id"))
            if rid is None or rid <= last_synced:
                continue
            _progress_tick(rid)
            latest_last_synced = state.get_last_synced_round(ws_id)
            if latest_last_synced is not None and latest_last_synced > last_synced:
                last_synced = latest_last_synced
            if rid <= last_synced:
                continue
            mapped = row_maps.get(rid)
            if mapped is None:
                fresh_map = state.get_round_row_map_bulk(ws_id, [rid]).get(rid)
                if fresh_map is not None:
                    mapped = fresh_map
                    row_maps[rid] = fresh_map
            if mapped is not None:
                if DEBUG_VERBOSE:
                    log_debug("sync.clan_round_skip_existing", sheet=ctx.title, round_id=rid, row_idx=mapped.get("row_idx"))
                continue

            round_ts = to_iso_utc(rec.get("snapshot_ts")) or to_iso_utc(rec.get("ended_at")) or utc_now_iso()
            calculated_at = _to_api_calculated_at(rec.get("ended_at") or rec.get("snapshot_ts"))
            clan_rows = clan_api.fetch_clan_rows_for_round(
                ctx.league_id,
                rid,
                calculated_at=calculated_at,
                snapshot_ts=round_ts,
            )
            if clan_rows is None:
                log_warn(
                    "sync.clan_round_api_incomplete",
                    sheet=ctx.title,
                    ws_id=ws_id,
                    round_id=rid,
                    league_id=ctx.league_id,
                    calculated_at=calculated_at,
                )
                # Block sequentially: do not enqueue this or newer rounds for this league.
                break

            if not clan_rows:
                log_warn(
                    "sync.clan_round_api_empty",
                    sheet=ctx.title,
                    ws_id=ws_id,
                    round_id=rid,
                    league_id=ctx.league_id,
                    calculated_at=calculated_at,
                )
                # Treat empty API data as incomplete to preserve strict round gating.
                break

            rows = build_clan_round_rows(rec, clan_rows)
            checksum = row_checksum(rows)  # type: ignore[arg-type]
            state.enqueue_op(
                ws_id,
                "append_clan_round",
                rid,
                checksum,
                {"rows": rows, "finalized": 1},
            )
            enqueued += 1
            if DEBUG_VERBOSE:
                log_debug(
                    "sync.clan_round_enqueued_append",
                    sheet=ctx.title,
                    round_id=rid,
                    rows=len(rows),
                    checksum=checksum[:12],
                    row_preview=rows[0][:8] if rows else [],
                )

    return enqueued

def _enqueue_specific_main_rounds(
    state: StateStore,
    ctx: SheetContext,
    records: Sequence[Dict[str, Any]],
    ability_id_to_name: Dict[str, str],
    ability_headers: List[str],
    round_ability_api: GoMiningRoundAbilityApiClient,
    read_limiter: Optional[TokenBucket],
    power_up_clan_api: Optional[GoMiningClanApiClient],
) -> int:
    if not records:
        return 0
    recs_sorted = sorted(
        [r for r in records if isinstance(r, dict) and safe_int(r.get("round_id")) is not None],
        key=lambda r: safe_int(r.get("round_id")) or -1,
    )
    if not recs_sorted:
        return 0
    orig_fetch = fetch_completed_rounds_prefer_api

    def _patched_fetch(
        db: Optional[DBClient],
        _state: StateStore,
        league_id: int,
        since_round: int,
        limit: int = MAX_ROUNDS_PER_POLL,
    ) -> List[Dict[str, Any]]:
        _ = (db, _state, since_round)
        if int(league_id) != int(ctx.league_id):
            return []
        out: List[Dict[str, Any]] = []
        for rec in recs_sorted:
            rid = safe_int(rec.get("round_id"))
            if rid is None:
                continue
            out.append(rec)
            if len(out) >= max(1, int(limit)):
                break
        return out

    globals()["fetch_completed_rounds_prefer_api"] = _patched_fetch  # type: ignore[assignment]
    try:
        return enqueue_main_sheet_ops(
            None,
            state,
            {ctx.ws_id: ctx},
            ability_id_to_name,
            ability_headers,
            round_ability_api,
            read_limiter,
            power_up_clan_api=power_up_clan_api,
            flush_tick=None,
            flush_every_seconds=ENQUEUE_FLUSH_EVERY_SECONDS,
        )
    finally:
        globals()["fetch_completed_rounds_prefer_api"] = orig_fetch  # type: ignore[assignment]
