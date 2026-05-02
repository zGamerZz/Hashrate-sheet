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

from ..api.auth import fetch_bearer_token_from_auth_api
from ..api.clan import GoMiningClanApiClient
from ..api.league_index import fetch_league_catalog_from_api
from ..api.round_ability import GoMiningRoundAbilityApiClient
from ..api.round_metrics import GoMiningRoundMetricsApiClient
from ..config import *
from ..domain.abilities import build_ability_id_to_header
from ..logging_utils import log_debug, log_info, log_warn
from ..runtime.lock import SingleInstanceLock
from ..runtime.rate_limit import AdaptiveRateController, TokenBucket
from ..sheets.context import open_spreadsheet
from ..sheets.layout import refresh_sheet_contexts
from ..sheets.queue import flush_sheet_queue_with_rate_limit, purge_stale_queue_ops
from ..storage.legacy_db import _warn_db_deprecated
from ..storage.state import StateStore
from .cache import sync_api_round_cache
from .enqueue import enqueue_clan_sheet_ops, enqueue_main_sheet_ops
from .reconcile import log_main_sheet_summaries, run_reconcile_pass

def main(once_reconcile: bool = False) -> None:
    lock = SingleInstanceLock(LOCK_FILE_PATH)
    lock.acquire()
    state: Optional[StateStore] = None
    try:
        ability_id_to_name = build_ability_id_to_header(ABILITY_DIM_STATIC)
        ability_headers = list(ABILITY_HEADER_ORDER)
        if not ability_id_to_name:
            raise RuntimeError("No matching boost abilities found in static ability mapping for configured fixed header set.")
        log_info("sync.ability_mapping_loaded", mapped_ids=len(ability_id_to_name), columns=len(ability_headers))

        main_expected_header = BASE_HEADERS + ability_headers + [
            POWER_UP_PRICE_HEADER,
            POWER_UP_PRICE_SENTINEL_HEADER,
            CLAN_POWER_UP_PRICE_HEADER,
            CLAN_POWER_UP_PRICE_SENTINEL_HEADER,
            MISSING_HEADER,
            EXCLUDED_USER_BOOST_AUDIT_HEADER,
            BTC_FUND_HEADER,
            TRACKED_USER_BLOCKS_MINED_HEADER,
        ]
        clan_expected_header = list(CLAN_HEADERS)

        state = StateStore(STATE_DB_PATH)
        write_limiter = TokenBucket(GS_WRITE_REQ_PER_MIN, name="sheets_write")
        read_limiter = TokenBucket(GS_READ_REQ_PER_MIN, name="sheets_read")
        gomining_limiter = TokenBucket(GOMINING_API_REQ_PER_MIN, name="gomining_api")
        rate_controller = AdaptiveRateController(gomining_limiter, ADAPTIVE_RPM_MIN, ADAPTIVE_RPM_MAX)
        if ENABLE_DB_FALLBACK_RAW:
            _warn_db_deprecated("ENABLE_DB_FALLBACK env")
        token_fetcher: Optional[Callable[[], Optional[str]]] = None
        effective_bearer = GOMINING_BEARER_TOKEN.strip()
        if TOKEN_URL and TOKEN_X_AUTH:
            token_fetcher = fetch_bearer_token_from_auth_api
            fetched = (token_fetcher() or "").strip()
            if fetched:
                effective_bearer = fetched
            elif not effective_bearer:
                raise RuntimeError("Failed to fetch bearer token from TOKEN_URL and no GOMINING_BEARER_TOKEN fallback is set.")
            else:
                log_warn("token_api.startup_failed_using_fallback", token_url=TOKEN_URL)
        if not effective_bearer:
            raise RuntimeError("GOMINING_BEARER_TOKEN is required when TOKEN_URL/TOKEN_X_AUTH is not configured.")
        power_up_clan_api: Optional[GoMiningClanApiClient] = GoMiningClanApiClient(
            effective_bearer,
            limiter=gomining_limiter,
            leaderboard_url=CLAN_LEADERBOARD_API_URL,
            clan_get_by_id_url=CLAN_GET_BY_ID_API_URL,
            page_limit=CLAN_API_PAGE_LIMIT,
            timeout_seconds=CLAN_API_TIMEOUT_SECONDS,
            max_retries=CLAN_API_MAX_RETRIES,
            token_fetcher=token_fetcher,
            rate_controller=rate_controller,
        )
        clan_api: Optional[GoMiningClanApiClient] = power_up_clan_api if ENABLE_CLAN_SYNC else None
        round_metrics_api = GoMiningRoundMetricsApiClient(
            effective_bearer,
            limiter=gomining_limiter,
            base_url=GOMINING_API_BASE_URL,
            multiplier_path=MULTIPLIER_PATH,
            round_clan_leaderboard_path=ROUND_CLAN_LEADERBOARD_PATH,
            player_leaderboard_path=PLAYER_LEADERBOARD_PATH,
            clan_leaderboard_path=ROUND_METRICS_CLAN_PATH,
            user_page_limit=ROUND_METRICS_USER_PAGE_LIMIT,
            clan_page_limit=ROUND_METRICS_CLAN_PAGE_LIMIT,
            round_scan_lookback=ROUND_GET_LAST_SCAN_LOOKBACK,
            timeout_seconds=ROUND_METRICS_TIMEOUT_SECONDS,
            max_retries=ROUND_METRICS_MAX_RETRIES,
            token_fetcher=token_fetcher,
            rate_controller=rate_controller,
        )
        round_ability_api = GoMiningRoundAbilityApiClient(
            effective_bearer,
            limiter=gomining_limiter,
            user_leaderboard_url=ROUND_USER_LEADERBOARD_API_URL,
            page_limit=ROUND_API_PAGE_LIMIT,
            timeout_seconds=ROUND_API_TIMEOUT_SECONDS,
            max_retries=ROUND_API_MAX_RETRIES,
            token_fetcher=token_fetcher,
            rate_controller=rate_controller,
        )

        sh = open_spreadsheet()
        main_contexts, clan_contexts = refresh_sheet_contexts(
            sh,
            state,
            main_expected_header,
            clan_expected_header,
            write_limiter,
            read_limiter,
            enable_clan_sync=ENABLE_CLAN_SYNC,
        )
        contexts_all = {**main_contexts, **clan_contexts}
        purge_stale_queue_ops(state, contexts_all)
        if not ENABLE_CLAN_SYNC:
            purged_clan_ops = state.purge_ops_by_type(["append_clan_round"])
            if purged_clan_ops > 0:
                log_info("queue.purged_clan_ops", deleted_ops=purged_clan_ops)
        configured_league_ids = sorted({ctx.league_id for ctx in main_contexts.values()} | {ctx.league_id for ctx in clan_contexts.values()})
        cached_written, cached_total = sync_api_round_cache(state, round_metrics_api, configured_league_ids)
        if cached_total > 0:
            log_info("sync.api_round_cache_seeded", leagues=len(configured_league_ids), seen=cached_total, written=cached_written)
        api_leagues = fetch_league_catalog_from_api(round_metrics_api.bearer_token)

        log_info(
            "sync.spreadsheet_opened",
            title=sh.title,
            active_main_tabs=len(main_contexts),
            active_clan_tabs=len(clan_contexts),
        )
        log_info(
            "sync.config",
            poll_s=SYNC_POLL_SECONDS,
            refresh_s=SHEET_REFRESH_SECONDS,
            write_limit_per_min=GS_WRITE_REQ_PER_MIN,
            read_limit_per_min=GS_READ_REQ_PER_MIN,
            missing_sheet_retries=MAX_MISSING_SHEET_RETRIES,
            auto_expand_rows=AUTO_EXPAND_SHEET_ROWS,
            drop_non_retryable=DROP_NON_RETRYABLE_SHEET_ERRORS,
            purge_missing_sheet_ops=PURGE_QUEUE_FOR_MISSING_SHEETS,
            sync_api_gap_fill_max=SYNC_API_GAP_FILL_MAX,
            sync_api_gap_fill_probe_max=SYNC_API_GAP_FILL_PROBE_MAX,
            gomining_api_req_per_min=GOMINING_API_REQ_PER_MIN,
            clan_sync_enabled=ENABLE_CLAN_SYNC,
            clan_api_page_limit=CLAN_API_PAGE_LIMIT,
            clan_api_timeout_s=CLAN_API_TIMEOUT_SECONDS,
            clan_api_max_retries=CLAN_API_MAX_RETRIES,
            gomining_api_base_url=GOMINING_API_BASE_URL,
            round_multiplier_path=MULTIPLIER_PATH,
            round_clan_leaderboard_path=ROUND_CLAN_LEADERBOARD_PATH,
            round_player_leaderboard_path=PLAYER_LEADERBOARD_PATH,
            round_metrics_clan_path=ROUND_METRICS_CLAN_PATH,
            round_metrics_timeout_s=ROUND_METRICS_TIMEOUT_SECONDS,
            round_metrics_max_retries=ROUND_METRICS_MAX_RETRIES,
            round_metrics_user_page_limit=ROUND_METRICS_USER_PAGE_LIMIT,
            round_metrics_clan_page_limit=ROUND_METRICS_CLAN_PAGE_LIMIT,
            round_get_last_scan_lookback=ROUND_GET_LAST_SCAN_LOOKBACK,
            round_close_on_rollover=ROUND_CLOSE_ON_ROLLOVER,
            round_get_last_strict_league_validation=ROUND_GET_LAST_STRICT_LEAGUE_VALIDATION,
            round_user_leaderboard_url=ROUND_USER_LEADERBOARD_API_URL,
            round_api_page_limit=ROUND_API_PAGE_LIMIT,
            round_api_timeout_s=ROUND_API_TIMEOUT_SECONDS,
            round_api_max_retries=ROUND_API_MAX_RETRIES,
            leagues_api_poll_s=LEAGUES_API_POLL_SECONDS,
            league_index_lookback_days=LEAGUE_INDEX_LOOKBACK_DAYS,
            league_index_try_without_calculated_at=LEAGUE_INDEX_TRY_WITHOUT_CALCULATED_AT,
            token_url_set=bool(TOKEN_URL),
            token_method=(TOKEN_METHOD if TOKEN_METHOD in {"GET", "POST"} else "GET"),
            token_verify_ssl=TOKEN_VERIFY_SSL,
            api_only_mode=True,
            enqueue_flush_every_s=ENQUEUE_FLUSH_EVERY_SECONDS,
            reconcile_interval_s=RECONCILE_INTERVAL_SECONDS,
            round_soft_fail_max_attempts=ROUND_SOFT_FAIL_MAX_ATTEMPTS,
            round_retry_backoff_min_s=ROUND_RETRY_BACKOFF_MIN_SECONDS,
            round_retry_backoff_max_s=ROUND_RETRY_BACKOFF_MAX_SECONDS,
            gap_scan_lookback_rounds=GAP_SCAN_LOOKBACK_ROUNDS,
            reconcile_deep_missing_per_pass=RECONCILE_DEEP_MISSING_PER_PASS,
            adaptive_rpm_enabled=ADAPTIVE_RPM_ENABLE,
            adaptive_rpm_min=ADAPTIVE_RPM_MIN,
            adaptive_rpm_max=ADAPTIVE_RPM_MAX,
            once_reconcile=bool(once_reconcile),
            dry_run=DRY_RUN,
            log_level=LOG_LEVEL,
            debug_verbose=DEBUG_VERBOSE,
        )
        if api_leagues:
            configured = {ctx.league_id for ctx in main_contexts.values()}
            api_set = set(api_leagues.keys())
            missing_in_sheets = sorted(api_set - configured)
            unknown_in_sheets = sorted(configured - api_set)
            log_info(
                "sync.league_catalog",
                api=len(api_set),
                configured=len(configured),
                missing_in_sheets=len(missing_in_sheets),
                unknown_in_sheets=len(unknown_in_sheets),
            )
            if missing_in_sheets:
                log_info("sync.leagues_missing_in_sheets", league_ids=missing_in_sheets)
            if unknown_in_sheets:
                log_info("sync.sheet_leagues_not_in_api_catalog", league_ids=unknown_in_sheets)

        last_refresh = time.time()
        last_poll = 0.0
        last_league_api_poll = 0.0
        last_reconcile_poll = 0.0
        last_heartbeat = 0.0

        if once_reconcile:
            rec_stats = run_reconcile_pass(
                state=state,
                main_contexts=main_contexts,
                ability_id_to_name=ability_id_to_name,
                ability_headers=ability_headers,
                round_ability_api=round_ability_api,
                read_limiter=read_limiter,
                power_up_clan_api=power_up_clan_api,
            )
            flushed_total = 0
            while True:
                done = flush_sheet_queue_with_rate_limit(state, contexts_all, write_limiter)
                if done <= 0:
                    break
                flushed_total += int(done)
            log_info("sync.once_reconcile_done", stats=rec_stats, flushed=flushed_total, queue_total=state.queue_total_count())
            return

        while True:
            now = time.time()

            if now - last_refresh >= SHEET_REFRESH_SECONDS:
                sh = open_spreadsheet()
                main_contexts, clan_contexts = refresh_sheet_contexts(
                    sh,
                    state,
                    main_expected_header,
                    clan_expected_header,
                    write_limiter,
                    read_limiter,
                    enable_clan_sync=ENABLE_CLAN_SYNC,
                )
                contexts_all = {**main_contexts, **clan_contexts}
                purge_stale_queue_ops(state, contexts_all)
                if not ENABLE_CLAN_SYNC:
                    purged_clan_ops = state.purge_ops_by_type(["append_clan_round"])
                    if purged_clan_ops > 0:
                        log_info("queue.purged_clan_ops", deleted_ops=purged_clan_ops)
                configured_league_ids = sorted(
                    {ctx.league_id for ctx in main_contexts.values()} | {ctx.league_id for ctx in clan_contexts.values()}
                )
                last_refresh = now
                log_info("sync.sheet_refresh_done", active_main=len(main_contexts), active_clan=len(clan_contexts))

            if now - last_league_api_poll >= LEAGUES_API_POLL_SECONDS:
                cached_written, cached_total = sync_api_round_cache(state, round_metrics_api, configured_league_ids)
                if cached_total > 0 and DEBUG_VERBOSE:
                    log_debug(
                        "sync.api_round_cache_poll",
                        leagues=len(configured_league_ids),
                        seen=cached_total,
                        written=cached_written,
                    )
                api_leagues = fetch_league_catalog_from_api(round_metrics_api.bearer_token)
                last_league_api_poll = now
                if api_leagues:
                    configured = {ctx.league_id for ctx in main_contexts.values()}
                    api_set = set(api_leagues.keys())
                    missing_in_sheets = sorted(api_set - configured)
                    unknown_in_sheets = sorted(configured - api_set)
                    log_info(
                        "sync.league_catalog",
                        api=len(api_set),
                        configured=len(configured),
                        missing_in_sheets=len(missing_in_sheets),
                        unknown_in_sheets=len(unknown_in_sheets),
                    )

            if now - last_poll >= SYNC_POLL_SECONDS:
                cached_written, cached_total = sync_api_round_cache(state, round_metrics_api, configured_league_ids)
                if cached_total > 0 and DEBUG_VERBOSE:
                    log_debug(
                        "sync.api_round_cache_poll",
                        leagues=len(configured_league_ids),
                        seen=cached_total,
                        written=cached_written,
                    )

                def _flush_due_queue_tick() -> int:
                    return flush_sheet_queue_with_rate_limit(state, contexts_all, write_limiter)

                enq_main = enqueue_main_sheet_ops(
                    None,
                    state,
                    main_contexts,
                    ability_id_to_name,
                    ability_headers,
                    round_ability_api,
                    read_limiter,
                    power_up_clan_api=power_up_clan_api,
                    flush_tick=_flush_due_queue_tick,
                    flush_every_seconds=ENQUEUE_FLUSH_EVERY_SECONDS,
                )
                enq_clan = 0
                if ENABLE_CLAN_SYNC and clan_api is not None:
                    enq_clan = enqueue_clan_sheet_ops(
                        None,
                        state,
                        clan_contexts,
                        clan_api,
                        flush_tick=_flush_due_queue_tick,
                        flush_every_seconds=ENQUEUE_FLUSH_EVERY_SECONDS,
                    )
                enq = enq_main + enq_clan
                last_poll = now
                if enq > 0:
                    log_info(
                        "queue.enqueued",
                        count=enq,
                        main=enq_main,
                        clan=enq_clan,
                        queue_total=state.queue_total_count(),
                        queue_due=state.queue_due_count(),
                    )

            if now - last_reconcile_poll >= RECONCILE_INTERVAL_SECONDS:
                rec_stats = run_reconcile_pass(
                    state=state,
                    main_contexts=main_contexts,
                    ability_id_to_name=ability_id_to_name,
                    ability_headers=ability_headers,
                    round_ability_api=round_ability_api,
                    read_limiter=read_limiter,
                    power_up_clan_api=power_up_clan_api,
                )
                last_reconcile_poll = now
                if rec_stats.get("queued_ops", 0) > 0 or DEBUG_VERBOSE:
                    log_info("sync.reconcile_done", **rec_stats, queue_total=state.queue_total_count(), queue_due=state.queue_due_count())

            done = flush_sheet_queue_with_rate_limit(state, contexts_all, write_limiter)
            if done > 0:
                log_info("queue.flushed", processed=done)

            if now - last_heartbeat >= HEARTBEAT_SECONDS:
                total_q = state.queue_total_count()
                due_q = state.queue_due_count()
                log_info(
                    "sync.heartbeat",
                    queue_total=total_q,
                    queue_due=due_q,
                    main_contexts=len(main_contexts),
                    clan_contexts=len(clan_contexts),
                    write_limiter=write_limiter.snapshot(),
                    read_limiter=read_limiter.snapshot(),
                    gomining_limiter=gomining_limiter.snapshot(),
                )
                log_main_sheet_summaries(state, main_contexts)
                last_heartbeat = now

            time.sleep(1.0)

    finally:
        try:
            if state is not None:
                state.close()
        except Exception:
            pass
        lock.release()
