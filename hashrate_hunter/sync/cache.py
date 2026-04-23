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

from ..api.round_metrics import GoMiningRoundMetricsApiClient
from ..config import *
from ..logging_utils import log_info, log_warn
from ..storage.legacy_db import DBClient, _warn_db_deprecated
from ..storage.state import StateStore
from ..utils import safe_int, to_iso_utc, utc_now_iso

def sync_api_round_cache(
    state: StateStore,
    round_metrics_api: GoMiningRoundMetricsApiClient,
    league_ids: Sequence[int],
) -> Tuple[int, int]:
    wanted = sorted({int(x) for x in league_ids if safe_int(x) is not None})
    if not wanted:
        return 0, 0

    records: List[Dict[str, Any]] = []
    failed = 0
    for league_id in wanted:
        prev = state.fetch_latest_api_round_record(league_id)
        prev_round_id: Optional[int] = None
        prev_ended_at: Optional[str] = None
        if isinstance(prev, dict):
            prev_round_id = safe_int(prev.get("round_id"))
            prev_ended_at = to_iso_utc(prev.get("ended_at"))

        rec = round_metrics_api.fetch_round_metrics_triplet(
            league_id,
            strict_league_validation=ROUND_GET_LAST_STRICT_LEAGUE_VALIDATION,
        )
        if not isinstance(rec, dict):
            failed += 1
            log_warn("sync.api_round_cache_league_skip", league_id=league_id, reason="round_metrics_unavailable")
            continue

        round_id = safe_int(rec.get("round_id"))
        if round_id is None:
            failed += 1
            log_warn("sync.api_round_cache_league_skip", league_id=league_id, reason="missing_round_id")
            continue

        league_records: List[Dict[str, Any]] = []
        if prev_round_id is not None and round_id > (prev_round_id + 1):
            missing_total = round_id - prev_round_id - 1
            attempted = min(missing_total, SYNC_API_GAP_FILL_MAX)
            fetched = 0
            probes = 0
            # Round IDs are global and sparse per league. Probe backwards from the latest
            # round to capture recent same-league rounds instead of assuming contiguity.
            probe_budget = min(
                missing_total,
                max(attempted, SYNC_API_GAP_FILL_PROBE_MAX),
            )
            fetch_by_round_id = getattr(round_metrics_api, "fetch_round_metrics_by_round_id", None)

            if callable(fetch_by_round_id):
                candidate_round_id = round_id - 1
                while (
                    candidate_round_id > prev_round_id
                    and fetched < attempted
                    and probes < probe_budget
                ):
                    gap_round_id = candidate_round_id
                    candidate_round_id -= 1
                    probes += 1
                    gap_rec = fetch_by_round_id(league_id, gap_round_id)
                    if not isinstance(gap_rec, dict):
                        log_warn(
                            "sync.api_round_gap_fill_round_skip",
                            league_id=league_id,
                            round_id=gap_round_id,
                            reason="round_metrics_unavailable",
                        )
                        continue
                    gap_round_id_resolved = safe_int(gap_rec.get("round_id"))
                    if gap_round_id_resolved != gap_round_id:
                        log_warn(
                            "sync.api_round_gap_fill_round_skip",
                            league_id=league_id,
                            round_id=gap_round_id,
                            reason="round_id_mismatch",
                            payload_round_id=gap_round_id_resolved,
                        )
                        continue
                    league_records.append(gap_rec)
                    fetched += 1
                league_records.sort(key=lambda x: safe_int(x.get("round_id")) or -1)
            else:
                log_warn(
                    "sync.api_round_gap_fill_round_skip",
                    league_id=league_id,
                    round_id=prev_round_id + 1,
                    reason="client_missing_method",
                )

            capped = missing_total > fetched
            probe_capped = bool(attempted > fetched and probes >= probe_budget and probe_budget > 0)
            remaining_missing = max(0, missing_total - fetched)
            log_info(
                "sync.api_round_gap_fill",
                league_id=league_id,
                prev_round_id=prev_round_id,
                new_round_id=round_id,
                missing_total=missing_total,
                attempted=attempted,
                probe_budget=probe_budget,
                probes=probes,
                fetched=fetched,
                capped=capped,
                probe_capped=probe_capped,
                remaining=remaining_missing,
            )
            league_records.append(rec)
        else:
            league_records.append(rec)

        if ROUND_CLOSE_ON_ROLLOVER and prev_round_id is not None and not prev_ended_at and league_records:
            next_round_id = safe_int(league_records[0].get("round_id"))
            if next_round_id is not None and next_round_id > prev_round_id:
                ended_at_synth = to_iso_utc(league_records[0].get("snapshot_ts")) or utc_now_iso()
                marked = state.mark_api_round_ended(league_id, prev_round_id, ended_at_synth)
                if marked > 0:
                    log_info(
                        "sync.api_round_rollover_closed",
                        league_id=league_id,
                        prev_round_id=prev_round_id,
                        next_round_id=next_round_id,
                        ended_at=ended_at_synth,
                    )

        for league_rec in league_records:
            league_round_id = safe_int(league_rec.get("round_id"))
            if league_round_id is None:
                continue
            records.append(league_rec)
            state.upsert_round_processing_state(
                league_id,
                league_round_id,
                metrics_status="ok",
                abilities_status="pending",
                sheet_status="pending",
                retry_after_ts=0.0,
            )

    if not records:
        log_warn("sync.api_round_cache_fetch_failed", leagues=len(wanted), failed=failed)
        return 0, 0

    written = state.upsert_api_round_records(records, source="api_triplet")
    return written, len(records)

def fetch_completed_rounds_prefer_api(
    db: Optional[DBClient],
    state: StateStore,
    league_id: int,
    since_round: int,
    limit: int = MAX_ROUNDS_PER_POLL,
) -> List[Dict[str, Any]]:
    if db is not None:
        _warn_db_deprecated("fetch_completed_rounds_prefer_api")
    return state.fetch_api_completed_rounds(league_id, since_round, limit)

def fetch_latest_completed_round_id_prefer_api(
    db: Optional[DBClient],
    state: StateStore,
    league_id: int,
) -> Optional[int]:
    if db is not None:
        _warn_db_deprecated("fetch_latest_completed_round_id_prefer_api")
    api_latest = state.fetch_latest_api_completed_round_id(league_id)
    return api_latest
