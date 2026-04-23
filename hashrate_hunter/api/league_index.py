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
from ..logging_utils import log_debug, log_warn
from ..utils import safe_float, safe_int, to_iso_utc, utc_now_iso

def _extract_league_index_array(payload: Dict[str, Any]) -> List[Any]:
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    candidates: List[Any] = []
    if isinstance(data, dict):
        candidates.extend(
            [
                data.get("array"),
                data.get("leagues"),
                data.get("items"),
                data.get("list"),
            ]
        )
    candidates.extend(
        [
            payload.get("array"),
            payload.get("leagues"),
            payload.get("items"),
            payload.get("list"),
        ]
    )
    for arr in candidates:
        if isinstance(arr, list):
            return arr
    return []

def parse_league_index_response(payload: Dict[str, Any]) -> Dict[int, str]:
    arr = _extract_league_index_array(payload or {})
    out: Dict[int, str] = {}
    for item in arr:
        if not isinstance(item, dict):
            continue
        league_id = safe_int(item.get("id"))
        league_name = str(item.get("name") or "").strip()
        if league_id is None:
            continue
        out[league_id] = league_name or f"league-{league_id}"
    return out

def _first_path_value(obj: Any, paths: Sequence[Sequence[str]]) -> Any:
    for path in paths:
        cur = obj
        ok = True
        for key in path:
            if not isinstance(cur, dict) or key not in cur:
                ok = False
                break
            cur = cur.get(key)
        if ok and cur is not None:
            return cur
    return None

def parse_league_round_records_from_index(payload: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    arr = _extract_league_index_array(payload or {})
    out: Dict[int, Dict[str, Any]] = {}

    now_iso = utc_now_iso()
    for item in arr:
        if not isinstance(item, dict):
            continue
        league_id = safe_int(item.get("id"))
        if league_id is None:
            league_id = safe_int(_first_path_value(item, [("league", "id"), ("data", "leagueId")]))
        if league_id is None:
            continue

        round_id = safe_int(
            _first_path_value(
                item,
                [
                    ("roundId",),
                    ("currentRoundId",),
                    ("round", "id"),
                    ("currentRound", "id"),
                    ("currentRound", "roundId"),
                    ("multiplierSnapshot", "roundId"),
                    ("snapshot", "roundId"),
                    ("data", "roundId"),
                ],
            )
        )
        if round_id is None:
            continue

        active_raw = _first_path_value(
            item,
            [
                ("active",),
                ("isActive",),
                ("round", "active"),
                ("round", "isActive"),
                ("currentRound", "active"),
                ("currentRound", "isActive"),
                ("multiplierSnapshot", "active"),
                ("multiplierSnapshot", "isActive"),
                ("data", "active"),
                ("data", "isActive"),
            ],
        )
        is_active: Optional[bool]
        if isinstance(active_raw, bool):
            is_active = active_raw
        elif isinstance(active_raw, (int, float)):
            is_active = bool(int(active_raw))
        elif isinstance(active_raw, str) and active_raw.strip().lower() in {"0", "1", "true", "false"}:
            is_active = active_raw.strip().lower() in {"1", "true"}
        else:
            is_active = None

        ended_at = to_iso_utc(
            _first_path_value(
                item,
                [
                    ("endedAt",),
                    ("roundEndedAt",),
                    ("round", "endedAt"),
                    ("currentRound", "endedAt"),
                    ("multiplierSnapshot", "endedAt"),
                    ("data", "endedAt"),
                ],
            )
        )
        if ended_at is None and is_active is False:
            # Some payload variants omit endedAt for already closed rounds.
            ended_at = now_iso

        rec: Dict[str, Any] = {
            "snapshot_ts": to_iso_utc(
                _first_path_value(item, [("calculatedAt",), ("updatedAt",), ("snapshotAt",), ("data", "calculatedAt")])
            )
            or now_iso,
            "league_id": league_id,
            "round_id": round_id,
            "block_number": safe_int(
                _first_path_value(
                    item,
                    [
                        ("blockNumber",),
                        ("currentBlockNumber",),
                        ("round", "blockNumber"),
                        ("currentRound", "blockNumber"),
                        ("multiplierSnapshot", "blockNumber"),
                        ("data", "blockNumber"),
                    ],
                )
            ),
            "multiplier": safe_float(
                _first_path_value(
                    item,
                    [
                        ("multiplier",),
                        ("round", "multiplier"),
                        ("currentRound", "multiplier"),
                        ("multiplierSnapshot", "multiplier"),
                        ("data", "multiplier"),
                    ],
                )
            ),
            "gmt_fund": safe_float(
                _first_path_value(
                    item,
                    [
                        ("gmtFund",),
                        ("round", "gmtFund"),
                        ("currentRound", "gmtFund"),
                        ("multiplierSnapshot", "gmtFund"),
                        ("data", "gmtFund"),
                    ],
                )
            ),
            "gmt_per_block": safe_float(
                _first_path_value(
                    item,
                    [
                        ("gmtPerBlock",),
                        ("round", "gmtPerBlock"),
                        ("currentRound", "gmtPerBlock"),
                        ("multiplierSnapshot", "gmtPerBlock"),
                        ("data", "gmtPerBlock"),
                    ],
                )
            ),
            "league_th": safe_float(
                _first_path_value(
                    item,
                    [
                        ("totalPower",),
                        ("leagueTh",),
                        ("round", "totalPower"),
                        ("currentRound", "totalPower"),
                        ("multiplierSnapshot", "totalPower"),
                        ("data", "totalPower"),
                    ],
                )
            ),
            "blocks_mined": safe_int(
                _first_path_value(
                    item,
                    [
                        ("blocksMined",),
                        ("round", "blocksMined"),
                        ("currentRound", "blocksMined"),
                        ("multiplierSnapshot", "blocksMined"),
                        ("data", "blocksMined"),
                    ],
                )
            ),
            "efficiency_league": safe_float(
                _first_path_value(
                    item,
                    [
                        ("efficiencyLeague",),
                        ("effciencyLeague",),
                        ("round", "efficiencyLeague"),
                        ("round", "effciencyLeague"),
                        ("currentRound", "efficiencyLeague"),
                        ("currentRound", "effciencyLeague"),
                        ("multiplierSnapshot", "efficiencyLeague"),
                        ("multiplierSnapshot", "effciencyLeague"),
                        ("data", "efficiencyLeague"),
                        ("data", "effciencyLeague"),
                    ],
                )
            ),
            "ended_at": ended_at,
            "round_duration_sec": safe_int(
                _first_path_value(
                    item,
                    [
                        ("roundTime",),
                        ("roundDurationSec",),
                        ("round", "roundTime"),
                        ("round", "roundDurationSec"),
                        ("currentRound", "roundTime"),
                        ("currentRound", "roundDurationSec"),
                        ("multiplierSnapshot", "roundTime"),
                        ("multiplierSnapshot", "roundDurationSec"),
                        ("data", "roundTime"),
                        ("data", "roundDurationSec"),
                    ],
                )
            ),
        }
        out[league_id] = rec
    return out

def _league_index_calculated_at_candidates(now_utc: Optional[datetime] = None) -> List[Optional[str]]:
    now_dt = now_utc or datetime.now(timezone.utc)
    candidates: List[Optional[str]] = [
        now_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
    ]
    for day_shift in range(0, LEAGUE_INDEX_LOOKBACK_DAYS + 1):
        dt = now_dt.astimezone(timezone.utc) - timedelta(days=day_shift)
        candidates.append(dt.strftime("%Y-%m-%dT00:00:00.000Z"))
    if LEAGUE_INDEX_TRY_WITHOUT_CALCULATED_AT:
        candidates.append(None)

    deduped: List[Optional[str]] = []
    seen: set[Optional[str]] = set()
    for item in candidates:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped

def fetch_league_index_payload_from_api(
    bearer_token: Optional[str] = None,
    *,
    require_round_records: bool = False,
) -> Optional[Dict[str, Any]]:
    token = str(bearer_token or GOMINING_BEARER_TOKEN).strip()
    if not token:
        return None
    headers = {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "authorization": f"Bearer {token}",
        "x-device-type": "desktop",
    }
    candidates = _league_index_calculated_at_candidates()
    empty_body: Optional[Dict[str, Any]] = None
    catalog_only_body: Optional[Dict[str, Any]] = None
    catalog_only_attempts = 0
    last_err: Optional[str] = None

    for attempt, calculated_at in enumerate(candidates, start=1):
        payload = {} if calculated_at is None else {"calculatedAt": calculated_at}
        try:
            resp = requests.post(LEAGUES_API_URL, headers=headers, json=payload, timeout=20)
        except Exception as e:
            last_err = repr(e)
            log_warn(
                "league_api.request_failed",
                attempt=attempt,
                calculated_at=(calculated_at or "<none>"),
                err=repr(e),
            )
            continue

        if resp.status_code != 200:
            log_warn(
                "league_api.http_error",
                attempt=attempt,
                calculated_at=(calculated_at or "<none>"),
                status=resp.status_code,
                body_preview=resp.text[:180],
            )
            if resp.status_code in {401, 403}:
                return None
            continue

        try:
            body = resp.json()
        except Exception as e:
            last_err = repr(e)
            log_warn(
                "league_api.bad_json",
                attempt=attempt,
                calculated_at=(calculated_at or "<none>"),
                err=repr(e),
                body_preview=resp.text[:180],
            )
            continue
        if not isinstance(body, dict):
            log_warn(
                "league_api.bad_shape",
                attempt=attempt,
                calculated_at=(calculated_at or "<none>"),
                typ=type(body).__name__,
            )
            continue

        parsed = parse_league_index_response(body)
        if parsed:
            if require_round_records:
                recs = parse_league_round_records_from_index(body)
                if recs:
                    log_debug(
                        "league_api.ok_rounds",
                        leagues=len(parsed),
                        rounds=len(recs),
                        attempt=attempt,
                        calculated_at=(calculated_at or "<none>"),
                    )
                    return body
                catalog_only_attempts += 1
                if catalog_only_body is None:
                    catalog_only_body = body
                log_warn(
                    "league_api.catalog_without_rounds",
                    leagues=len(parsed),
                    attempt=attempt,
                    calculated_at=(calculated_at or "<none>"),
                )
                continue
            log_debug(
                "league_api.ok",
                leagues=len(parsed),
                attempt=attempt,
                calculated_at=(calculated_at or "<none>"),
            )
            return body

        if empty_body is None:
            empty_body = body
        log_debug(
            "league_api.empty",
            attempt=attempt,
            calculated_at=(calculated_at or "<none>"),
        )

    if require_round_records and catalog_only_body is not None:
        preview = [(x or "<none>") for x in candidates[:8]]
        log_warn(
            "league_api.rounds_missing_all_candidates",
            attempts=len(candidates),
            catalog_only_attempts=catalog_only_attempts,
            lookback_days=LEAGUE_INDEX_LOOKBACK_DAYS,
            try_without_calculated_at=LEAGUE_INDEX_TRY_WITHOUT_CALCULATED_AT,
            calculated_at_preview=preview,
        )
        return None

    if empty_body is not None:
        preview = [(x or "<none>") for x in candidates[:8]]
        log_warn(
            "league_api.empty_all_candidates",
            attempts=len(candidates),
            lookback_days=LEAGUE_INDEX_LOOKBACK_DAYS,
            try_without_calculated_at=LEAGUE_INDEX_TRY_WITHOUT_CALCULATED_AT,
            calculated_at_preview=preview,
        )
        return empty_body

    if last_err is not None:
        log_warn(
            "league_api.failed_all_candidates",
            attempts=len(candidates),
            err=last_err,
        )
    return None

def fetch_league_catalog_from_api(bearer_token: Optional[str] = None) -> Dict[int, str]:
    payload = fetch_league_index_payload_from_api(bearer_token)
    if not isinstance(payload, dict):
        return {}
    parsed = parse_league_index_response(payload)
    log_debug("league_api.ok", leagues=len(parsed))
    return parsed

def _to_api_calculated_at(value: Any) -> str:
    """
    API expects RFC3339 UTC timestamp with millisecond precision and trailing Z.
    """
    raw = to_iso_utc(value) or utc_now_iso()
    try:
        s = str(raw).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    except Exception:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
