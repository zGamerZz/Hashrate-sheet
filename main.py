#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import errno
import random
import re
import sqlite3
import threading
import time
import weakref
from decimal import Decimal, ROUND_HALF_UP
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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

try:
    import dotenv
except Exception:  # pragma: no cover
    dotenv = None  # type: ignore

if dotenv is not None:
    dotenv.load_dotenv()

# Config
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "service_acc.json")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1bq5Sy2pV35x33Q12G5_EJ2S0Kb1iXo1Y2FCxL7QIxGg")

SYNC_POLL_SECONDS = int(os.getenv("SYNC_POLL_SECONDS", "5"))
SYNC_API_GAP_FILL_MAX = max(1, int(os.getenv("SYNC_API_GAP_FILL_MAX", "20")))
SHEET_REFRESH_SECONDS = int(os.getenv("SHEET_REFRESH_SECONDS", "1800"))
GS_WRITE_REQ_PER_MIN = int(os.getenv("GS_WRITE_REQ_PER_MIN", "45"))
GS_READ_REQ_PER_MIN = int(os.getenv("GS_READ_REQ_PER_MIN", "20"))
STATE_DB_PATH = os.getenv("STATE_DB_PATH", "./sync_state.sqlite")
LOCK_FILE_PATH = os.getenv("LOCK_FILE_PATH", "./sync.lock")
LEAGUES_API_URL = os.getenv("LEAGUES_API_URL", "https://api.gomining.com/api/nft-game/league/index")
GOMINING_BEARER_TOKEN = os.getenv("GOMINING_BEARER_TOKEN", "")
GOMINING_API_BASE_URL = os.getenv("GOMINING_API_BASE_URL", "https://api.gomining.com").rstrip("/")
MULTIPLIER_PATH = os.getenv("MULTIPLIER_PATH", "/api/nft-game/round/get-last")
PLAYER_LEADERBOARD_PATH = os.getenv("PLAYER_LEADERBOARD_PATH", "/api/nft-game/user-leaderboard/index")
ROUND_METRICS_CLAN_PATH = os.getenv("ROUND_METRICS_CLAN_PATH", "/api/nft-game/clan-leaderboard/index-v2")
ROUND_CLAN_LEADERBOARD_PATH = os.getenv("ROUND_CLAN_LEADERBOARD_PATH", "/api/nft-game/round/clan-leaderboard")
ROUND_METRICS_TIMEOUT_SECONDS = int(os.getenv("ROUND_METRICS_TIMEOUT_SECONDS", "45"))
ROUND_METRICS_MAX_RETRIES = max(1, int(os.getenv("ROUND_METRICS_MAX_RETRIES", "4")))
ROUND_METRICS_USER_PAGE_LIMIT = max(1, min(50, int(os.getenv("ROUND_METRICS_USER_PAGE_LIMIT", "50"))))
ROUND_METRICS_CLAN_PAGE_LIMIT = max(1, min(50, int(os.getenv("ROUND_METRICS_CLAN_PAGE_LIMIT", "1"))))
ROUND_GET_LAST_SCAN_LOOKBACK = max(1, int(os.getenv("ROUND_GET_LAST_SCAN_LOOKBACK", "200")))
ROUND_CLOSE_ON_ROLLOVER = os.getenv("ROUND_CLOSE_ON_ROLLOVER", "1").strip() in {"1", "true", "TRUE", "yes", "YES"}
ROUND_GET_LAST_STRICT_LEAGUE_VALIDATION = os.getenv(
    "ROUND_GET_LAST_STRICT_LEAGUE_VALIDATION",
    "0",
).strip() in {"1", "true", "TRUE", "yes", "YES"}
CLAN_LEADERBOARD_API_URL = os.getenv(
    "CLAN_LEADERBOARD_API_URL",
    "https://api.gomining.com/api/nft-game/clan-leaderboard/index-v2",
)
CLAN_GET_BY_ID_API_URL = os.getenv(
    "CLAN_GET_BY_ID_API_URL",
    "https://api.gomining.com/api/nft-game/clan/get-by-id",
)
CLAN_API_PAGE_LIMIT = max(1, min(50, int(os.getenv("CLAN_API_PAGE_LIMIT", "50"))))
CLAN_API_TIMEOUT_SECONDS = int(os.getenv("CLAN_API_TIMEOUT_SECONDS", "45"))
CLAN_API_MAX_RETRIES = max(1, int(os.getenv("CLAN_API_MAX_RETRIES", "4")))
ENABLE_CLAN_SYNC = os.getenv("ENABLE_CLAN_SYNC", "0").strip() in {"1", "true", "TRUE", "yes", "YES"}
ROUND_USER_LEADERBOARD_API_URL = os.getenv(
    "ROUND_USER_LEADERBOARD_API_URL",
    "https://api.gomining.com/api/nft-game/round/user-leaderboard",
)
ROUND_API_PAGE_LIMIT = max(1, min(50, int(os.getenv("ROUND_API_PAGE_LIMIT", "50"))))
ROUND_API_TIMEOUT_SECONDS = int(os.getenv("ROUND_API_TIMEOUT_SECONDS", "45"))
ROUND_API_MAX_RETRIES = max(1, int(os.getenv("ROUND_API_MAX_RETRIES", "4")))
GOMINING_API_REQ_PER_MIN = int(os.getenv("GOMINING_API_REQ_PER_MIN", "120"))
TOKEN_URL = os.getenv("TOKEN_URL", "").strip()
TOKEN_X_AUTH = os.getenv("TOKEN_X_AUTH", "").strip()
TOKEN_METHOD = os.getenv("TOKEN_METHOD", "GET").strip().upper()
TOKEN_TIMEOUT_SECONDS = max(1, int(os.getenv("TOKEN_TIMEOUT_SECONDS", "20")))
TOKEN_VERIFY_SSL = os.getenv("TOKEN_VERIFY_SSL", "1").strip() in {"1", "true", "TRUE", "yes", "YES"}
try:
    TOKEN_SHARED_REFRESH_MIN_INTERVAL_SECONDS = max(
        0.0,
        float(os.getenv("TOKEN_SHARED_REFRESH_MIN_INTERVAL_SECONDS", "1.5")),
    )
except Exception:
    TOKEN_SHARED_REFRESH_MIN_INTERVAL_SECONDS = 1.5
try:
    TOKEN_SHARED_REFRESH_WAIT_TIMEOUT_SECONDS = max(
        0.1,
        float(os.getenv("TOKEN_SHARED_REFRESH_WAIT_TIMEOUT_SECONDS", "5.0")),
    )
except Exception:
    TOKEN_SHARED_REFRESH_WAIT_TIMEOUT_SECONDS = 5.0
LEAGUES_API_POLL_SECONDS = int(os.getenv("LEAGUES_API_POLL_SECONDS", str(SHEET_REFRESH_SECONDS)))
LEAGUE_INDEX_LOOKBACK_DAYS = max(0, int(os.getenv("LEAGUE_INDEX_LOOKBACK_DAYS", "7")))
LEAGUE_INDEX_TRY_WITHOUT_CALCULATED_AT = os.getenv(
    "LEAGUE_INDEX_TRY_WITHOUT_CALCULATED_AT",
    "1",
).strip() in {"1", "true", "TRUE", "yes", "YES"}
ENABLE_DB_FALLBACK_RAW = os.getenv("ENABLE_DB_FALLBACK", "").strip()

STABILIZATION_ROUNDS = int(os.getenv("STABILIZATION_ROUNDS", "2"))
MAX_ROUNDS_PER_POLL = int(os.getenv("MAX_ROUNDS_PER_POLL", "250"))
QUEUE_FETCH_LIMIT = int(os.getenv("QUEUE_FETCH_LIMIT", "200"))
APPEND_BATCH_SIZE = int(os.getenv("APPEND_BATCH_SIZE", "25"))
ROW_MAP_KEEP_PER_SHEET = int(os.getenv("ROW_MAP_KEEP_PER_SHEET", "500"))
MAX_MISSING_SHEET_RETRIES = int(os.getenv("MAX_MISSING_SHEET_RETRIES", "20"))
AUTO_EXPAND_SHEET_ROWS = os.getenv("AUTO_EXPAND_SHEET_ROWS", "1").strip() in {"1", "true", "TRUE", "yes", "YES"}
DROP_NON_RETRYABLE_SHEET_ERRORS = os.getenv("DROP_NON_RETRYABLE_SHEET_ERRORS", "1").strip() in {"1", "true", "TRUE", "yes", "YES"}
PURGE_QUEUE_FOR_MISSING_SHEETS = os.getenv("PURGE_QUEUE_FOR_MISSING_SHEETS", "1").strip() in {"1", "true", "TRUE", "yes", "YES"}
DRY_RUN = os.getenv("DRY_RUN", "0").strip() in {"1", "true", "TRUE", "yes", "YES"}
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").strip().upper()
DEBUG_VERBOSE = os.getenv("DEBUG_VERBOSE", "1").strip() in {"1", "true", "TRUE", "yes", "YES"}
LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", "")
QUEUE_DEBUG_PREVIEW = int(os.getenv("QUEUE_DEBUG_PREVIEW", "180"))
HEARTBEAT_SECONDS = int(os.getenv("HEARTBEAT_SECONDS", "30"))
try:
    ENQUEUE_FLUSH_EVERY_SECONDS = max(0.0, float(os.getenv("ENQUEUE_FLUSH_EVERY_SECONDS", "5")))
except Exception:
    ENQUEUE_FLUSH_EVERY_SECONDS = 5.0
RECONCILE_INTERVAL_SECONDS = int(os.getenv("RECONCILE_INTERVAL_SECONDS", "900"))
ROUND_SOFT_FAIL_MAX_ATTEMPTS = max(1, int(os.getenv("ROUND_SOFT_FAIL_MAX_ATTEMPTS", "8")))
try:
    ROUND_RETRY_BACKOFF_MIN_SECONDS = max(1.0, float(os.getenv("ROUND_RETRY_BACKOFF_MIN_SECONDS", "15")))
except Exception:
    ROUND_RETRY_BACKOFF_MIN_SECONDS = 15.0
try:
    ROUND_RETRY_BACKOFF_MAX_SECONDS = max(
        ROUND_RETRY_BACKOFF_MIN_SECONDS,
        float(os.getenv("ROUND_RETRY_BACKOFF_MAX_SECONDS", "600")),
    )
except Exception:
    ROUND_RETRY_BACKOFF_MAX_SECONDS = max(ROUND_RETRY_BACKOFF_MIN_SECONDS, 600.0)
GAP_SCAN_LOOKBACK_ROUNDS = max(10, int(os.getenv("GAP_SCAN_LOOKBACK_ROUNDS", "500")))
ADAPTIVE_RPM_ENABLE = os.getenv("ADAPTIVE_RPM_ENABLE", "1").strip() in {"1", "true", "TRUE", "yes", "YES"}
ADAPTIVE_RPM_MIN = max(1, int(os.getenv("ADAPTIVE_RPM_MIN", "120")))
ADAPTIVE_RPM_MAX = max(ADAPTIVE_RPM_MIN, int(os.getenv("ADAPTIVE_RPM_MAX", str(GOMINING_API_REQ_PER_MIN))))

CONFIG_CELL = "B1"
LOG_START_ROW = 4

MAIN_SHEET_MARKER = "leagueId_to_log"
CLAN_SHEET_MARKER = "leagueId_to_log_clan"
CLAN_TAB_SUFFIX = " - Clan"

LEAGUE_TH_HEADER = "League global TH"
LEAGUE_TH_COL_INDEX = 7  # 0-based index, column H in the sheet.
CLAN_LEGACY_GMT_COL_INDEX = 11  # 0-based index, legacy column L in clan tabs.
MIGRATION_CHECKED_SHEETS: set[int] = set()
POWER_UP_PRICE_HEADER = "Power Up GMT Price"
POWER_UP_PRICE_SENTINEL_HEADER = "Power Up GMT Price (Sentinel)"
CLAN_POWER_UP_PRICE_HEADER = "Clan Power Up GMT Price"
CLAN_POWER_UP_PRICE_SENTINEL_HEADER = "Clan Power Up GMT Price (Sentinel)"
MISSING_HEADER = "missing"
EXCLUDED_USER_BOOST_AUDIT_HEADER = "Excluded User 2144425 Boosts (Audit)"
EXCLUDED_BOOST_USER_ID = 2144425

PPS_BASE_EE_W_PER_TH = 20.0
# Legacy alias kept to minimize churn in downstream imports/tests.
PPS_FACTOR = PPS_BASE_EE_W_PER_TH
POWER_UP_GMT_FACTOR = 0.0389
POWER_UP_ABILITY_ID = "5d6f8166-0f20-486f-b920-e898ca94dcc1"
CLAN_POWER_UP_ABILITY_ID = "8a9b57a9-8a17-4647-8b2f-a164dc52b1f4"
CLAN_POWER_UP_GMT_FACTOR = 0.000555
CLAN_EXACT_MEMBER_COVERAGE_THRESHOLD = float(os.getenv("CLAN_EXACT_MEMBER_COVERAGE_THRESHOLD", "0.90"))
CLAN_EXACT_POWER_COVERAGE_THRESHOLD = float(os.getenv("CLAN_EXACT_POWER_COVERAGE_THRESHOLD", "0.90"))
CLAN_SNAPSHOT_MIN_COVERAGE = float(os.getenv("CLAN_SNAPSHOT_MIN_COVERAGE", "0.50"))
CLAN_SNAPSHOT_FALLBACK_ROUND_WINDOW = int(os.getenv("CLAN_SNAPSHOT_FALLBACK_ROUND_WINDOW", "250"))

BASE_HEADERS: List[str] = [
    "timestamp_utc",
    "leagueId",
    "blockNumber",
    "multiplier",
    "gmtFund",
    "gmtPerBlock",
    "roundId",
    LEAGUE_TH_HEADER,
    "endedAt_utc",
    "roundDuration_sec",
    "blocks_mined",
    "efficiency_league",
]

CLAN_HEADERS: List[str] = [
    "timestamp_utc",
    "leagueId",
    "roundId",
    "snapshotRoundId",
    "clanId",
    "clanName",
    "members_total",
    "members_seen",
    "member_coverage",
    "team_th",
    "team_pps",
    "calc_mode",
]

# Fixed, clean boost layout (no stealer/maintenance/other columns).
ABILITY_HEADER_ORDER: List[str] = [
    "Power Up Boost",
    "Clan Power Up Boost",
    "Rocket (x1)",
    "Rocket (x10)",
    "Rocket (x100)",
    "Instant Boost (x1)",
    "Instant Boost (x10)",
    "Instant Boost (x100)",
    "Echo Boost (x1)",
    "Echo Boost (x10)",
    "Echo Boost (x100)",
    "Focus Boost (x1)",
    "Focus Boost (x10)",
    "Focus Boost (x100)",
]

# Static ability mapping source (API-only abilities path, independent from DB ability_dim).
# Format: (ability_id, ability_name, sort_order)
ABILITY_DIM_STATIC: List[Tuple[str, str, int]] = [
    (CLAN_POWER_UP_ABILITY_ID, "Clan Power Up Boost", 1),
    (POWER_UP_ABILITY_ID, "Power Up Boost", 2),
    ("646d3c76-fe06-414f-8d39-eb0aa7da429a", "Echo Boost (x1)", 3),
    ("2ebfd30d-2950-46fc-b3e9-498a924fd021", "Echo Boost (x10)", 4),
    ("0ffad19c-8091-4527-a600-b7c2f238147d", "Echo Boost (x100)", 5),
    ("370be7b3-d843-4e2f-a23a-eeea37648f99", "Instant Boost (x1)", 6),
    ("a7554d7f-0e50-45e0-ab01-6ac8db629bfd", "Instant Boost (x10)", 7),
    ("8b6fad11-cfc1-4922-ad17-30c41cee39b9", "Instant Boost (x100)", 8),
    ("592724c5-1f59-43b2-af13-2de8d2c97a9d", "Focus Boost (x1)", 9),
    ("a74dd2e0-14a9-46e3-86e5-1f1e8ab1f5c4", "Focus Boost (x10)", 10),
    ("bea03b75-6dbf-4a99-a2c9-24dfe79a3ea1", "Focus Boost (x100)", 11),
    ("3f735c47-cead-4d15-8dad-7db4a7b3f4e7", "Rocket (x1)", 12),
    ("17831b5e-afdf-4cb7-98d7-3a60d81e19ae", "Boost (x10)", 13),
    ("2ec728e7-f31f-4df3-b486-5e82a4976563", "Boost (x10)", 13),
    ("e7af0c32-1409-4b22-9e0b-99e7ea4939df", "Rocket (x100)", 14),
]


# Helpers


def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("hashrate_sync")
    if logger.handlers:
        return logger

    level = getattr(logging, LOG_LEVEL, logging.INFO)
    logger.setLevel(level)
    logger.propagate = False
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    if LOG_FILE_PATH.strip():
        try:
            fh = logging.FileHandler(LOG_FILE_PATH, encoding="utf-8")
            fh.setFormatter(fmt)
            logger.addHandler(fh)
        except Exception as e:
            logger.warning("Failed to init file logger path=%s err=%r", LOG_FILE_PATH, e)
    return logger


LOGGER = _setup_logger()


def _render_fields(**fields: Any) -> str:
    if not fields:
        return ""
    parts: List[str] = []
    for k, v in fields.items():
        if isinstance(v, (dict, list, tuple)):
            try:
                raw = json.dumps(v, ensure_ascii=True, separators=(",", ":"), default=str)
            except Exception:
                raw = repr(v)
        else:
            raw = str(v)
        if len(raw) > 500:
            raw = raw[:500] + "...(+trunc)"
        parts.append(f"{k}={raw}")
    return " | " + " ".join(parts)


def log_debug(msg: str, **fields: Any) -> None:
    if LOGGER.isEnabledFor(logging.DEBUG):
        LOGGER.debug(msg + _render_fields(**fields))


def log_info(msg: str, **fields: Any) -> None:
    LOGGER.info(msg + _render_fields(**fields))


def log_warn(msg: str, **fields: Any) -> None:
    LOGGER.warning(msg + _render_fields(**fields))


def log_error(msg: str, **fields: Any) -> None:
    LOGGER.error(msg + _render_fields(**fields))

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


def calc_league_pps(league_th: Optional[float], efficiency_league: Optional[float]) -> Optional[float]:
    if league_th is None or efficiency_league is None or efficiency_league <= 0:
        return None
    return PPS_BASE_EE_W_PER_TH * league_th / efficiency_league


def calc_power_up_gmt(league_th: Optional[float], efficiency_league: Optional[float]) -> Optional[float]:
    pps = calc_league_pps(league_th, efficiency_league)
    if pps is None:
        return None
    return pps * POWER_UP_GMT_FACTOR


def round_gmt_2(value: float) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def has_sentinel_avatar_discount(avatar_url: Any) -> bool:
    s = str(avatar_url or "").strip()
    if not s:
        return False
    base = s.split("?", 1)[0].rstrip("/")
    m = re.search(r"(\d+)(?:\.[A-Za-z0-9]+)?$", base)
    if not m:
        return False
    try:
        n = int(m.group(1))
    except Exception:
        return False
    return 200 <= n <= 299


def has_sentinel_alias_discount(alias: Any) -> bool:
    s = str(alias or "").strip().lower()
    if not s:
        return False
    # Fallback marker used by some users/clans when avatar URL is unavailable.
    return ("sentinel" in s) or ("\U0001F5B2" in s)


def has_sentinel_user_discount(avatar_url: Any, alias: Any) -> bool:
    return has_sentinel_avatar_discount(avatar_url) or has_sentinel_alias_discount(alias)


def calc_boost_gmt_from_api_round(
    rec: Dict[str, Any],
    counts_by_name: Dict[str, int],
    boost_header: str,
) -> Optional[float]:
    boost_count = int(counts_by_name.get(boost_header, 0) or 0)
    if boost_count <= 0:
        return 0.0
    league_th = safe_float(rec.get("league_th"))
    efficiency_league = safe_float(rec.get("efficiency_league"))
    per_use = calc_power_up_gmt(league_th, efficiency_league)
    if per_use is None:
        return None
    return float(boost_count) * per_use


def calc_power_up_gmt_from_api_round(
    rec: Dict[str, Any],
    counts_by_name: Dict[str, int],
) -> Optional[float]:
    return calc_boost_gmt_from_api_round(rec, counts_by_name, "Power Up Boost")


def calc_boost_gmt_pair_from_boost_users_api(
    boost_users: Sequence[Dict[str, Any]],
    clan_api: Optional["GoMiningClanApiClient"],
) -> Tuple[Optional[float], Optional[float]]:
    normal, sentinel, _missing_aliases = calc_boost_gmt_triplet_from_boost_users_api(
        boost_users,
        clan_api,
    )
    return normal, sentinel


def calc_boost_gmt_triplet_from_boost_users_api(
    boost_users: Sequence[Dict[str, Any]],
    clan_api: Optional["GoMiningClanApiClient"],
    resolution_stats: Optional[Dict[str, int]] = None,
) -> Tuple[Optional[float], Optional[float], List[str]]:
    """
    Exact API-only calculation based on users who activated boost ability (boolean activation).
    Each user is counted once per round regardless of API count.
    Per-user price is rounded to 2 decimals before summation.
    Returns (normal, sentinel_adjusted) where sentinel-adjusted applies 20% discount
    for users with avatar id in [200..299] (or sentinel marker in alias).
    Returns (None, None, missing_aliases) when no user can be resolved.
    """
    by_user_usage: Dict[int, Dict[str, Any]] = {}
    clan_ids: List[int] = []
    needed_users_by_clan: Dict[int, set[int]] = {}
    for item in boost_users:
        if not isinstance(item, dict):
            continue
        uid = safe_int(item.get("user_id"))
        cnt = safe_int(item.get("count")) or 0
        cid = safe_int(item.get("clan_id"))
        avatar_url = str(item.get("avatar_url") or "")
        alias = str(item.get("alias") or "")
        if uid is None or cnt <= 0:
            continue
        usage = by_user_usage.get(uid)
        sentinel = has_sentinel_user_discount(avatar_url, alias)
        if usage is None:
            by_user_usage[uid] = {
                "clan_id": cid,
                "sentinel": sentinel,
                "alias": alias,
            }
        else:
            if usage.get("clan_id") is None and cid is not None:
                usage["clan_id"] = cid
            usage["sentinel"] = bool(usage.get("sentinel")) or sentinel
            if not str(usage.get("alias") or "").strip() and alias:
                usage["alias"] = alias
    if resolution_stats is not None:
        resolution_stats["resolved_api"] = 0
        resolution_stats["missing"] = 0
        resolution_stats["total_users"] = len(by_user_usage)

    if not by_user_usage:
        return 0.0, 0.0, []
    by_clan_user: Dict[Tuple[int, int], Tuple[float, float]] = {}
    by_user: Dict[int, Tuple[float, float]] = {}
    if clan_api is not None:
        for uid, usage in by_user_usage.items():
            cid = safe_int(usage.get("clan_id"))
            if cid is not None:
                clan_ids.append(cid)
                needed_users_by_clan.setdefault(cid, set()).add(uid)
        try:
            by_clan_user, by_user = clan_api.fetch_user_power_ee_for_clans(
                clan_ids,
                needed_users_by_clan=needed_users_by_clan,
            )
        except TypeError:
            # Backward-compatible fallback for stubs/tests without the new named arg.
            by_clan_user, by_user = clan_api.fetch_user_power_ee_for_clans(clan_ids)

    total_gmt = Decimal("0.00")
    total_gmt_sentinel = Decimal("0.00")
    resolved_users = 0
    missing_aliases: List[str] = []

    for uid, usage in by_user_usage.items():
        cid = safe_int(usage.get("clan_id"))
        sentinel = bool(usage.get("sentinel"))
        alias = str(usage.get("alias") or "").strip() or f"user_{uid}"
        power_ee: Optional[Tuple[float, float]] = None
        resolved_source: Optional[str] = None
        if cid is not None:
            power_ee = by_clan_user.get((cid, uid))
            if power_ee is not None:
                resolved_source = "api"
        if power_ee is None:
            power_ee = by_user.get(uid)
            if power_ee is not None:
                resolved_source = "api"
        if power_ee is None:
            missing_aliases.append(alias)
            continue
        power, ee = power_ee
        per_use = calc_power_up_gmt(power, ee)
        if per_use is None:
            missing_aliases.append(alias)
            continue
        user_price = round_gmt_2(float(per_use))
        total_gmt += user_price
        total_gmt_sentinel += round_gmt_2(float(user_price * Decimal("0.8"))) if sentinel else user_price
        resolved_users += 1
        if resolution_stats is not None:
            if resolved_source == "api":
                resolution_stats["resolved_api"] = int(resolution_stats.get("resolved_api", 0)) + 1

    if resolution_stats is not None:
        resolution_stats["missing"] = len(missing_aliases)

    if resolved_users <= 0:
        return None, None, missing_aliases
    return float(total_gmt), float(total_gmt_sentinel), missing_aliases


def calc_power_up_gmt_pair_from_power_up_users_api(
    power_up_users: Sequence[Dict[str, Any]],
    clan_api: Optional["GoMiningClanApiClient"],
) -> Tuple[Optional[float], Optional[float]]:
    return calc_boost_gmt_pair_from_boost_users_api(power_up_users, clan_api)


def calc_power_up_gmt_triplet_from_power_up_users_api(
    power_up_users: Sequence[Dict[str, Any]],
    clan_api: Optional["GoMiningClanApiClient"],
    resolution_stats: Optional[Dict[str, int]] = None,
) -> Tuple[Optional[float], Optional[float], List[str]]:
    return calc_boost_gmt_triplet_from_boost_users_api(
        power_up_users,
        clan_api,
        resolution_stats=resolution_stats,
    )


def calc_clan_power_up_gmt_pair_from_boost_users_api(
    clan_power_up_users: Sequence[Dict[str, Any]],
    clan_api: Optional["GoMiningClanApiClient"],
) -> Tuple[Optional[float], Optional[float]]:
    """
    Exact clan-based calculation for Clan Power Up:
      per_use_gmt(clan) = (SUM_over_clan_members(PPS_BASE_EE_W_PER_TH * power / ee)) * CLAN_POWER_UP_GMT_FACTOR
      total = SUM(per_user_once * per_use_gmt(user_clan))
    Sentinel-adjusted value applies 20% discount for users with avatar id in [200..299]
    (or sentinel marker in alias).
    Returns (None, None) when users exist but clan pricing cannot be fully resolved.
    """
    try:
        by_user_usage: Dict[int, Dict[str, Any]] = {}
        clan_ids: List[int] = []
        for item in clan_power_up_users:
            if not isinstance(item, dict):
                continue
            uid = safe_int(item.get("user_id"))
            cnt = safe_int(item.get("count")) or 0
            cid = safe_int(item.get("clan_id"))
            avatar_url = str(item.get("avatar_url") or "")
            alias = str(item.get("alias") or "")
            if uid is None or cnt <= 0:
                continue
            usage = by_user_usage.get(uid)
            sentinel = has_sentinel_user_discount(avatar_url, alias)
            if usage is None:
                by_user_usage[uid] = {
                    "clan_id": cid,
                    "sentinel": sentinel,
                }
            else:
                if usage.get("clan_id") is None and cid is not None:
                    usage["clan_id"] = cid
                usage["sentinel"] = bool(usage.get("sentinel")) or sentinel
        if not by_user_usage:
            return 0.0, 0.0
        for usage in by_user_usage.values():
            cid = safe_int(usage.get("clan_id"))
            if cid is not None:
                clan_ids.append(cid)
        if clan_api is None:
            return None, None
        team_pps_by_clan = clan_api.fetch_clan_team_pps_for_clans(clan_ids)
        if not team_pps_by_clan:
            return None, None

        total_gmt = Decimal("0.00")
        total_gmt_sentinel = Decimal("0.00")
        resolved_users = 0
        for _uid, usage in by_user_usage.items():
            cid = safe_int(usage.get("clan_id"))
            sentinel = bool(usage.get("sentinel"))
            if cid is None:
                continue
            team_pps = safe_float(team_pps_by_clan.get(cid))
            if team_pps is None or team_pps <= 0:
                continue
            per_use = calc_clan_power_up_gmt(team_pps)
            if per_use is None:
                continue
            user_price = round_gmt_2(float(per_use))
            total_gmt += user_price
            total_gmt_sentinel += round_gmt_2(float(user_price * Decimal("0.8"))) if sentinel else user_price
            resolved_users += 1
        if resolved_users <= 0:
            return None, None
        return float(total_gmt), float(total_gmt_sentinel)
    except Exception as e:
        log_warn(
            "sync.clan_power_up_calc_failed",
            err=repr(e),
            users=len([u for u in clan_power_up_users if isinstance(u, dict)]),
        )
        return None, None


def calc_team_pps_exact(sum_th_over_w: Optional[float]) -> Optional[float]:
    if sum_th_over_w is None or sum_th_over_w <= 0:
        return None
    return PPS_BASE_EE_W_PER_TH * sum_th_over_w


def calc_team_pps_fallback(team_th: Optional[float], efficiency_league: Optional[float]) -> Optional[float]:
    if team_th is None or team_th <= 0:
        return None
    return calc_league_pps(team_th, efficiency_league)


def calc_clan_power_up_gmt(team_pps: Optional[float]) -> Optional[float]:
    if team_pps is None:
        return None
    return team_pps * CLAN_POWER_UP_GMT_FACTOR


def canonical_ability_header(ability_name: str) -> Optional[str]:
    """
    Map DB ability names to the fixed output header set.
    Only selected abilities are mapped; everything else is ignored.
    """
    n = re.sub(r"\s+", " ", (ability_name or "").strip().lower())
    if not n:
        return None

    if n in {"power up boost", "power-up boost"}:
        return "Power Up Boost"
    if n in {"clan power up boost", "clan powerup boost", "clan powerup"}:
        return "Clan Power Up Boost"

    m = re.match(r"^(rocket|boost)\s*\(x(1|10|100)\)$", n)
    if m:
        return f"Rocket (x{m.group(2)})"

    m = re.match(r"^instant boost\s*\(x(1|10|100)\)$", n)
    if m:
        return f"Instant Boost (x{m.group(1)})"

    m = re.match(r"^echo boost\s*\(x(1|10|100)\)$", n)
    if m:
        return f"Echo Boost (x{m.group(1)})"

    m = re.match(r"^focus boost\s*\(x(1|10|100)\)$", n)
    if m:
        return f"Focus Boost (x{m.group(1)})"

    # Optional tolerance for old style names like "Boost X10", "Echo Boost X1", etc.
    m = re.match(r"^(rocket|boost)\s*x(1|10|100)$", n)
    if m:
        return f"Rocket (x{m.group(2)})"
    m = re.match(r"^instant boost\s*x(1|10|100)$", n)
    if m:
        return f"Instant Boost (x{m.group(1)})"
    m = re.match(r"^echo boost\s*x(1|10|100)$", n)
    if m:
        return f"Echo Boost (x{m.group(1)})"
    m = re.match(r"^focus boost\s*x(1|10|100)$", n)
    if m:
        return f"Focus Boost (x{m.group(1)})"
    return None


def build_ability_id_to_header(catalog: Sequence[Tuple[str, str, int]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for ability_id, ability_name, _sort_order in catalog:
        hdr = canonical_ability_header(ability_name)
        if hdr is None:
            continue
        out[str(ability_id)] = hdr
    return out


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


def _extract_token_candidate(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, dict):
        for k in ("token", "access_token", "bearer", "jwt", "jwtToken", "jwt_token"):
            if k in value:
                out = _extract_token_candidate(value.get(k))
                if out:
                    return out
        nested = value.get("data")
        if nested is not None:
            out = _extract_token_candidate(nested)
            if out:
                return out
        return None
    if isinstance(value, list):
        for item in value:
            out = _extract_token_candidate(item)
            if out:
                return out
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.lower().startswith("bearer "):
        s = s[7:].strip()
    return s or None


def _is_jwt_expired_response(resp: Any) -> bool:
    """
    Detect JWT-expired responses where API may return 403 instead of 401.
    """
    status = safe_int(getattr(resp, "status_code", None))
    if status not in {401, 403}:
        return False
    text = str(getattr(resp, "text", "") or "")
    text_up = text.upper()
    if "JWT_TOKEN_EXPIRED" in text_up or "JWT TOKEN IS EXPIRED" in text_up:
        return True
    try:
        body = resp.json()
    except Exception:
        body = None
    if isinstance(body, dict):
        parts: List[str] = []
        for key in ("statusMessage", "description", "message", "error"):
            val = body.get(key)
            if val is not None:
                parts.append(str(val))
        meta = body.get("meta")
        if isinstance(meta, dict):
            for key in ("statusMessage", "description", "message", "error"):
                val = meta.get(key)
                if val is not None:
                    parts.append(str(val))
        merged = " ".join(parts).upper()
        if "JWT_TOKEN_EXPIRED" in merged or "JWT TOKEN IS EXPIRED" in merged:
            return True
    return False


def fetch_bearer_token_from_auth_api() -> Optional[str]:
    if not TOKEN_URL or not TOKEN_X_AUTH:
        return None
    method = TOKEN_METHOD if TOKEN_METHOD in {"GET", "POST"} else "GET"
    headers = {
        "x-auth": TOKEN_X_AUTH,
        "accept": "application/json, text/plain, */*",
    }
    try:
        if method == "POST":
            resp = requests.post(
                TOKEN_URL,
                headers=headers,
                json={},
                timeout=TOKEN_TIMEOUT_SECONDS,
                verify=TOKEN_VERIFY_SSL,
            )
        else:
            resp = requests.get(
                TOKEN_URL,
                headers=headers,
                timeout=TOKEN_TIMEOUT_SECONDS,
                verify=TOKEN_VERIFY_SSL,
            )
    except Exception as e:
        log_warn("token_api.request_failed", url=TOKEN_URL, method=method, err=repr(e))
        return None

    if resp.status_code != 200:
        log_warn("token_api.http_error", url=TOKEN_URL, method=method, status=resp.status_code, body_preview=resp.text[:180])
        return None

    token: Optional[str] = None
    try:
        token = _extract_token_candidate(resp.json())
    except Exception:
        token = _extract_token_candidate(resp.text)

    if not token:
        log_warn("token_api.token_missing", url=TOKEN_URL, method=method, body_preview=resp.text[:180])
        return None
    log_info("token_api.ok", url=TOKEN_URL, method=method)
    return token


class _SharedTokenState:
    def __init__(self) -> None:
        self.cond = threading.Condition()
        self.token: str = ""
        self.refreshed_at_mono: float = 0.0
        self.refreshing: bool = False


_SHARED_TOKEN_STATES_LOCK = threading.Lock()
_SHARED_TOKEN_STATES_WEAK: "weakref.WeakKeyDictionary[Callable[[], Optional[str]], _SharedTokenState]" = weakref.WeakKeyDictionary()
_SHARED_TOKEN_STATES_STRONG: Dict[int, _SharedTokenState] = {}


def _get_shared_token_state(token_fetcher: Callable[[], Optional[str]]) -> _SharedTokenState:
    with _SHARED_TOKEN_STATES_LOCK:
        try:
            state = _SHARED_TOKEN_STATES_WEAK.get(token_fetcher)
            if state is None:
                state = _SharedTokenState()
                _SHARED_TOKEN_STATES_WEAK[token_fetcher] = state
            return state
        except TypeError:
            key = id(token_fetcher)
            state = _SHARED_TOKEN_STATES_STRONG.get(key)
            if state is None:
                state = _SharedTokenState()
                _SHARED_TOKEN_STATES_STRONG[key] = state
            return state


def _shared_token_cached(token_fetcher: Callable[[], Optional[str]]) -> Optional[str]:
    state = _get_shared_token_state(token_fetcher)
    with state.cond:
        tok = (state.token or "").strip()
        return tok or None


def _shared_token_fetch(token_fetcher: Callable[[], Optional[str]], *, force: bool) -> Optional[str]:
    state = _get_shared_token_state(token_fetcher)
    now_mono = time.monotonic()
    with state.cond:
        if state.token and not force:
            return state.token
        if (
            force
            and state.token
            and (now_mono - state.refreshed_at_mono) < TOKEN_SHARED_REFRESH_MIN_INTERVAL_SECONDS
        ):
            return state.token

        if state.refreshing:
            deadline = now_mono + TOKEN_SHARED_REFRESH_WAIT_TIMEOUT_SECONDS
            while state.refreshing:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                state.cond.wait(timeout=remaining)
            now_mono = time.monotonic()
            if state.token and (
                not force or (now_mono - state.refreshed_at_mono) < TOKEN_SHARED_REFRESH_MIN_INTERVAL_SECONDS
            ):
                return state.token
            if state.refreshing:
                return (state.token or "").strip() or None
        state.refreshing = True

    tok: Optional[str] = None
    try:
        tok = (token_fetcher() or "").strip()
    except Exception as e:
        log_warn("token_api.fetcher_exception", err=repr(e))

    with state.cond:
        if tok:
            state.token = tok
            state.refreshed_at_mono = time.monotonic()
        state.refreshing = False
        state.cond.notify_all()
        if tok:
            return tok
        cached = (state.token or "").strip()
        return cached or None


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


def calc_team_th_and_pps_from_users(users: Sequence[Dict[str, Any]]) -> Tuple[float, float]:
    team_th = 0.0
    team_pps = 0.0
    for user in users:
        if not isinstance(user, dict):
            continue
        pwr = safe_float(user.get("power"))
        ee = safe_float(user.get("ee"))
        if pwr is None or pwr <= 0:
            continue
        team_th += pwr
        if ee is None or ee <= 0:
            continue
        team_pps += PPS_BASE_EE_W_PER_TH * pwr / ee
    return team_th, team_pps


class GoMiningClanApiClient:
    def __init__(
        self,
        bearer_token: str,
        limiter: TokenBucket,
        leaderboard_url: str = CLAN_LEADERBOARD_API_URL,
        clan_get_by_id_url: str = CLAN_GET_BY_ID_API_URL,
        page_limit: int = CLAN_API_PAGE_LIMIT,
        timeout_seconds: int = CLAN_API_TIMEOUT_SECONDS,
        max_retries: int = CLAN_API_MAX_RETRIES,
        token_fetcher: Optional[Callable[[], Optional[str]]] = None,
        rate_controller: Optional[AdaptiveRateController] = None,
    ) -> None:
        self.bearer_token = bearer_token.strip()
        self.token_fetcher = token_fetcher
        self.limiter = limiter
        self.leaderboard_url = leaderboard_url
        self.clan_get_by_id_url = clan_get_by_id_url
        self.page_limit = max(1, min(50, int(page_limit)))
        self.timeout_seconds = max(1, int(timeout_seconds))
        self.max_retries = max(1, int(max_retries))
        self.rate_controller = rate_controller
        self.session = requests.Session()
        self.headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "x-device-type": "desktop",
            "origin": "https://app.gomining.com",
            "referer": "https://app.gomining.com/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        }
        if self.bearer_token:
            self._set_bearer(self.bearer_token)
        # Cache exact user power/ee by (clan_id, user_id) to avoid repeated full clan scans.
        self._user_power_ee_cache: Dict[Tuple[int, int], Tuple[float, float]] = {}
        # Cache clan team PPS (derived from full clan/get-by-id user lists).
        self._clan_team_pps_cache: Dict[int, float] = {}

    def _set_bearer(self, token: str) -> None:
        self.bearer_token = token.strip()
        self.headers["authorization"] = f"Bearer {self.bearer_token}"

    def _refresh_bearer(self, force: bool = False) -> bool:
        if not self.token_fetcher:
            return bool(self.bearer_token)
        if not force:
            cached = _shared_token_cached(self.token_fetcher)
            if cached and cached != self.bearer_token:
                self._set_bearer(cached)
                return True
            if self.bearer_token:
                return True
        tok = _shared_token_fetch(self.token_fetcher, force=force)
        if not tok:
            return bool(self.bearer_token)
        if tok != self.bearer_token:
            self._set_bearer(tok)
        return True

    @staticmethod
    def _extract_clan_meta(item: Any) -> Tuple[Optional[int], str]:
        if not isinstance(item, dict):
            return None, ""
        cid = safe_int(item.get("clanId"))
        if cid is None:
            cid = safe_int(item.get("id"))
        clan = item.get("clan")
        name = ""
        if isinstance(clan, dict):
            name = str(clan.get("name") or "").strip()
        if not name:
            name = str(item.get("clanName") or item.get("name") or "").strip()
        return cid, name

    def _post_json_with_retry(self, url: str, payload: Dict[str, Any], op: str, **ctx: Any) -> Optional[Dict[str, Any]]:
        self._refresh_bearer(force=False)
        delay_s = 0.6
        for attempt in range(1, self.max_retries + 1):
            self.limiter.wait_for_token(1.0)
            try:
                resp = self.session.post(url, headers=self.headers, json=payload, timeout=self.timeout_seconds)
            except Exception as e:
                if attempt >= self.max_retries:
                    log_warn("gomining_api.request_failed", op=op, attempt=attempt, err=repr(e), **ctx)
                    return None
                sleep_s = delay_s + random.uniform(0.0, 0.2)
                log_warn("gomining_api.request_retry", op=op, attempt=attempt, sleep_s=round(sleep_s, 3), err=repr(e), **ctx)
                time.sleep(sleep_s)
                delay_s = min(8.0, delay_s * 2.0)
                continue

            if resp.status_code == 200:
                if self.rate_controller is not None:
                    self.rate_controller.record_status(200)
                try:
                    data = resp.json()
                    if isinstance(data, dict):
                        return data
                    log_warn("gomining_api.invalid_json_type", op=op, attempt=attempt, typ=type(data).__name__, **ctx)
                    return None
                except Exception as e:
                    log_warn("gomining_api.invalid_json", op=op, attempt=attempt, err=repr(e), **ctx)
                    return None

            if resp.status_code == 401:
                if attempt < self.max_retries and self._refresh_bearer(force=True):
                    log_warn("gomining_api.http_unauthorized_retry", op=op, attempt=attempt, **ctx)
                    continue
                log_warn("gomining_api.http_unauthorized", op=op, attempt=attempt, **ctx)
                return None
            if resp.status_code == 403 and _is_jwt_expired_response(resp):
                if attempt < self.max_retries and self._refresh_bearer(force=True):
                    log_warn("gomining_api.http_forbidden_expired_retry", op=op, attempt=attempt, **ctx)
                    continue
                log_warn("gomining_api.http_forbidden_expired", op=op, attempt=attempt, **ctx)
                return None

            retryable = resp.status_code in {429, 500, 502, 503, 504}
            if self.rate_controller is not None:
                self.rate_controller.record_status(resp.status_code)
            if retryable and attempt < self.max_retries:
                retry_after = None
                try:
                    ra = resp.headers.get("Retry-After")
                    if ra:
                        retry_after = float(ra)
                except Exception:
                    retry_after = None
                sleep_s = max(delay_s, retry_after or 0.0) + random.uniform(0.0, 0.2)
                log_warn(
                    "gomining_api.http_retry",
                    op=op,
                    attempt=attempt,
                    status=resp.status_code,
                    sleep_s=round(sleep_s, 3),
                    **ctx,
                )
                time.sleep(sleep_s)
                delay_s = min(8.0, delay_s * 2.0)
                continue

            log_warn(
                "gomining_api.http_error",
                op=op,
                attempt=attempt,
                status=resp.status_code,
                body_preview=resp.text[:180],
                **ctx,
            )
            return None
        return None

    def _fetch_leaderboard_clans(self, league_id: int, calculated_at: str) -> Optional[Dict[int, str]]:
        clans: Dict[int, str] = {}
        skip = 0
        max_pages = 200
        page_no = 0
        while True:
            payload = {
                "calculatedAt": calculated_at,
                "leagueId": league_id,
                "pagination": {"skip": skip, "limit": self.page_limit},
            }
            body = self._post_json_with_retry(
                self.leaderboard_url,
                payload,
                op="clan_leaderboard",
                league_id=league_id,
                skip=skip,
            )
            if body is None:
                return None
            data = body.get("data")
            if not isinstance(data, dict):
                log_warn("gomining_api.leaderboard_bad_shape", league_id=league_id, skip=skip)
                return None

            remaining = data.get("clansRemaining") or []
            promoted = data.get("clansPromoted") or []
            relegated = data.get("clansRelegated") or []
            for group in (remaining, promoted, relegated):
                if not isinstance(group, list):
                    continue
                for item in group:
                    cid, name = self._extract_clan_meta(item)
                    if cid is None:
                        continue
                    if name:
                        clans[cid] = name
                    else:
                        clans.setdefault(cid, f"clan-{cid}")

            total_count = safe_int(data.get("count"))
            max_group_len = max(
                len(remaining) if isinstance(remaining, list) else 0,
                len(promoted) if isinstance(promoted, list) else 0,
                len(relegated) if isinstance(relegated, list) else 0,
            )
            if total_count is not None and total_count >= 0:
                if (skip + self.page_limit) >= total_count:
                    break
            elif max_group_len < self.page_limit:
                break

            skip += self.page_limit
            page_no += 1
            if page_no >= max_pages:
                log_warn("gomining_api.leaderboard_pagination_guard", league_id=league_id, pages=page_no)
                return None
        return clans

    def _fetch_clan_detail_all_pages(self, clan_id: int) -> Optional[Dict[str, Any]]:
        skip = 0
        users_all: List[Dict[str, Any]] = []
        base: Optional[Dict[str, Any]] = None
        max_pages = 400
        page_no = 0
        while True:
            payload = {
                "clanId": clan_id,
                "pagination": {"limit": self.page_limit, "skip": skip, "count": 0},
                "filters": {"filterType": "none"},
                "sort": {"sortType": "none"},
            }
            body = self._post_json_with_retry(
                self.clan_get_by_id_url,
                payload,
                op="clan_get_by_id",
                clan_id=clan_id,
                skip=skip,
            )
            if body is None:
                return None
            data = body.get("data")
            if not isinstance(data, dict):
                log_warn("gomining_api.clan_bad_shape", clan_id=clan_id, skip=skip)
                return None

            if base is None:
                base = {k: v for k, v in data.items() if k != "usersForClient"}

            chunk = data.get("usersForClient") or []
            if isinstance(chunk, list):
                for item in chunk:
                    if isinstance(item, dict):
                        users_all.append(item)
            else:
                chunk = []

            users_count = safe_int(data.get("usersCount"))
            chunk_len = len(chunk)
            if chunk_len < self.page_limit:
                break
            if users_count is not None and users_count >= 0 and len(users_all) >= users_count:
                break

            skip += self.page_limit
            page_no += 1
            if page_no >= max_pages:
                log_warn("gomining_api.clan_pagination_guard", clan_id=clan_id, pages=page_no)
                return None

        if base is None:
            return None
        base["usersForClient"] = users_all
        expected_total = safe_int(base.get("usersCount"))
        if expected_total is not None and expected_total > 0 and len(users_all) < expected_total:
            log_warn("gomining_api.clan_incomplete", clan_id=clan_id, expected=expected_total, seen=len(users_all))
            return None
        return base

    def fetch_clan_rows_for_round(
        self,
        league_id: int,
        round_id: int,
        calculated_at: str,
        snapshot_ts: Optional[str] = None,
    ) -> Optional[List[Dict[str, Any]]]:
        clan_names = self._fetch_leaderboard_clans(league_id, calculated_at)
        if clan_names is None:
            return None
        rows: List[Dict[str, Any]] = []
        snap_ts = snapshot_ts or utc_now_iso()
        for clan_id in sorted(clan_names.keys()):
            detail = self._fetch_clan_detail_all_pages(clan_id)
            if detail is None:
                return None
            users_raw = detail.get("usersForClient") or []
            users = [u for u in users_raw if isinstance(u, dict)] if isinstance(users_raw, list) else []
            members_seen = len(users)
            members_total = safe_int(detail.get("usersCount"))
            if members_total is None or members_total <= 0:
                members_total = members_seen
            member_cov = (float(members_seen) / float(members_total)) if members_total > 0 else None
            team_th, team_pps = calc_team_th_and_pps_from_users(users)
            clan_name = str(detail.get("name") or clan_names.get(clan_id) or f"clan-{clan_id}")
            rows.append(
                {
                    "round_id": round_id,
                    "snapshot_round_id": round_id,
                    "snapshot_ts": snap_ts,
                    "clan_id": clan_id,
                    "clan_name": clan_name,
                    "members_total": members_total,
                    "members_seen": members_seen,
                    "member_coverage": member_cov,
                    "team_th": team_th,
                    "team_pps": team_pps,
                    "calc_mode": "api_exact",
                }
            )
        rows.sort(key=lambda x: (-(safe_float(x.get("team_pps")) or 0.0), safe_int(x.get("clan_id")) or 0))
        return rows

    def _extract_user_power_ee_from_clan_row(self, user: Dict[str, Any]) -> Optional[Tuple[int, float, float]]:
        uid = safe_int(user.get("id"))
        if uid is None:
            nested = user.get("user")
            if isinstance(nested, dict):
                uid = safe_int(nested.get("id"))
        if uid is None:
            uid = safe_int(user.get("userId"))
        power = safe_float(user.get("power"))
        ee = safe_float(user.get("ee"))
        if ee is None:
            ee = safe_float(user.get("meanEnergyEfficiency"))
        if ee is None:
            ee = safe_float(user.get("efficiency"))
        if uid is None or power is None or power <= 0 or ee is None or ee <= 0:
            return None
        return uid, power, ee

    def _fetch_clan_user_power_ee_partial(
        self,
        clan_id: int,
        needed_user_ids: Optional[set[int]],
    ) -> Dict[int, Tuple[float, float]]:
        """
        Fetch only the needed users' power/ee for one clan.
        Stops pagination early as soon as all needed users are found.
        """
        needed = set(int(x) for x in (needed_user_ids or set()) if safe_int(x) is not None)
        # If caller asked for a concrete set and it is empty, nothing to do.
        if needed_user_ids is not None and not needed:
            return {}

        out: Dict[int, Tuple[float, float]] = {}
        skip = 0
        page_no = 0
        max_pages = 400
        while True:
            payload = {
                "clanId": clan_id,
                "pagination": {"limit": self.page_limit, "skip": skip, "count": 0},
                "filters": {"filterType": "none"},
                "sort": {"sortType": "none"},
            }
            body = self._post_json_with_retry(
                self.clan_get_by_id_url,
                payload,
                op="clan_get_by_id_partial",
                clan_id=clan_id,
                skip=skip,
            )
            if body is None:
                return out
            data = body.get("data")
            if not isinstance(data, dict):
                log_warn("gomining_api.clan_bad_shape", clan_id=clan_id, skip=skip)
                return out

            chunk = data.get("usersForClient") or []
            if not isinstance(chunk, list):
                chunk = []

            for raw_user in chunk:
                if not isinstance(raw_user, dict):
                    continue
                parsed = self._extract_user_power_ee_from_clan_row(raw_user)
                if parsed is None:
                    continue
                uid, power, ee = parsed
                if needed_user_ids is not None and uid not in needed:
                    continue
                out[uid] = (power, ee)

            if needed_user_ids is not None and len(out) >= len(needed):
                break

            users_count = safe_int(data.get("usersCount"))
            if len(chunk) < self.page_limit:
                break
            if users_count is not None and users_count >= 0 and (skip + len(chunk)) >= users_count:
                break

            skip += self.page_limit
            page_no += 1
            if page_no >= max_pages:
                log_warn("gomining_api.clan_partial_pagination_guard", clan_id=clan_id, pages=page_no)
                break
        return out

    def fetch_user_power_ee_for_clans(
        self,
        clan_ids: Sequence[int],
        needed_users_by_clan: Optional[Dict[int, set[int]]] = None,
    ) -> Tuple[Dict[Tuple[int, int], Tuple[float, float]], Dict[int, Tuple[float, float]]]:
        """
        Build user power/eff indexes from clan/get-by-id (API-only source).
        Returns:
          - by_clan_user[(clan_id, user_id)] = (power, ee)
          - by_user[user_id] = (power, ee)  # fallback index
        """
        by_clan_user: Dict[Tuple[int, int], Tuple[float, float]] = {}
        by_user: Dict[int, Tuple[float, float]] = {}
        clan_id_set = sorted({int(x) for x in clan_ids if safe_int(x) is not None})
        needed_map: Dict[int, set[int]] = {}
        if isinstance(needed_users_by_clan, dict):
            for k, vals in needed_users_by_clan.items():
                cid = safe_int(k)
                if cid is None:
                    continue
                if isinstance(vals, set):
                    uids = {int(x) for x in vals if safe_int(x) is not None}
                else:
                    try:
                        uids = {int(x) for x in list(vals) if safe_int(x) is not None}  # type: ignore[arg-type]
                    except Exception:
                        uids = set()
                if uids:
                    needed_map[cid] = uids

        for clan_id in clan_id_set:
            needed_for_clan = needed_map.get(clan_id)

            # Fast path: use cache when all required users are already known.
            if needed_for_clan:
                all_cached = True
                for uid in needed_for_clan:
                    pair = self._user_power_ee_cache.get((clan_id, uid))
                    if pair is None:
                        all_cached = False
                        break
                    by_clan_user[(clan_id, uid)] = pair
                    by_user[uid] = pair
                if all_cached:
                    continue

            fetched = self._fetch_clan_user_power_ee_partial(clan_id, needed_for_clan)
            for uid, pair in fetched.items():
                self._user_power_ee_cache[(clan_id, uid)] = pair
                by_clan_user[(clan_id, uid)] = pair
                by_user[uid] = pair
        return by_clan_user, by_user

    def fetch_clan_team_pps_for_clans(self, clan_ids: Sequence[int]) -> Dict[int, float]:
        """
        Compute team PPS per clan from full clan/get-by-id user lists:
          team_pps = SUM(PPS_BASE_EE_W_PER_TH * user_power / user_ee) for users with valid power/ee.
        """
        out: Dict[int, float] = {}
        clan_id_set = sorted({int(x) for x in clan_ids if safe_int(x) is not None})
        if not clan_id_set:
            return out

        missing: List[int] = []
        for clan_id in clan_id_set:
            cached = self._clan_team_pps_cache.get(clan_id)
            if cached is None:
                missing.append(clan_id)
                continue
            out[clan_id] = float(cached)

        for clan_id in missing:
            detail = self._fetch_clan_detail_all_pages(clan_id)
            if not isinstance(detail, dict):
                continue
            users_raw = detail.get("usersForClient") or []
            users = [u for u in users_raw if isinstance(u, dict)] if isinstance(users_raw, list) else []
            _team_th, team_pps = calc_team_th_and_pps_from_users(users)
            self._clan_team_pps_cache[clan_id] = float(team_pps)
            out[clan_id] = float(team_pps)
        return out


class GoMiningRoundMetricsApiClient:
    def __init__(
        self,
        bearer_token: str,
        limiter: TokenBucket,
        base_url: str = GOMINING_API_BASE_URL,
        multiplier_path: str = MULTIPLIER_PATH,
        round_clan_leaderboard_path: str = ROUND_CLAN_LEADERBOARD_PATH,
        player_leaderboard_path: str = PLAYER_LEADERBOARD_PATH,
        clan_leaderboard_path: str = ROUND_METRICS_CLAN_PATH,
        user_page_limit: int = ROUND_METRICS_USER_PAGE_LIMIT,
        clan_page_limit: int = ROUND_METRICS_CLAN_PAGE_LIMIT,
        round_scan_lookback: int = ROUND_GET_LAST_SCAN_LOOKBACK,
        timeout_seconds: int = ROUND_METRICS_TIMEOUT_SECONDS,
        max_retries: int = ROUND_METRICS_MAX_RETRIES,
        token_fetcher: Optional[Callable[[], Optional[str]]] = None,
        rate_controller: Optional[AdaptiveRateController] = None,
    ) -> None:
        self.bearer_token = bearer_token.strip()
        self.token_fetcher = token_fetcher
        self.limiter = limiter
        self.base_url = str(base_url or "").strip().rstrip("/")
        self.multiplier_url = self._build_url(multiplier_path)
        self.round_clan_leaderboard_url = self._build_url(round_clan_leaderboard_path)
        self.player_leaderboard_url = self._build_url(player_leaderboard_path)
        self.clan_leaderboard_url = self._build_url(clan_leaderboard_path)
        self.user_page_limit = max(1, min(50, int(user_page_limit)))
        self.clan_page_limit = max(1, min(50, int(clan_page_limit)))
        self.round_scan_lookback = max(1, int(round_scan_lookback))
        self.timeout_seconds = max(1, int(timeout_seconds))
        self.max_retries = max(1, int(max_retries))
        self.rate_controller = rate_controller
        self.session = requests.Session()
        self.headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "x-device-type": "desktop",
            "origin": "https://app.gomining.com",
            "referer": "https://app.gomining.com/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        }
        if self.bearer_token:
            self._set_bearer(self.bearer_token)

    def _build_url(self, path: str) -> str:
        s = str(path or "").strip()
        if not s:
            raise ValueError("GoMining round metrics path/url is empty")
        if s.startswith("http://") or s.startswith("https://"):
            return s
        if not self.base_url:
            raise ValueError("GOMINING_API_BASE_URL is required for relative round metrics paths")
        if not s.startswith("/"):
            s = "/" + s
        return f"{self.base_url}{s}"

    def _set_bearer(self, token: str) -> None:
        self.bearer_token = token.strip()
        self.headers["authorization"] = f"Bearer {self.bearer_token}"

    def _refresh_bearer(self, force: bool = False) -> bool:
        if not self.token_fetcher:
            return bool(self.bearer_token)
        if not force:
            cached = _shared_token_cached(self.token_fetcher)
            if cached and cached != self.bearer_token:
                self._set_bearer(cached)
                return True
            if self.bearer_token:
                return True
        tok = _shared_token_fetch(self.token_fetcher, force=force)
        if not tok:
            return bool(self.bearer_token)
        if tok != self.bearer_token:
            self._set_bearer(tok)
        return True

    def _request_json_with_retry(
        self,
        method: str,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_payload: Optional[Dict[str, Any]] = None,
        op: str,
        **ctx: Any,
    ) -> Optional[Dict[str, Any]]:
        self._refresh_bearer(force=False)
        delay_s = 0.6
        method_upper = str(method or "GET").upper()
        for attempt in range(1, self.max_retries + 1):
            self.limiter.wait_for_token(1.0)
            try:
                resp = self.session.request(
                    method_upper,
                    url,
                    headers=self.headers,
                    params=params,
                    json=json_payload,
                    timeout=self.timeout_seconds,
                )
            except Exception as e:
                if attempt >= self.max_retries:
                    log_warn("gomining_api.round_metrics_request_failed", op=op, attempt=attempt, err=repr(e), **ctx)
                    return None
                sleep_s = delay_s + random.uniform(0.0, 0.2)
                log_warn(
                    "gomining_api.round_metrics_request_retry",
                    op=op,
                    attempt=attempt,
                    sleep_s=round(sleep_s, 3),
                    err=repr(e),
                    **ctx,
                )
                time.sleep(sleep_s)
                delay_s = min(8.0, delay_s * 2.0)
                continue

            if resp.status_code == 200:
                if self.rate_controller is not None:
                    self.rate_controller.record_status(200)
                try:
                    data = resp.json()
                except Exception as e:
                    log_warn("gomining_api.round_metrics_invalid_json", op=op, attempt=attempt, err=repr(e), **ctx)
                    return None
                if isinstance(data, dict):
                    return data
                log_warn(
                    "gomining_api.round_metrics_invalid_json_type",
                    op=op,
                    attempt=attempt,
                    typ=type(data).__name__,
                    **ctx,
                )
                return None

            if resp.status_code == 401:
                if attempt < self.max_retries and self._refresh_bearer(force=True):
                    log_warn("gomining_api.round_metrics_unauthorized_retry", op=op, attempt=attempt, **ctx)
                    continue
                log_warn("gomining_api.round_metrics_unauthorized", op=op, attempt=attempt, **ctx)
                return None

            if resp.status_code == 403 and _is_jwt_expired_response(resp):
                if attempt < self.max_retries and self._refresh_bearer(force=True):
                    log_warn("gomining_api.round_metrics_forbidden_expired_retry", op=op, attempt=attempt, **ctx)
                    continue
                log_warn("gomining_api.round_metrics_forbidden_expired", op=op, attempt=attempt, **ctx)
                return None

            retryable = resp.status_code in {429, 500, 502, 503, 504}
            if self.rate_controller is not None:
                self.rate_controller.record_status(resp.status_code)
            if retryable and attempt < self.max_retries:
                retry_after = None
                try:
                    ra = resp.headers.get("Retry-After")
                    if ra:
                        retry_after = float(ra)
                except Exception:
                    retry_after = None
                sleep_s = max(delay_s, retry_after or 0.0) + random.uniform(0.0, 0.2)
                log_warn(
                    "gomining_api.round_metrics_http_retry",
                    op=op,
                    attempt=attempt,
                    status=resp.status_code,
                    sleep_s=round(sleep_s, 3),
                    **ctx,
                )
                time.sleep(sleep_s)
                delay_s = min(8.0, delay_s * 2.0)
                continue

            log_warn(
                "gomining_api.round_metrics_http_error",
                op=op,
                attempt=attempt,
                status=resp.status_code,
                body_preview=resp.text[:180],
                **ctx,
            )
            return None
        return None

    @staticmethod
    def _payload_data(body: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(body, dict):
            return None
        data = body.get("data")
        if isinstance(data, dict):
            return data
        return body

    @staticmethod
    def _payload_round_id(body: Any) -> Optional[int]:
        data = GoMiningRoundMetricsApiClient._payload_data(body)
        if not isinstance(data, dict):
            return None
        rid = safe_int(data.get("roundId"))
        if rid is None:
            rid = safe_int(data.get("id"))
        return rid

    @staticmethod
    def _payload_league_id(body: Any) -> Optional[int]:
        data = GoMiningRoundMetricsApiClient._payload_data(body)
        if not isinstance(data, dict):
            return None
        return safe_int(data.get("leagueId"))

    def _fetch_round_clan_leaderboard_by_round_id(
        self,
        league_id: int,
        round_id: int,
    ) -> Optional[Dict[str, Any]]:
        return self._request_json_with_retry(
            "POST",
            self.round_clan_leaderboard_url,
            json_payload={
                "roundId": int(round_id),
                "pagination": {"skip": 0, "limit": 1},
            },
            op="round_clan_leaderboard_by_round",
            league_id=league_id,
            round_id=round_id,
        )

    def _fetch_get_last_with_probe(
        self,
        requested_league_id: int,
        strict_league_validation: bool = ROUND_GET_LAST_STRICT_LEAGUE_VALIDATION,
    ) -> Tuple[Optional[Dict[str, Any]], bool]:
        primary = self._request_json_with_retry(
            "GET",
            self.multiplier_url,
            params={"leagueId": requested_league_id},
            op="round_get_last",
            league_id=requested_league_id,
        )
        if not isinstance(primary, dict):
            return None, True

        primary_league_id = self._payload_league_id(primary)
        if primary_league_id == requested_league_id:
            return primary, False

        primary_round_id = self._payload_round_id(primary)
        log_warn(
            "gomining_api.round_get_last_league_mismatch",
            requested_league_id=requested_league_id,
            payload_league_id=primary_league_id,
            strict=bool(strict_league_validation),
            round_id=primary_round_id,
            method="GET",
        )

        secondary = self._request_json_with_retry(
            "POST",
            self.multiplier_url,
            json_payload={"leagueId": requested_league_id},
            op="round_get_last_post_probe",
            league_id=requested_league_id,
        )
        if not isinstance(secondary, dict):
            log_warn(
                "gomining_api.round_get_last_post_probe_failed",
                requested_league_id=requested_league_id,
            )
            return primary, True

        secondary_league_id = self._payload_league_id(secondary)
        secondary_round_id = self._payload_round_id(secondary)
        if secondary_league_id == requested_league_id:
            log_info(
                "gomining_api.round_get_last_post_probe_resolved",
                requested_league_id=requested_league_id,
                round_id=secondary_round_id,
            )
            return secondary, False

        log_warn(
            "gomining_api.round_get_last_league_mismatch",
            requested_league_id=requested_league_id,
            payload_league_id=secondary_league_id,
            strict=bool(strict_league_validation),
            round_id=secondary_round_id,
            method="POST",
        )
        if primary_round_id is None:
            return secondary, True
        if secondary_round_id is None:
            return primary, True
        if secondary_round_id >= primary_round_id:
            return secondary, True
        return primary, True

    def _scan_closed_round_for_league(
        self,
        requested_league_id: int,
        start_round_id: int,
        lookback: int,
    ) -> Optional[int]:
        min_round_id = max(1, int(start_round_id) - max(1, int(lookback)))
        for rid in range(int(start_round_id), min_round_id - 1, -1):
            body = self._fetch_round_clan_leaderboard_by_round_id(requested_league_id, rid)
            if not isinstance(body, dict):
                continue
            data = self._payload_data(body)
            if not isinstance(data, dict):
                continue
            found_league_id = safe_int(data.get("leagueId"))
            found_round_id = safe_int(data.get("roundId"))
            if found_round_id is None:
                found_round_id = safe_int(data.get("id"))
            active_flag = data.get("active")
            if found_league_id == requested_league_id and found_round_id is not None and active_flag is False:
                log_info(
                    "gomining_api.round_scan_hit",
                    league_id=requested_league_id,
                    requested_round_id=rid,
                    resolved_round_id=found_round_id,
                )
                return found_round_id
        return None

    def _compute_round_duration_sec(self, round_data: Dict[str, Any], ended_at_iso: str) -> Optional[int]:
        ended_at_norm = to_iso_utc(ended_at_iso)
        ended_dt: Optional[datetime] = None
        if ended_at_norm:
            try:
                ended_dt = datetime.fromisoformat(str(ended_at_norm).replace("Z", "+00:00"))
                if ended_dt.tzinfo is None:
                    ended_dt = ended_dt.replace(tzinfo=timezone.utc)
                ended_dt = ended_dt.astimezone(timezone.utc)
            except Exception:
                ended_dt = None

        started_at_iso = to_iso_utc(round_data.get("startedAt"))
        if ended_dt is not None and started_at_iso:
            try:
                started_dt = datetime.fromisoformat(str(started_at_iso).replace("Z", "+00:00"))
                if started_dt.tzinfo is None:
                    started_dt = started_dt.replace(tzinfo=timezone.utc)
                started_dt = started_dt.astimezone(timezone.utc)
                duration = int(round((ended_dt - started_dt).total_seconds()))
                if duration >= 0:
                    return duration
                return None
            except Exception:
                pass

        return safe_int(round_data.get("roundTime"))

    def _fetch_round_metrics_for_round_id(
        self,
        requested_league_id: int,
        target_round_id: int,
    ) -> Optional[Dict[str, Any]]:
        round_body = self._fetch_round_clan_leaderboard_by_round_id(requested_league_id, int(target_round_id))
        if not isinstance(round_body, dict):
            return None
        round_data = self._payload_data(round_body)
        if not isinstance(round_data, dict):
            log_warn(
                "gomining_api.round_clan_leaderboard_bad_shape",
                league_id=requested_league_id,
                target_round_id=target_round_id,
            )
            return None

        resolved_round_id = safe_int(round_data.get("roundId"))
        if resolved_round_id is None:
            resolved_round_id = safe_int(round_data.get("id"))
        if resolved_round_id != target_round_id:
            log_warn(
                "gomining_api.round_clan_round_mismatch",
                league_id=requested_league_id,
                target_round_id=target_round_id,
                payload_round_id=resolved_round_id,
            )
            return None

        payload_league_id = safe_int(round_data.get("leagueId"))
        if payload_league_id != requested_league_id:
            log_warn(
                "gomining_api.round_clan_league_mismatch",
                league_id=requested_league_id,
                target_round_id=target_round_id,
                payload_league_id=payload_league_id,
            )
            return None

        active_flag = round_data.get("active")
        if active_flag is not False:
            log_warn(
                "gomining_api.round_clan_not_finalized",
                league_id=requested_league_id,
                round_id=target_round_id,
                active=active_flag,
            )
            return None

        ended_at = to_iso_utc(round_data.get("endedAt"))
        if not ended_at:
            log_warn(
                "gomining_api.round_clan_missing_ended_at",
                league_id=requested_league_id,
                round_id=target_round_id,
            )
            return None
        round_duration_sec = self._compute_round_duration_sec(round_data, ended_at)
        if round_duration_sec is None:
            log_warn(
                "gomining_api.round_clan_missing_duration",
                league_id=requested_league_id,
                round_id=target_round_id,
            )
            return None

        calculated_at = _to_api_calculated_at(ended_at)
        snapshot_ts = utc_now_iso()

        user_payload = {
            "pagination": {"skip": 0, "limit": self.user_page_limit},
            "calculatedAt": calculated_at,
            "leagueId": requested_league_id,
        }
        user_body = self._request_json_with_retry(
            "POST",
            self.player_leaderboard_url,
            json_payload=user_payload,
            op="user_leaderboard_index",
            league_id=requested_league_id,
            round_id=target_round_id,
        )
        if not isinstance(user_body, dict):
            return None
        user_data = self._payload_data(user_body)
        if not isinstance(user_data, dict):
            log_warn("gomining_api.user_leaderboard_bad_shape", league_id=requested_league_id, round_id=target_round_id)
            return None

        clan_payload = {
            "pagination": {"skip": 0, "limit": self.clan_page_limit},
            "calculatedAt": calculated_at,
            "leagueId": requested_league_id,
        }
        clan_body = self._request_json_with_retry(
            "POST",
            self.clan_leaderboard_url,
            json_payload=clan_payload,
            op="clan_leaderboard_index_v2",
            league_id=requested_league_id,
            round_id=target_round_id,
        )
        if not isinstance(clan_body, dict):
            return None
        clan_data = self._payload_data(clan_body)
        if not isinstance(clan_data, dict):
            log_warn("gomining_api.clan_leaderboard_bad_shape", league_id=requested_league_id, round_id=target_round_id)
            return None

        gmt_fund = safe_float(user_data.get("gmtFund"))
        user_blocks_mined = safe_int(user_data.get("totalMinedBlocks"))
        clan_blocks_mined = safe_int(clan_data.get("totalMinedBlocks"))
        blocks_mined = user_blocks_mined if user_blocks_mined is not None else clan_blocks_mined

        gmt_per_block: Optional[float] = None
        if gmt_fund is not None and blocks_mined is not None and blocks_mined > 0:
            gmt_per_block = gmt_fund / float(blocks_mined)

        return {
            "snapshot_ts": snapshot_ts,
            "league_id": requested_league_id,
            "round_id": target_round_id,
            "block_number": safe_int(round_data.get("blockNumber")),
            "multiplier": safe_float(round_data.get("multiplier")),
            "gmt_fund": gmt_fund,
            "gmt_per_block": gmt_per_block,
            "league_th": safe_float(clan_data.get("totalPower")),
            "blocks_mined": blocks_mined,
            "efficiency_league": safe_float(clan_data.get("weightedEnergyEfficiencyPerTh")),
            "ended_at": ended_at,
            "round_duration_sec": round_duration_sec,
        }

    def fetch_round_metrics_by_round_id(
        self,
        league_id: int,
        round_id: int,
    ) -> Optional[Dict[str, Any]]:
        requested_league_id = int(league_id)
        target_round_id = safe_int(round_id)
        if target_round_id is None or target_round_id <= 0:
            log_warn(
                "gomining_api.round_metrics_invalid_round_id",
                league_id=requested_league_id,
                round_id=round_id,
            )
            return None
        return self._fetch_round_metrics_for_round_id(requested_league_id, target_round_id)

    def fetch_round_metrics_triplet(
        self,
        league_id: int,
        strict_league_validation: bool = ROUND_GET_LAST_STRICT_LEAGUE_VALIDATION,
    ) -> Optional[Dict[str, Any]]:
        requested_league_id = int(league_id)
        _ = strict_league_validation

        get_last_body, unresolved_mismatch = self._fetch_get_last_with_probe(
            requested_league_id,
            strict_league_validation=strict_league_validation,
        )
        if not isinstance(get_last_body, dict):
            return None
        get_last_data = self._payload_data(get_last_body)
        if not isinstance(get_last_data, dict):
            log_warn("gomining_api.round_get_last_bad_shape", league_id=requested_league_id)
            return None

        previous_round_id = safe_int(get_last_data.get("previousRoundId"))
        if previous_round_id is not None and previous_round_id <= 0:
            previous_round_id = None

        target_round_id: Optional[int]
        if previous_round_id is not None and not unresolved_mismatch:
            target_round_id = previous_round_id
        else:
            start_round_id = self._payload_round_id(get_last_body)
            if start_round_id is None or start_round_id <= 0:
                log_warn(
                    "gomining_api.round_get_last_scan_missing_start_round",
                    league_id=requested_league_id,
                    previous_round_id=previous_round_id,
                    unresolved_mismatch=bool(unresolved_mismatch),
                )
                return None
            target_round_id = self._scan_closed_round_for_league(
                requested_league_id,
                start_round_id=start_round_id,
                lookback=self.round_scan_lookback,
            )
            if target_round_id is None:
                log_warn(
                    "gomining_api.round_get_last_scan_failed",
                    league_id=requested_league_id,
                    start_round_id=start_round_id,
                    lookback=self.round_scan_lookback,
                    previous_round_id=previous_round_id,
                    unresolved_mismatch=bool(unresolved_mismatch),
                )
                return None
            log_info(
                "gomining_api.round_get_last_scan_resolved",
                league_id=requested_league_id,
                start_round_id=start_round_id,
                target_round_id=target_round_id,
                lookback=self.round_scan_lookback,
                unresolved_mismatch=bool(unresolved_mismatch),
            )

        return self._fetch_round_metrics_for_round_id(requested_league_id, target_round_id)


class GoMiningRoundAbilityApiClient:
    def __init__(
        self,
        bearer_token: str,
        limiter: TokenBucket,
        user_leaderboard_url: str = ROUND_USER_LEADERBOARD_API_URL,
        page_limit: int = ROUND_API_PAGE_LIMIT,
        timeout_seconds: int = ROUND_API_TIMEOUT_SECONDS,
        max_retries: int = ROUND_API_MAX_RETRIES,
        token_fetcher: Optional[Callable[[], Optional[str]]] = None,
        rate_controller: Optional[AdaptiveRateController] = None,
    ) -> None:
        self.bearer_token = bearer_token.strip()
        self.token_fetcher = token_fetcher
        self.limiter = limiter
        self.user_leaderboard_url = user_leaderboard_url
        self.page_limit = max(1, min(50, int(page_limit)))
        self.timeout_seconds = max(1, int(timeout_seconds))
        self.max_retries = max(1, int(max_retries))
        self.rate_controller = rate_controller
        self.session = requests.Session()
        self.headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "x-device-type": "desktop",
            "origin": "https://app.gomining.com",
            "referer": "https://app.gomining.com/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        }
        if self.bearer_token:
            self._set_bearer(self.bearer_token)
        self.round_ability_users_cache: Dict[int, Dict[str, List[Dict[str, Any]]]] = {}
        # Cache aggregate ability counts per round_id to avoid re-fetching across poll cycles.
        self.round_counts_cache: Dict[int, Dict[str, int]] = {}
        # Cache excluded user's per-round ability usage for audit output in sheet.
        self.round_excluded_user_boosts_cache: Dict[int, Dict[str, int]] = {}

    def _set_bearer(self, token: str) -> None:
        self.bearer_token = token.strip()
        self.headers["authorization"] = f"Bearer {self.bearer_token}"

    def _refresh_bearer(self, force: bool = False) -> bool:
        if not self.token_fetcher:
            return bool(self.bearer_token)
        if not force:
            cached = _shared_token_cached(self.token_fetcher)
            if cached and cached != self.bearer_token:
                self._set_bearer(cached)
                return True
            if self.bearer_token:
                return True
        tok = _shared_token_fetch(self.token_fetcher, force=force)
        if not tok:
            return bool(self.bearer_token)
        if tok != self.bearer_token:
            self._set_bearer(tok)
        return True

    def _post_json_with_retry(self, payload: Dict[str, Any], **ctx: Any) -> Optional[Dict[str, Any]]:
        self._refresh_bearer(force=False)
        delay_s = 0.6
        for attempt in range(1, self.max_retries + 1):
            self.limiter.wait_for_token(1.0)
            try:
                resp = self.session.post(
                    self.user_leaderboard_url,
                    headers=self.headers,
                    json=payload,
                    timeout=self.timeout_seconds,
                )
            except Exception as e:
                if attempt >= self.max_retries:
                    log_warn("gomining_api.round_user_request_failed", attempt=attempt, err=repr(e), **ctx)
                    return None
                sleep_s = delay_s + random.uniform(0.0, 0.2)
                log_warn(
                    "gomining_api.round_user_request_retry",
                    attempt=attempt,
                    sleep_s=round(sleep_s, 3),
                    err=repr(e),
                    **ctx,
                )
                time.sleep(sleep_s)
                delay_s = min(8.0, delay_s * 2.0)
                continue

            if resp.status_code == 200:
                if self.rate_controller is not None:
                    self.rate_controller.record_status(200)
                try:
                    data = resp.json()
                    if isinstance(data, dict):
                        return data
                    log_warn("gomining_api.round_user_invalid_json_type", attempt=attempt, typ=type(data).__name__, **ctx)
                    return None
                except Exception as e:
                    log_warn("gomining_api.round_user_invalid_json", attempt=attempt, err=repr(e), **ctx)
                    return None

            if resp.status_code == 401:
                if attempt < self.max_retries and self._refresh_bearer(force=True):
                    log_warn("gomining_api.round_user_unauthorized_retry", attempt=attempt, **ctx)
                    continue
                log_warn("gomining_api.round_user_unauthorized", attempt=attempt, **ctx)
                return None
            if resp.status_code == 403 and _is_jwt_expired_response(resp):
                if attempt < self.max_retries and self._refresh_bearer(force=True):
                    log_warn("gomining_api.round_user_forbidden_expired_retry", attempt=attempt, **ctx)
                    continue
                log_warn("gomining_api.round_user_forbidden_expired", attempt=attempt, **ctx)
                return None

            retryable = resp.status_code in {429, 500, 502, 503, 504}
            if self.rate_controller is not None:
                self.rate_controller.record_status(resp.status_code)
            if retryable and attempt < self.max_retries:
                retry_after = None
                try:
                    ra = resp.headers.get("Retry-After")
                    if ra:
                        retry_after = float(ra)
                except Exception:
                    retry_after = None
                sleep_s = max(delay_s, retry_after or 0.0) + random.uniform(0.0, 0.2)
                log_warn(
                    "gomining_api.round_user_http_retry",
                    attempt=attempt,
                    status=resp.status_code,
                    sleep_s=round(sleep_s, 3),
                    **ctx,
                )
                time.sleep(sleep_s)
                delay_s = min(8.0, delay_s * 2.0)
                continue

            log_warn(
                "gomining_api.round_user_http_error",
                attempt=attempt,
                status=resp.status_code,
                body_preview=resp.text[:180],
                **ctx,
            )
            return None
        return None

    def fetch_round_ability_counts(
        self,
        round_id: int,
        expected_league_id: Optional[int] = None,
        progress_hook: Optional[Callable[[], Any]] = None,
    ) -> Optional[Dict[str, int]]:
        rid = int(round_id)
        # Return cached counts only when both caches are consistent.
        # If round_ability_users_cache is missing (e.g. trimmed), we must re-fetch
        # to repopulate per-user data needed for exact Power Up GMT calculation.
        cached_counts = self.round_counts_cache.get(rid)
        if (
            cached_counts is not None
            and rid in self.round_ability_users_cache
            and rid in self.round_excluded_user_boosts_cache
        ):
            return dict(cached_counts)
        skip = 0
        fetched = 0
        total_count: Optional[int] = None
        page_no = 0
        max_pages = 10000
        counts: Dict[str, int] = {}
        excluded_user_counts: Dict[str, int] = {}
        tracked_users_by_ability: Dict[str, Dict[Tuple[int, Optional[int]], Dict[str, Any]]] = {
            POWER_UP_ABILITY_ID: {},
            CLAN_POWER_UP_ABILITY_ID: {},
        }

        while True:
            if progress_hook is not None:
                try:
                    progress_hook()
                except Exception as e:
                    log_warn("gomining_api.round_user_progress_hook_failed", round_id=rid, skip=skip, err=repr(e))
            payload = {"roundId": rid, "pagination": {"limit": self.page_limit, "skip": skip, "count": 0}}
            body = self._post_json_with_retry(payload, round_id=rid, skip=skip)
            if body is None:
                return None

            data = body.get("data")
            if not isinstance(data, dict):
                log_warn("gomining_api.round_user_bad_shape", round_id=rid, skip=skip)
                return None

            payload_round_id = safe_int(data.get("roundId"))
            if payload_round_id != rid:
                log_warn(
                    "gomining_api.round_user_round_mismatch",
                    requested_round_id=rid,
                    payload_round_id=payload_round_id,
                    skip=skip,
                )
                return None

            payload_league_id = safe_int(data.get("leagueId"))
            if expected_league_id is not None and payload_league_id is not None and payload_league_id != expected_league_id:
                log_warn(
                    "gomining_api.round_user_league_mismatch",
                    requested_round_id=rid,
                    expected_league_id=expected_league_id,
                    payload_league_id=payload_league_id,
                    skip=skip,
                )
                return None

            participants = data.get("participants")
            if not isinstance(participants, list):
                log_warn("gomining_api.round_user_participants_bad_shape", round_id=rid, skip=skip)
                return None

            if total_count is None:
                total_count = safe_int(data.get("count"))
                if total_count is None or total_count < 0:
                    log_warn("gomining_api.round_user_count_invalid", round_id=rid, count=data.get("count"), skip=skip)
                    return None
                if total_count == 0:
                    return {}

            for participant in participants:
                if not isinstance(participant, dict):
                    continue
                participant_user_id = None
                participant_avatar_url = ""
                participant_alias = ""
                user_raw = participant.get("user")
                if isinstance(user_raw, dict):
                    participant_user_id = safe_int(user_raw.get("id"))
                    participant_avatar_url = str(user_raw.get("avatar") or "")
                    participant_alias = str(user_raw.get("alias") or "")
                if participant_user_id is None:
                    participant_user_id = safe_int(participant.get("userId"))
                participant_clan_id = None
                clan_raw = participant.get("clan")
                if isinstance(clan_raw, dict):
                    participant_clan_id = safe_int(clan_raw.get("id"))
                if participant_clan_id is None:
                    participant_clan_id = safe_int(participant.get("clanId"))
                used = participant.get("usedAbilities") or []
                if not isinstance(used, list):
                    continue
                for item in used:
                    if not isinstance(item, dict):
                        continue
                    ability_id = str(item.get("nftGameAbilityId") or "").strip()
                    if not ability_id:
                        continue
                    cnt = safe_int(item.get("count")) or 0
                    if cnt <= 0:
                        continue
                    if participant_user_id == EXCLUDED_BOOST_USER_ID:
                        excluded_user_counts[ability_id] = excluded_user_counts.get(ability_id, 0) + cnt
                        continue
                    counts[ability_id] = counts.get(ability_id, 0) + cnt
                    if ability_id in tracked_users_by_ability and participant_user_id is not None:
                        ability_users = tracked_users_by_ability[ability_id]
                        key = (participant_user_id, participant_clan_id)
                        bucket = ability_users.get(key)
                        if bucket is None:
                            bucket = {"count": 0, "avatar_url": participant_avatar_url, "alias": participant_alias}
                            ability_users[key] = bucket
                        bucket["count"] = int(bucket.get("count") or 0) + cnt
                        if (not str(bucket.get("avatar_url") or "").strip()) and participant_avatar_url:
                            bucket["avatar_url"] = participant_avatar_url
                        if (not str(bucket.get("alias") or "").strip()) and participant_alias:
                            bucket["alias"] = participant_alias

            fetched += len(participants)
            if len(participants) < self.page_limit:
                break
            if total_count is not None and fetched >= total_count:
                break

            skip += self.page_limit
            page_no += 1
            if page_no >= max_pages:
                log_warn("gomining_api.round_user_pagination_guard", round_id=rid, pages=page_no)
                return None

        if total_count is not None and fetched < total_count:
            log_warn(
                "gomining_api.round_user_incomplete",
                round_id=rid,
                fetched=fetched,
                expected=total_count,
                page_limit=self.page_limit,
            )
            return None
        users_payload_by_ability: Dict[str, List[Dict[str, Any]]] = {}
        for ability_id, users_by_key in tracked_users_by_ability.items():
            users_payload: List[Dict[str, Any]] = []
            for (user_id, clan_id), meta in users_by_key.items():
                users_payload.append(
                    {
                        "user_id": user_id,
                        "clan_id": clan_id,
                        "count": safe_int(meta.get("count")) or 0,
                        "avatar_url": str(meta.get("avatar_url") or ""),
                        "alias": str(meta.get("alias") or ""),
                    }
                )
            users_payload_by_ability[ability_id] = users_payload

        self.round_ability_users_cache[rid] = users_payload_by_ability
        if len(self.round_ability_users_cache) > 512:
            # Keep cache bounded for long-running process.
            for old_rid in sorted(self.round_ability_users_cache.keys())[:-256]:
                self.round_ability_users_cache.pop(old_rid, None)

        self.round_counts_cache[rid] = dict(counts)
        if len(self.round_counts_cache) > 512:
            for old_rid in sorted(self.round_counts_cache.keys())[:-256]:
                self.round_counts_cache.pop(old_rid, None)
        self.round_excluded_user_boosts_cache[rid] = dict(excluded_user_counts)
        if len(self.round_excluded_user_boosts_cache) > 512:
            for old_rid in sorted(self.round_excluded_user_boosts_cache.keys())[:-256]:
                self.round_excluded_user_boosts_cache.pop(old_rid, None)
        return counts

    def get_cached_ability_users_for_round(self, round_id: int, ability_id: str) -> List[Dict[str, Any]]:
        rid = int(round_id)
        aid = str(ability_id or "").strip()
        if not aid:
            return []
        cached = self.round_ability_users_cache.get(rid)
        if not isinstance(cached, dict):
            return []
        users = cached.get(aid)
        if not isinstance(users, list):
            return []
        return [u for u in users if isinstance(u, dict)]

    def get_cached_power_up_users_for_round(self, round_id: int) -> List[Dict[str, Any]]:
        return self.get_cached_ability_users_for_round(round_id, POWER_UP_ABILITY_ID)

    def get_cached_excluded_user_boosts_for_round(self, round_id: int) -> Dict[str, int]:
        rid = int(round_id)
        cached = self.round_excluded_user_boosts_cache.get(rid)
        if not isinstance(cached, dict):
            return {}
        out: Dict[str, int] = {}
        for ability_id, cnt in cached.items():
            aid = str(ability_id or "").strip()
            c = safe_int(cnt) or 0
            if not aid or c <= 0:
                continue
            out[aid] = c
        return out


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


class TokenBucket:
    def __init__(self, per_minute: int, name: str = "bucket") -> None:
        self.name = name
        self.capacity = max(1, int(per_minute))
        self.tokens = float(self.capacity)
        self.refill = float(self.capacity) / 60.0
        self.last = time.monotonic()
        self.total_acquired = 0
        self.total_wait_s = 0.0
        self.wait_events = 0
        self._lock = threading.Lock()

    def wait_for_token(self, amount: float = 1.0) -> None:
        start_wait = time.monotonic()
        local_wait_events = 0
        while True:
            with self._lock:
                now = time.monotonic()
                dt = now - self.last
                self.last = now
                self.tokens = min(self.capacity, self.tokens + dt * self.refill)
                if self.tokens >= amount:
                    self.tokens -= amount
                    self.total_acquired += int(amount)
                    waited = time.monotonic() - start_wait
                    if waited > 0:
                        self.total_wait_s += waited
                        self.wait_events += local_wait_events
                    if DEBUG_VERBOSE and (waited > 0.001 or LOGGER.isEnabledFor(logging.DEBUG)):
                        log_debug(
                            "rate.acquire",
                            bucket=self.name,
                            amount=amount,
                            waited_s=f"{waited:.4f}",
                            tokens_left=f"{self.tokens:.3f}",
                            total_acquired=self.total_acquired,
                            total_wait_s=f"{self.total_wait_s:.3f}",
                        )
                    return
                need = amount - self.tokens
            sleep_s = max(0.01, need / self.refill)
            local_wait_events += 1
            if DEBUG_VERBOSE and LOGGER.isEnabledFor(logging.DEBUG):
                log_debug(
                    "rate.wait",
                    bucket=self.name,
                    amount=amount,
                    tokens_now=f"{self.tokens:.3f}",
                    need=f"{need:.3f}",
                    sleep_s=f"{sleep_s:.3f}",
                )
            time.sleep(sleep_s)

    def set_rate_per_minute(self, per_minute: int) -> None:
        new_cap = max(1, int(per_minute))
        with self._lock:
            now = time.monotonic()
            dt = now - self.last
            self.last = now
            self.tokens = min(self.capacity, self.tokens + dt * self.refill)
            self.capacity = new_cap
            self.refill = float(new_cap) / 60.0
            self.tokens = min(float(self.capacity), self.tokens)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "name": self.name,
                "capacity_per_min": self.capacity,
                "tokens_now": round(self.tokens, 3),
                "total_acquired": self.total_acquired,
                "total_wait_s": round(self.total_wait_s, 3),
                "wait_events": self.wait_events,
            }


class AdaptiveRateController:
    def __init__(self, limiter: TokenBucket, min_rpm: int, max_rpm: int) -> None:
        self.limiter = limiter
        self.min_rpm = max(1, int(min_rpm))
        self.max_rpm = max(self.min_rpm, int(max_rpm))
        self.current_rpm = min(self.max_rpm, max(self.min_rpm, int(limiter.capacity)))
        self._retryable_streak = 0
        self._success_streak = 0
        self._lock = threading.Lock()

    def record_status(self, status_code: Optional[int]) -> None:
        if not ADAPTIVE_RPM_ENABLE:
            return
        with self._lock:
            if status_code is not None and int(status_code) in {429, 500, 502, 503, 504}:
                self._retryable_streak += 1
                self._success_streak = 0
                if self._retryable_streak >= 5:
                    new_rpm = max(self.min_rpm, int(round(self.current_rpm * 0.9)))
                    if new_rpm < self.current_rpm:
                        prev = self.current_rpm
                        self.current_rpm = new_rpm
                        self.limiter.set_rate_per_minute(new_rpm)
                        log_warn("rate.adaptive_down", prev_rpm=prev, new_rpm=new_rpm, status=status_code, streak=self._retryable_streak)
                    self._retryable_streak = 0
                return

            if status_code == 200:
                self._success_streak += 1
                self._retryable_streak = 0
                if self._success_streak >= 120:
                    new_rpm = min(self.max_rpm, int(round(self.current_rpm * 1.05)))
                    if new_rpm > self.current_rpm:
                        prev = self.current_rpm
                        self.current_rpm = new_rpm
                        self.limiter.set_rate_per_minute(new_rpm)
                        log_info("rate.adaptive_up", prev_rpm=prev, new_rpm=new_rpm, success_streak=self._success_streak)
                    self._success_streak = 0


def _is_pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError as e:
        err = getattr(e, "errno", None)
        winerr = getattr(e, "winerror", None)
        if err == errno.ESRCH:
            return False
        # Windows commonly reports dead PID probes as winerror 87 / EINVAL.
        if winerr == 87 or err == errno.EINVAL:
            return False
        if err == errno.EPERM:
            return True
        # Unknown platform-specific error; default to "running" to avoid false unlock.
        return True
    return True


class SingleInstanceLock:
    def __init__(self, path: str) -> None:
        self.path = path
        self.acquired = False

    @staticmethod
    def _read_lock_pid(path: str) -> Optional[int]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                txt = f.read()
        except Exception:
            return None
        m = re.search(r"\bpid=(\d+)\b", txt)
        if not m:
            return None
        try:
            return int(m.group(1))
        except Exception:
            return None

    def acquire(self) -> None:
        p = Path(self.path)
        for attempt in (1, 2):
            try:
                fd = os.open(str(p), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    f.write(f"pid={os.getpid()} ts={utc_now_iso()}\n")
                self.acquired = True
                return
            except FileExistsError:
                if attempt >= 2:
                    break
                existing_pid = self._read_lock_pid(str(p))
                # Conservative stale-lock cleanup: only delete when lock owner is clearly dead
                # (or lock was left by the current process).
                if existing_pid is None:
                    break
                if existing_pid != os.getpid() and _is_pid_running(existing_pid):
                    break
                try:
                    os.remove(self.path)
                    log_warn("lockfile.stale_removed", path=self.path, stale_pid=existing_pid)
                except FileNotFoundError:
                    pass
                except Exception as e:
                    log_warn("lockfile.stale_remove_failed", path=self.path, stale_pid=existing_pid, err=repr(e))
                    break
        raise RuntimeError(f"Lock file exists: {self.path}")

    def release(self) -> None:
        if not self.acquired:
            return
        try:
            os.remove(self.path)
        except Exception:
            pass


_DB_DEPRECATION_LOGGED = False


def _warn_db_deprecated(context: str = "runtime") -> None:
    global _DB_DEPRECATION_LOGGED
    if _DB_DEPRECATION_LOGGED:
        return
    _DB_DEPRECATION_LOGGED = True
    log_warn("sync.db_dependency_removed", context=context, note="PostgreSQL path is disabled; API-only mode is active.")


class DBClient:
    """
    Deprecated compatibility shim.
    Kept only so legacy scripts importing main.DBClient do not break.
    """

    def connect(self) -> None:
        _warn_db_deprecated("DBClient.connect")

    def close(self) -> None:
        return


class StateStore:
    def __init__(self, path: str) -> None:
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

    def close(self) -> None:
        self.conn.close()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS sheet_state (
              sheet_id INTEGER PRIMARY KEY,
              sheet_title TEXT NOT NULL,
              league_id INTEGER,
              last_synced_round INTEGER,
              price_cutover_round INTEGER,
              layout_sig TEXT,
              updated_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS round_row_map (
              sheet_id INTEGER NOT NULL,
              round_id INTEGER NOT NULL,
              row_idx INTEGER NOT NULL,
              checksum TEXT NOT NULL,
              finalized INTEGER NOT NULL DEFAULT 0,
              updated_at REAL NOT NULL,
              PRIMARY KEY (sheet_id, round_id)
            );

            CREATE TABLE IF NOT EXISTS pending_ops (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              sheet_id INTEGER NOT NULL,
              op_type TEXT NOT NULL,
              round_id INTEGER NOT NULL,
              checksum TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              retry_count INTEGER NOT NULL DEFAULT 0,
              next_attempt_ts REAL NOT NULL,
              created_at REAL NOT NULL
            );

            CREATE UNIQUE INDEX IF NOT EXISTS ux_pending_op_round ON pending_ops(sheet_id, op_type, round_id);
            CREATE INDEX IF NOT EXISTS ix_pending_due ON pending_ops(next_attempt_ts, id);
            CREATE INDEX IF NOT EXISTS ix_rowmap ON round_row_map(sheet_id, round_id DESC);

            CREATE TABLE IF NOT EXISTS api_round_cache (
              league_id INTEGER NOT NULL,
              round_id INTEGER NOT NULL,
              snapshot_ts TEXT,
              block_number INTEGER,
              multiplier REAL,
              gmt_fund REAL,
              gmt_per_block REAL,
              league_th REAL,
              blocks_mined INTEGER,
              efficiency_league REAL,
              ended_at TEXT,
              round_duration_sec INTEGER,
              source TEXT NOT NULL DEFAULT 'api_triplet',
              updated_at REAL NOT NULL,
              PRIMARY KEY (league_id, round_id)
            );
            CREATE INDEX IF NOT EXISTS ix_api_round_cache_lookup
              ON api_round_cache(league_id, round_id DESC);

            CREATE TABLE IF NOT EXISTS round_processing_state (
              league_id INTEGER NOT NULL,
              round_id INTEGER NOT NULL,
              metrics_status TEXT NOT NULL DEFAULT 'pending',
              abilities_status TEXT NOT NULL DEFAULT 'pending',
              sheet_status TEXT NOT NULL DEFAULT 'pending',
              last_error TEXT,
              retry_after_ts REAL NOT NULL DEFAULT 0,
              attempt_count INTEGER NOT NULL DEFAULT 0,
              updated_at REAL NOT NULL,
              PRIMARY KEY (league_id, round_id)
            );
            CREATE INDEX IF NOT EXISTS ix_round_processing_retry
              ON round_processing_state(retry_after_ts, league_id, round_id);
            """
        )
        self._ensure_sheet_state_columns()
        self.conn.commit()

    def _ensure_sheet_state_columns(self) -> None:
        rows = self.conn.execute("PRAGMA table_info(sheet_state)").fetchall()
        cols = {str(r["name"]) for r in rows}
        if "price_cutover_round" not in cols:
            self.conn.execute("ALTER TABLE sheet_state ADD COLUMN price_cutover_round INTEGER")

    def get_sheet_state(self, sheet_id: int) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT sheet_id,sheet_title,league_id,last_synced_round,price_cutover_round,layout_sig "
            "FROM sheet_state WHERE sheet_id=?",
            (sheet_id,),
        ).fetchone()
        return dict(row) if row else None

    def upsert_sheet_meta(self, sheet_id: int, sheet_title: str, league_id: Optional[int], layout_sig: Optional[str] = None) -> None:
        now = time.time()
        self.conn.execute(
            """
            INSERT INTO sheet_state(sheet_id,sheet_title,league_id,last_synced_round,price_cutover_round,layout_sig,updated_at)
            VALUES(?,?,?,NULL,NULL,?,?)
            ON CONFLICT(sheet_id) DO UPDATE SET
              sheet_title=excluded.sheet_title,
              league_id=excluded.league_id,
              layout_sig=COALESCE(excluded.layout_sig, sheet_state.layout_sig),
              updated_at=excluded.updated_at
            """,
            (sheet_id, sheet_title, league_id, layout_sig, now),
        )
        self.conn.commit()

    def get_last_synced_round(self, sheet_id: int) -> Optional[int]:
        row = self.conn.execute("SELECT last_synced_round FROM sheet_state WHERE sheet_id=?", (sheet_id,)).fetchone()
        return safe_int(row[0]) if row else None

    def set_last_synced_round(self, sheet_id: int, round_id: int) -> None:
        cur = self.get_last_synced_round(sheet_id)
        if cur is not None and round_id <= cur:
            return
        self.conn.execute(
            "UPDATE sheet_state SET last_synced_round=?, updated_at=? WHERE sheet_id=?",
            (round_id, time.time(), sheet_id),
        )
        self.conn.commit()

    def get_price_cutover_round(self, sheet_id: int) -> Optional[int]:
        row = self.conn.execute("SELECT price_cutover_round FROM sheet_state WHERE sheet_id=?", (sheet_id,)).fetchone()
        return safe_int(row[0]) if row else None

    def set_price_cutover_round(self, sheet_id: int, round_id: int) -> None:
        cur = self.get_price_cutover_round(sheet_id)
        if cur is not None:
            return
        self.conn.execute(
            "UPDATE sheet_state SET price_cutover_round=?, updated_at=? WHERE sheet_id=?",
            (round_id, time.time(), sheet_id),
        )
        self.conn.commit()

    def upsert_round_processing_state(
        self,
        league_id: int,
        round_id: int,
        *,
        metrics_status: Optional[str] = None,
        abilities_status: Optional[str] = None,
        sheet_status: Optional[str] = None,
        last_error: Optional[str] = None,
        retry_after_ts: Optional[float] = None,
        attempt_count: Optional[int] = None,
    ) -> None:
        lid = safe_int(league_id)
        rid = safe_int(round_id)
        if lid is None or rid is None:
            return
        prev = self.get_round_processing_state(lid, rid) or {}
        now = time.time()
        self.conn.execute(
            """
            INSERT INTO round_processing_state(
              league_id,round_id,metrics_status,abilities_status,sheet_status,last_error,retry_after_ts,attempt_count,updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(league_id,round_id) DO UPDATE SET
              metrics_status=excluded.metrics_status,
              abilities_status=excluded.abilities_status,
              sheet_status=excluded.sheet_status,
              last_error=excluded.last_error,
              retry_after_ts=excluded.retry_after_ts,
              attempt_count=excluded.attempt_count,
              updated_at=excluded.updated_at
            """,
            (
                lid,
                rid,
                str(metrics_status or prev.get("metrics_status") or "pending"),
                str(abilities_status or prev.get("abilities_status") or "pending"),
                str(sheet_status or prev.get("sheet_status") or "pending"),
                (str(last_error)[:4000] if last_error else prev.get("last_error")),
                float(retry_after_ts if retry_after_ts is not None else prev.get("retry_after_ts") or 0.0),
                int(attempt_count if attempt_count is not None else prev.get("attempt_count") or 0),
                now,
            ),
        )
        self.conn.commit()

    def get_round_processing_state(self, league_id: int, round_id: int) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            """
            SELECT league_id,round_id,metrics_status,abilities_status,sheet_status,last_error,retry_after_ts,attempt_count,updated_at
            FROM round_processing_state
            WHERE league_id=? AND round_id=?
            """,
            (league_id, round_id),
        ).fetchone()
        return dict(row) if row else None

    def list_retryable_round_states(self, league_id: int, limit: int = 200) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT league_id,round_id,metrics_status,abilities_status,sheet_status,last_error,retry_after_ts,attempt_count,updated_at
            FROM round_processing_state
            WHERE league_id=?
              AND retry_after_ts<=?
              AND (metrics_status!='ok' OR abilities_status!='ok' OR sheet_status!='ok')
            ORDER BY round_id ASC
            LIMIT ?
            """,
            (league_id, time.time(), max(1, int(limit))),
        ).fetchall()
        return [dict(r) for r in rows]

    def count_missing_rows_for_range(self, sheet_id: int, league_id: int, from_round: int, to_round: int) -> int:
        row = self.conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM api_round_cache a
            LEFT JOIN round_row_map m
              ON m.sheet_id=? AND m.round_id=a.round_id
            WHERE a.league_id=?
              AND a.round_id>=?
              AND a.round_id<=?
              AND a.ended_at IS NOT NULL
              AND m.round_id IS NULL
            """,
            (sheet_id, league_id, from_round, to_round),
        ).fetchone()
        return int(row["c"]) if row else 0

    def get_round_row_map_bulk(self, sheet_id: int, round_ids: Sequence[int]) -> Dict[int, Dict[str, Any]]:
        if not round_ids:
            return {}
        ids = sorted({int(x) for x in round_ids})
        placeholders = ",".join(["?"] * len(ids))
        rows = self.conn.execute(
            f"SELECT round_id,row_idx,checksum,finalized FROM round_row_map WHERE sheet_id=? AND round_id IN ({placeholders})",
            tuple([sheet_id] + ids),
        ).fetchall()
        out: Dict[int, Dict[str, Any]] = {}
        for r in rows:
            rid = safe_int(r["round_id"])
            if rid is None:
                continue
            out[rid] = {
                "row_idx": safe_int(r["row_idx"]),
                "checksum": str(r["checksum"]),
                "finalized": int(r["finalized"]),
            }
        return out

    def get_max_row_idx(self, sheet_id: int) -> Optional[int]:
        row = self.conn.execute(
            "SELECT MAX(row_idx) AS max_row_idx FROM round_row_map WHERE sheet_id=?",
            (sheet_id,),
        ).fetchone()
        if not row:
            return None
        return safe_int(row["max_row_idx"])

    def upsert_row_map(self, sheet_id: int, round_id: int, row_idx: int, checksum: str, finalized: int) -> None:
        self.conn.execute(
            """
            INSERT INTO round_row_map(sheet_id,round_id,row_idx,checksum,finalized,updated_at)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(sheet_id,round_id) DO UPDATE SET
              row_idx=excluded.row_idx,
              checksum=excluded.checksum,
              finalized=excluded.finalized,
              updated_at=excluded.updated_at
            """,
            (sheet_id, round_id, row_idx, checksum, finalized, time.time()),
        )
        self.conn.commit()

    def prune_row_map(self, sheet_id: int, keep_rows: int = ROW_MAP_KEEP_PER_SHEET) -> None:
        self.conn.execute(
            """
            DELETE FROM round_row_map
            WHERE sheet_id=?
              AND round_id NOT IN (
                SELECT round_id FROM round_row_map WHERE sheet_id=? ORDER BY round_id DESC LIMIT ?
              )
            """,
            (sheet_id, sheet_id, keep_rows),
        )
        self.conn.commit()

    def enqueue_op(self, sheet_id: int, op_type: str, round_id: int, checksum: str, payload: Dict[str, Any]) -> None:
        now = time.time()
        payload_json = json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
        self.conn.execute(
            """
            INSERT INTO pending_ops(sheet_id,op_type,round_id,checksum,payload_json,retry_count,next_attempt_ts,created_at)
            VALUES(?,?,?,?,?,0,?,?)
            ON CONFLICT(sheet_id,op_type,round_id) DO UPDATE SET
              checksum=excluded.checksum,
              payload_json=excluded.payload_json,
              next_attempt_ts=CASE
                WHEN pending_ops.next_attempt_ts > excluded.next_attempt_ts THEN excluded.next_attempt_ts
                ELSE pending_ops.next_attempt_ts
              END
            """,
            (sheet_id, op_type, round_id, checksum, payload_json, now, now),
        )
        self.conn.commit()
        if DEBUG_VERBOSE:
            preview = payload_json[:QUEUE_DEBUG_PREVIEW]
            log_debug(
                "queue.op_enqueued",
                sheet_id=sheet_id,
                op_type=op_type,
                round_id=round_id,
                checksum=checksum[:12],
                payload_preview=preview,
            )

    def fetch_due_ops(self, limit: int = QUEUE_FETCH_LIMIT) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT id,sheet_id,op_type,round_id,checksum,payload_json,retry_count,next_attempt_ts,created_at "
            "FROM pending_ops WHERE next_attempt_ts<=? ORDER BY next_attempt_ts ASC, id ASC LIMIT ?",
            (time.time(), limit),
        ).fetchall()
        out = [dict(r) for r in rows]
        if DEBUG_VERBOSE:
            log_debug("queue.fetch_due", limit=limit, due=len(out))
        return out

    def purge_ops_for_missing_sheets(self, active_sheet_ids: Sequence[int]) -> int:
        ids = sorted({int(x) for x in active_sheet_ids})
        if not ids:
            return 0
        placeholders = ",".join(["?"] * len(ids))
        cur = self.conn.execute(
            f"DELETE FROM pending_ops WHERE sheet_id NOT IN ({placeholders})",
            tuple(ids),
        )
        self.conn.commit()
        return int(cur.rowcount or 0)

    def purge_ops_by_type(self, op_types: Sequence[str]) -> int:
        types = [str(x).strip() for x in op_types if str(x).strip()]
        if not types:
            return 0
        placeholders = ",".join(["?"] * len(types))
        cur = self.conn.execute(
            f"DELETE FROM pending_ops WHERE op_type IN ({placeholders})",
            tuple(types),
        )
        self.conn.commit()
        return int(cur.rowcount or 0)

    def delete_op(self, op_id: int) -> None:
        self.conn.execute("DELETE FROM pending_ops WHERE id=?", (op_id,))
        self.conn.commit()
        if DEBUG_VERBOSE:
            log_debug("queue.op_deleted", op_id=op_id)

    def reschedule_op(self, op_id: int, retry_count: int, next_attempt_ts: float) -> None:
        self.conn.execute(
            "UPDATE pending_ops SET retry_count=?, next_attempt_ts=? WHERE id=?",
            (retry_count, next_attempt_ts, op_id),
        )
        self.conn.commit()
        if DEBUG_VERBOSE:
            log_debug("queue.op_rescheduled", op_id=op_id, retry_count=retry_count, next_attempt_ts=round(next_attempt_ts, 3))

    def queue_total_count(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) AS c FROM pending_ops").fetchone()
        return int(row["c"]) if row else 0

    def queue_due_count(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) AS c FROM pending_ops WHERE next_attempt_ts<=?", (time.time(),)).fetchone()
        return int(row["c"]) if row else 0

    def upsert_api_round_record(self, rec: Dict[str, Any], source: str = "api_triplet") -> None:
        league_id = safe_int(rec.get("league_id"))
        round_id = safe_int(rec.get("round_id"))
        if league_id is None or round_id is None:
            return
        self.conn.execute(
            """
            INSERT INTO api_round_cache(
              league_id, round_id, snapshot_ts, block_number, multiplier, gmt_fund, gmt_per_block,
              league_th, blocks_mined, efficiency_league, ended_at, round_duration_sec, source, updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(league_id, round_id) DO UPDATE SET
              snapshot_ts=COALESCE(excluded.snapshot_ts, api_round_cache.snapshot_ts),
              block_number=COALESCE(excluded.block_number, api_round_cache.block_number),
              multiplier=COALESCE(excluded.multiplier, api_round_cache.multiplier),
              gmt_fund=COALESCE(excluded.gmt_fund, api_round_cache.gmt_fund),
              gmt_per_block=COALESCE(excluded.gmt_per_block, api_round_cache.gmt_per_block),
              league_th=COALESCE(excluded.league_th, api_round_cache.league_th),
              blocks_mined=COALESCE(excluded.blocks_mined, api_round_cache.blocks_mined),
              efficiency_league=COALESCE(excluded.efficiency_league, api_round_cache.efficiency_league),
              ended_at=COALESCE(excluded.ended_at, api_round_cache.ended_at),
              round_duration_sec=COALESCE(excluded.round_duration_sec, api_round_cache.round_duration_sec),
              source=excluded.source,
              updated_at=excluded.updated_at
            """,
            (
                league_id,
                round_id,
                to_iso_utc(rec.get("snapshot_ts")),
                safe_int(rec.get("block_number")),
                safe_float(rec.get("multiplier")),
                safe_float(rec.get("gmt_fund")),
                safe_float(rec.get("gmt_per_block")),
                safe_float(rec.get("league_th")),
                safe_int(rec.get("blocks_mined")),
                safe_float(rec.get("efficiency_league")),
                to_iso_utc(rec.get("ended_at")),
                safe_int(rec.get("round_duration_sec")),
                str(source or "api_triplet"),
                time.time(),
            ),
        )
        self.conn.commit()

    def upsert_api_round_records(self, records: Sequence[Dict[str, Any]], source: str = "api_triplet") -> int:
        cnt = 0
        for rec in records:
            if not isinstance(rec, dict):
                continue
            league_id = safe_int(rec.get("league_id"))
            round_id = safe_int(rec.get("round_id"))
            if league_id is None or round_id is None:
                continue
            self.conn.execute(
                """
                INSERT INTO api_round_cache(
                  league_id, round_id, snapshot_ts, block_number, multiplier, gmt_fund, gmt_per_block,
                  league_th, blocks_mined, efficiency_league, ended_at, round_duration_sec, source, updated_at
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(league_id, round_id) DO UPDATE SET
                  snapshot_ts=COALESCE(excluded.snapshot_ts, api_round_cache.snapshot_ts),
                  block_number=COALESCE(excluded.block_number, api_round_cache.block_number),
                  multiplier=COALESCE(excluded.multiplier, api_round_cache.multiplier),
                  gmt_fund=COALESCE(excluded.gmt_fund, api_round_cache.gmt_fund),
                  gmt_per_block=COALESCE(excluded.gmt_per_block, api_round_cache.gmt_per_block),
                  league_th=COALESCE(excluded.league_th, api_round_cache.league_th),
                  blocks_mined=COALESCE(excluded.blocks_mined, api_round_cache.blocks_mined),
                  efficiency_league=COALESCE(excluded.efficiency_league, api_round_cache.efficiency_league),
                  ended_at=COALESCE(excluded.ended_at, api_round_cache.ended_at),
                  round_duration_sec=COALESCE(excluded.round_duration_sec, api_round_cache.round_duration_sec),
                  source=excluded.source,
                  updated_at=excluded.updated_at
                """,
                (
                    league_id,
                    round_id,
                    to_iso_utc(rec.get("snapshot_ts")),
                    safe_int(rec.get("block_number")),
                    safe_float(rec.get("multiplier")),
                    safe_float(rec.get("gmt_fund")),
                    safe_float(rec.get("gmt_per_block")),
                    safe_float(rec.get("league_th")),
                    safe_int(rec.get("blocks_mined")),
                    safe_float(rec.get("efficiency_league")),
                    to_iso_utc(rec.get("ended_at")),
                    safe_int(rec.get("round_duration_sec")),
                    str(source or "api_triplet"),
                    time.time(),
                ),
            )
            cnt += 1
        self.conn.commit()
        return cnt

    def fetch_api_completed_rounds(self, league_id: int, since_round: int, limit: int = MAX_ROUNDS_PER_POLL) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT
              snapshot_ts,
              league_id,
              round_id,
              block_number,
              multiplier,
              gmt_fund,
              gmt_per_block,
              league_th,
              blocks_mined,
              efficiency_league,
              ended_at,
              round_duration_sec
            FROM api_round_cache
            WHERE league_id=?
              AND round_id>?
              AND ended_at IS NOT NULL
            ORDER BY round_id ASC
            LIMIT ?
            """,
            (league_id, since_round, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def fetch_latest_api_completed_round_id(self, league_id: int) -> Optional[int]:
        row = self.conn.execute(
            """
            SELECT round_id
            FROM api_round_cache
            WHERE league_id=?
              AND ended_at IS NOT NULL
            ORDER BY round_id DESC
            LIMIT 1
            """,
            (league_id,),
        ).fetchone()
        return safe_int(row["round_id"]) if row else None

    def fetch_latest_api_round_record(self, league_id: int) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            """
            SELECT
              snapshot_ts,
              league_id,
              round_id,
              block_number,
              multiplier,
              gmt_fund,
              gmt_per_block,
              league_th,
              blocks_mined,
              efficiency_league,
              ended_at,
              round_duration_sec,
              source
            FROM api_round_cache
            WHERE league_id=?
            ORDER BY round_id DESC
            LIMIT 1
            """,
            (league_id,),
        ).fetchone()
        return dict(row) if row else None

    def mark_api_round_ended(self, league_id: int, round_id: int, ended_at_iso: str) -> int:
        ended_at = to_iso_utc(ended_at_iso) or utc_now_iso()
        cur = self.conn.execute(
            """
            UPDATE api_round_cache
            SET ended_at=?, updated_at=?
            WHERE league_id=?
              AND round_id=?
              AND (ended_at IS NULL OR TRIM(ended_at)='')
            """,
            (ended_at, time.time(), league_id, round_id),
        )
        self.conn.commit()
        return int(cur.rowcount or 0)


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


def _normalize_payload_row(row: Any, expected_cols: int, round_id: int, round_col_idx: Optional[int] = 6) -> Optional[List[Any]]:
    if not isinstance(row, list):
        return None
    out = list(row[:expected_cols])
    if len(out) < expected_cols:
        out.extend([""] * (expected_cols - len(out)))
    if round_col_idx is not None and round_col_idx >= 0 and expected_cols > round_col_idx:
        out[round_col_idx] = round_id
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
) -> Tuple[Dict[int, SheetContext], Dict[int, SheetContext]]:
    read_limiter.wait_for_token(1)
    worksheets = sh.worksheets()
    selectors = read_sheet_selectors(sh, worksheets, read_limiter)
    main_contexts: Dict[int, SheetContext] = {}
    clan_contexts: Dict[int, SheetContext] = {}
    main_layout_sig = row_checksum(main_expected_header)
    clan_layout_sig = row_checksum(clan_expected_header)
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

    if DEBUG_VERBOSE:
        log_debug("sheet.refresh_done", main_contexts=len(main_contexts), clan_contexts=len(clan_contexts))

    return main_contexts, clan_contexts


def _reschedule(state: StateStore, op_id: int, retry_count: int, retry_after: Optional[float] = None) -> None:
    delay = min(300.0, float(2 ** min(retry_count, 8))) + random.uniform(0.0, 1.0)
    if retry_after is not None:
        delay = max(delay, retry_after)
    if DEBUG_VERBOSE:
        log_debug("queue.reschedule_calc", op_id=op_id, retry_count=retry_count, retry_after=retry_after, delay_s=round(delay, 3))
    state.reschedule_op(op_id, retry_count, time.time() + delay)


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

        else:
            i += 1
            log_warn("queue.unknown_op_dropped", op_type=op_type, op_id=op["id"])
            state.delete_op(int(op["id"]))

    return processed


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
            fetch_by_round_id = getattr(round_metrics_api, "fetch_round_metrics_by_round_id", None)

            if callable(fetch_by_round_id):
                for gap_round_id in range(prev_round_id + 1, prev_round_id + attempted + 1):
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
            else:
                log_warn(
                    "sync.api_round_gap_fill_round_skip",
                    league_id=league_id,
                    round_id=prev_round_id + 1,
                    reason="client_missing_method",
                )

            capped = missing_total > SYNC_API_GAP_FILL_MAX
            remaining_missing = missing_total - fetched
            log_info(
                "sync.api_round_gap_fill",
                league_id=league_id,
                prev_round_id=prev_round_id,
                new_round_id=round_id,
                missing_total=missing_total,
                attempted=attempted,
                fetched=fetched,
                capped=capped,
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


def build_canonical_row(
    rec: Dict[str, Any],
    ability_headers: List[str],
    counts_by_name: Dict[str, int],
    price_cutover_round: Optional[int],
    power_up_gmt_value: Optional[float] = None,
    power_up_gmt_sentinel_value: Optional[float] = None,
    clan_power_up_gmt_value: Optional[float] = None,
    clan_power_up_gmt_sentinel_value: Optional[float] = None,
    power_up_missing_aliases: Optional[Sequence[str]] = None,
    excluded_user_boosts_audit: str = "",
) -> List[Any]:
    ts_iso = to_iso_utc(rec.get("snapshot_ts")) or ""
    ended_iso = to_iso_utc(rec.get("ended_at")) or ""

    league_id = safe_int(rec.get("league_id")) or ""
    round_id_num = safe_int(rec.get("round_id"))
    round_id = round_id_num or ""
    block_number = safe_int(rec.get("block_number"))
    multiplier = safe_float(rec.get("multiplier"))
    gmt_fund = safe_float(rec.get("gmt_fund"))
    gmt_per_block = safe_float(rec.get("gmt_per_block"))
    league_th = safe_float(rec.get("league_th"))
    duration = safe_int(rec.get("round_duration_sec"))
    blocks_mined = safe_int(rec.get("blocks_mined"))
    efficiency_league = safe_float(rec.get("efficiency_league"))

    row: List[Any] = [
        ts_iso,
        league_id,
        "" if block_number is None else block_number,
        "" if multiplier is None else multiplier,
        "" if gmt_fund is None else gmt_fund,
        "" if gmt_per_block is None else gmt_per_block,
        round_id,
        "" if league_th is None else league_th,
        ended_iso,
        "" if duration is None else duration,
        "" if blocks_mined is None else blocks_mined,
        "" if efficiency_league is None else efficiency_league,
    ]
    for name in ability_headers:
        row.append(int(counts_by_name.get(name, 0)))

    power_up_count = int(counts_by_name.get("Power Up Boost", 0) or 0)
    clan_power_up_count = int(counts_by_name.get("Clan Power Up Boost", 0) or 0)
    power_up_gmt: Any = ""
    power_up_gmt_sentinel: Any = ""
    clan_power_up_gmt: Any = ""
    clan_power_up_gmt_sentinel: Any = ""
    if round_id_num is not None and (price_cutover_round is None or round_id_num > price_cutover_round):
        # After cutover we persist explicit zero only when no Power Up usage was observed.
        # If usage exists but exact user-based price could not be resolved, keep cell empty (not misleading zero).
        if power_up_count <= 0:
            power_up_gmt = 0.0 if power_up_gmt_value is None else power_up_gmt_value
            power_up_gmt_sentinel = 0.0 if power_up_gmt_sentinel_value is None else power_up_gmt_sentinel_value
        else:
            power_up_gmt = "" if power_up_gmt_value is None else power_up_gmt_value
            power_up_gmt_sentinel = "" if power_up_gmt_sentinel_value is None else power_up_gmt_sentinel_value
        if clan_power_up_count <= 0:
            clan_power_up_gmt = 0.0 if clan_power_up_gmt_value is None else clan_power_up_gmt_value
            clan_power_up_gmt_sentinel = (
                0.0 if clan_power_up_gmt_sentinel_value is None else clan_power_up_gmt_sentinel_value
            )
        else:
            clan_power_up_gmt = "" if clan_power_up_gmt_value is None else clan_power_up_gmt_value
            clan_power_up_gmt_sentinel = (
                "" if clan_power_up_gmt_sentinel_value is None else clan_power_up_gmt_sentinel_value
            )
    row.append(power_up_gmt)
    row.append(power_up_gmt_sentinel)
    row.append(clan_power_up_gmt)
    row.append(clan_power_up_gmt_sentinel)
    aliases = [str(x).strip() for x in (power_up_missing_aliases or []) if str(x).strip()]
    row.append(", ".join(aliases))
    row.append(str(excluded_user_boosts_audit or "").strip())
    return row


def build_clan_round_rows(rec: Dict[str, Any], clan_rows: Sequence[Dict[str, Any]]) -> List[List[Any]]:
    fallback_ts = to_iso_utc(rec.get("snapshot_ts")) or ""
    league_id = safe_int(rec.get("league_id")) or ""
    round_id = safe_int(rec.get("round_id")) or ""
    out: List[List[Any]] = []
    for clan in clan_rows:
        snap_ts = to_iso_utc(clan.get("snapshot_ts")) or fallback_ts
        snapshot_round_id = safe_int(clan.get("snapshot_round_id"))
        clan_id = safe_int(clan.get("clan_id"))
        members_total = safe_int(clan.get("members_total"))
        members_seen = safe_int(clan.get("members_seen"))
        member_cov = safe_float(clan.get("member_coverage"))
        team_th = safe_float(clan.get("team_th"))
        team_pps = safe_float(clan.get("team_pps"))
        out.append(
            [
                snap_ts,
                league_id,
                round_id,
                "" if snapshot_round_id is None else snapshot_round_id,
                "" if clan_id is None else clan_id,
                str(clan.get("clan_name") or ""),
                "" if members_total is None else members_total,
                "" if members_seen is None else members_seen,
                "" if member_cov is None else member_cov,
                "" if team_th is None else team_th,
                "" if team_pps is None else team_pps,
                str(clan.get("calc_mode") or "missing"),
            ]
        )
    return out


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
                    mapped = fresh_map
                    row_maps[rid] = fresh_map
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
                        mapped = ability_id_to_name.get(aid)
                        if mapped:
                            known_counts[mapped] = known_counts.get(mapped, 0) + cnt
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
                        exact_power_up_gmt, exact_power_up_gmt_sentinel, exact_missing = calc_power_up_gmt_triplet_from_power_up_users_api(
                            power_up_users,
                            power_up_clan_api,
                            resolution_stats=power_up_resolution_stats,
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
                        exact_clan_power_up_gmt, exact_clan_power_up_gmt_sentinel = calc_clan_power_up_gmt_pair_from_boost_users_api(
                            clan_power_up_users,
                            power_up_clan_api,
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

            try:
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
                )
                checksum = row_checksum(row)
                finalized = 1 if rid <= (max_round - STABILIZATION_ROUNDS) else 0

                if mapped is None:
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
                    row_idx = safe_int(mapped.get("row_idx"))
                    if row_idx is None:
                        if DEBUG_VERBOSE:
                            log_debug("sync.round_skip_update_missing_row_idx", sheet=ctx.title, round_id=rid)
                        continue
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
                    err=repr(e),
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
        _ = (db, _state)
        if int(league_id) != int(ctx.league_id):
            return []
        out: List[Dict[str, Any]] = []
        for rec in recs_sorted:
            rid = safe_int(rec.get("round_id"))
            if rid is None or rid <= since_round:
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
        target_ids = sorted({rid for rid in missing_ids if rid > 0} | {rid for rid in retry_ids if rid > 0})
        if not target_ids:
            continue
        sheets_with_gaps += 1
        rec_by_rid = {int(safe_int(r.get("round_id")) or -1): r for r in records if safe_int(r.get("round_id")) is not None}
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Hashrate Hunter continuous sync service")
    parser.add_argument(
        "--once-reconcile",
        action="store_true",
        help="Run one reconcile pass (plus queue flush) and exit.",
    )
    args = parser.parse_args()
    main(once_reconcile=bool(args.once_reconcile))
