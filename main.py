#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
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
    import psycopg  # type: ignore
except Exception:
    psycopg = None

try:
    import psycopg2  # type: ignore
except Exception:
    psycopg2 = None

try:
    import dotenv
except Exception:  # pragma: no cover
    dotenv = None  # type: ignore

if dotenv is not None:
    dotenv.load_dotenv()

# Config
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "service_acc.json")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1bq5Sy2pV35x33Q12G5_EJ2S0Kb1iXo1Y2FCxL7QIxGg")

SYNC_POLL_SECONDS = int(os.getenv("SYNC_POLL_SECONDS", "120"))
SHEET_REFRESH_SECONDS = int(os.getenv("SHEET_REFRESH_SECONDS", "1800"))
GS_WRITE_REQ_PER_MIN = int(os.getenv("GS_WRITE_REQ_PER_MIN", "45"))
GS_READ_REQ_PER_MIN = int(os.getenv("GS_READ_REQ_PER_MIN", "20"))
STATE_DB_PATH = os.getenv("STATE_DB_PATH", "./sync_state.sqlite")
LOCK_FILE_PATH = os.getenv("LOCK_FILE_PATH", "./sync.lock")
LEAGUES_API_URL = os.getenv("LEAGUES_API_URL", "https://api.gomining.com/api/nft-game/league/index")
GOMINING_BEARER_TOKEN = os.getenv("GOMINING_BEARER_TOKEN", "")
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
GOMINING_API_REQ_PER_MIN = int(os.getenv("GOMINING_API_REQ_PER_MIN", "120"))
TOKEN_URL = os.getenv("TOKEN_URL", "").strip()
TOKEN_X_AUTH = os.getenv("TOKEN_X_AUTH", "").strip()
TOKEN_METHOD = os.getenv("TOKEN_METHOD", "GET").strip().upper()
TOKEN_TIMEOUT_SECONDS = max(1, int(os.getenv("TOKEN_TIMEOUT_SECONDS", "20")))
TOKEN_VERIFY_SSL = os.getenv("TOKEN_VERIFY_SSL", "1").strip() in {"1", "true", "TRUE", "yes", "YES"}
LEAGUES_API_POLL_SECONDS = int(os.getenv("LEAGUES_API_POLL_SECONDS", str(SHEET_REFRESH_SECONDS)))

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

CONFIG_CELL = "B1"
LOG_START_ROW = 4

MAIN_SHEET_MARKER = "leagueId_to_log"
CLAN_SHEET_MARKER = "leagueId_to_log_clan_shield"
CLAN_TAB_SUFFIX = " - Clan Shield"

LEAGUE_TH_HEADER = "League global TH"
LEAGUE_TH_COL_INDEX = 7  # 0-based index, column H in the sheet.
MIGRATION_CHECKED_SHEETS: set[int] = set()
POWER_UP_PRICE_HEADER = "Power Up GMT Price"

PPS_FACTOR = 28.0
POWER_UP_GMT_FACTOR = 0.0389
CLAN_SHIELD_GMT_FACTOR = 0.000555
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
    "clan_shield_gmt",
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
    return PPS_FACTOR * league_th / efficiency_league


def calc_power_up_gmt(league_th: Optional[float], efficiency_league: Optional[float]) -> Optional[float]:
    pps = calc_league_pps(league_th, efficiency_league)
    if pps is None:
        return None
    return pps * POWER_UP_GMT_FACTOR


def calc_team_pps_exact(sum_th_over_w: Optional[float]) -> Optional[float]:
    if sum_th_over_w is None or sum_th_over_w <= 0:
        return None
    return PPS_FACTOR * sum_th_over_w


def calc_team_pps_fallback(team_th: Optional[float], efficiency_league: Optional[float]) -> Optional[float]:
    if team_th is None or team_th <= 0:
        return None
    return calc_league_pps(team_th, efficiency_league)


def calc_clan_shield_gmt(team_pps: Optional[float]) -> Optional[float]:
    if team_pps is None:
        return None
    return team_pps * CLAN_SHIELD_GMT_FACTOR


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


def parse_league_index_response(payload: Dict[str, Any]) -> Dict[int, str]:
    data = (payload or {}).get("data") or {}
    arr = data.get("array") or []
    out: Dict[int, str] = {}
    if not isinstance(arr, list):
        return out
    for item in arr:
        if not isinstance(item, dict):
            continue
        league_id = safe_int(item.get("id"))
        league_name = str(item.get("name") or "").strip()
        if league_id is None:
            continue
        out[league_id] = league_name or f"league-{league_id}"
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


def fetch_league_catalog_from_api(bearer_token: Optional[str] = None) -> Dict[int, str]:
    token = str(bearer_token or GOMINING_BEARER_TOKEN).strip()
    if not token:
        return {}
    headers = {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "authorization": f"Bearer {token}",
        "x-device-type": "desktop",
    }
    payload = {"calculatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00.000Z")}
    try:
        resp = requests.post(LEAGUES_API_URL, headers=headers, json=payload, timeout=20)
        if resp.status_code != 200:
            log_warn("league_api.http_error", status=resp.status_code, body_preview=resp.text[:180])
            return {}
        parsed = parse_league_index_response(resp.json())
        log_debug("league_api.ok", leagues=len(parsed))
        return parsed
    except Exception as e:
        log_warn("league_api.failed", err=repr(e))
        return {}


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
        pwr = safe_float(user.get("power"))
        ee = safe_float(user.get("ee"))
        if pwr is None or pwr <= 0:
            continue
        team_th += pwr
        if ee is None or ee <= 0:
            continue
        team_pps += PPS_FACTOR * pwr / ee
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
    ) -> None:
        self.bearer_token = bearer_token.strip()
        self.token_fetcher = token_fetcher
        self.limiter = limiter
        self.leaderboard_url = leaderboard_url
        self.clan_get_by_id_url = clan_get_by_id_url
        self.page_limit = max(1, min(50, int(page_limit)))
        self.timeout_seconds = max(1, int(timeout_seconds))
        self.max_retries = max(1, int(max_retries))
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

    def _set_bearer(self, token: str) -> None:
        self.bearer_token = token.strip()
        self.headers["authorization"] = f"Bearer {self.bearer_token}"

    def _refresh_bearer(self, force: bool = False) -> bool:
        if not self.token_fetcher:
            return bool(self.bearer_token)
        if self.bearer_token and not force:
            return True
        tok = (self.token_fetcher() or "").strip()
        if not tok:
            return bool(self.bearer_token)
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

            retryable = resp.status_code in {429, 500, 502, 503, 504}
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
                    "clan_shield_gmt": calc_clan_shield_gmt(team_pps),
                    "calc_mode": "api_exact",
                }
            )
        rows.sort(key=lambda x: (-(safe_float(x.get("team_pps")) or 0.0), safe_int(x.get("clan_id")) or 0))
        return rows


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
        self.capacity = max(1, per_minute)
        self.tokens = float(self.capacity)
        self.refill = float(self.capacity) / 60.0
        self.last = time.monotonic()
        self.total_acquired = 0
        self.total_wait_s = 0.0
        self.wait_events = 0

    def wait_for_token(self, amount: float = 1.0) -> None:
        start_wait = time.monotonic()
        local_wait_events = 0
        while True:
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

    def snapshot(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "capacity_per_min": self.capacity,
            "tokens_now": round(self.tokens, 3),
            "total_acquired": self.total_acquired,
            "total_wait_s": round(self.total_wait_s, 3),
            "wait_events": self.wait_events,
        }


class SingleInstanceLock:
    def __init__(self, path: str) -> None:
        self.path = path
        self.acquired = False

    def acquire(self) -> None:
        p = Path(self.path)
        try:
            fd = os.open(str(p), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(f"pid={os.getpid()} ts={utc_now_iso()}\n")
            self.acquired = True
        except FileExistsError:
            raise RuntimeError(f"Lock file exists: {self.path}")

    def release(self) -> None:
        if not self.acquired:
            return
        try:
            os.remove(self.path)
        except Exception:
            pass


class DBClient:
    def __init__(self) -> None:
        self.conn: Any = None
        self.cols: set[str] = set()
        self.primary_enabled = True

    def connect(self) -> None:
        if self.conn is not None:
            return
        kwargs = {
            "host": os.getenv("PGHOST", "127.0.0.1"),
            "port": int(os.getenv("PGPORT", "5432")),
            "dbname": os.getenv("PGDATABASE", "GoMining"),
            "user": os.getenv("PGUSER", "postgres"),
            "password": os.getenv("PGPASSWORD", ""),
            "sslmode": os.getenv("PGSSLMODE", "prefer"),
        }
        if psycopg is not None:
            self.conn = psycopg.connect(**kwargs)
            self.conn.autocommit = True
        elif psycopg2 is not None:
            self.conn = psycopg2.connect(**kwargs)
            self.conn.autocommit = True
        else:
            raise RuntimeError("Install psycopg or psycopg2")
        self.load_multiplier_schema()

    def close(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            finally:
                self.conn = None

    def query(self, sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
        if self.conn is None:
            self.connect()
        cur = self.conn.cursor()
        try:
            cur.execute(sql, tuple(params))
            if cur.description is None:
                return []
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            return [{cols[i]: row[i] for i in range(len(cols))} for row in rows]
        finally:
            cur.close()

    def load_multiplier_schema(self) -> None:
        try:
            rows = self.query(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='gomining' AND table_name='multiplier_snapshots'"
            )
            self.cols = {str(r["column_name"]).strip() for r in rows if r.get("column_name")}
            self.primary_enabled = len(self.cols) > 0
        except Exception as e:
            log_warn("db.schema_introspection_failed", err=repr(e))
            self.cols = set()
            self.primary_enabled = False

    def _has(self, name: str) -> bool:
        return name in self.cols

    @staticmethod
    def _co(*exprs: Optional[str]) -> str:
        arr = [x for x in exprs if x]
        if not arr:
            return "NULL"
        if len(arr) == 1:
            return arr[0]
        return "COALESCE(" + ", ".join(arr) + ")"

    def _payload_bigint(self, path: str) -> str:
        if not self._has("source_payload"):
            return "NULL"
        return f"CASE WHEN (source_payload #>> '{path}') ~ '^-?[0-9]+$' THEN (source_payload #>> '{path}')::bigint END"

    def _payload_int(self, path: str) -> str:
        if not self._has("source_payload"):
            return "NULL"
        return f"CASE WHEN (source_payload #>> '{path}') ~ '^-?[0-9]+$' THEN (source_payload #>> '{path}')::integer END"

    def _payload_num(self, path: str) -> str:
        if not self._has("source_payload"):
            return "NULL"
        return f"CASE WHEN (source_payload #>> '{path}') ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN (source_payload #>> '{path}')::numeric END"

    def _payload_ts(self, path: str) -> str:
        if not self._has("source_payload"):
            return "NULL"
        return f"NULLIF(source_payload #>> '{path}','')::timestamptz"

    def _mult_exprs(self) -> Dict[str, str]:
        c = self._has
        return {
            "league": self._co("league_id" if c("league_id") else None, self._payload_int("{data,leagueId}"), self._payload_int("{leagueId}")),
            "round": self._co("round_id" if c("round_id") else None, self._payload_bigint("{data,roundId}"), self._payload_bigint("{roundId}")),
            "block": self._co("block_number" if c("block_number") else None, self._payload_bigint("{data,blockNumber}"), self._payload_bigint("{blockNumber}")),
            "mult": self._co("multiplier" if c("multiplier") else None, self._payload_num("{data,multiplier}"), self._payload_num("{multiplier}")),
            "gmt_fund": self._co("gmt_fund" if c("gmt_fund") else None, "gmtFund" if c("gmtFund") else None, self._payload_num("{data,gmtFund}"), self._payload_num("{gmtFund}")),
            "gmt_pb": self._co("gmt_per_block" if c("gmt_per_block") else None, "gmtPerBlock" if c("gmtPerBlock") else None, self._payload_num("{data,gmtPerBlock}"), self._payload_num("{gmtPerBlock}")),
            "total_power": self._co(
                "total_power" if c("total_power") else None,
                self._payload_num("{data,totalPower}"),
                self._payload_num("{totalPower}"),
                self._payload_num("{data,total_power}"),
                self._payload_num("{total_power}"),
            ),
            "blocks_mined": self._co("blocks_mined" if c("blocks_mined") else None, self._payload_int("{data,blocksMined}"), self._payload_int("{blocksMined}")),
            "eff_league": self._co("efficiency_league" if c("efficiency_league") else None, "effciency_league" if c("effciency_league") else None, self._payload_num("{data,efficiencyLeague}"), self._payload_num("{efficiencyLeague}"), self._payload_num("{data,effciencyLeague}"), self._payload_num("{effciencyLeague}")),
            "ended": self._co("ended_at" if c("ended_at") else None, "round_ended_at" if c("round_ended_at") else None, self._payload_ts("{data,endedAt}"), self._payload_ts("{endedAt}")),
            "duration": self._co("round_duration_sec" if c("round_duration_sec") else None, "round_time" if c("round_time") else None, self._payload_int("{data,roundTime}"), self._payload_int("{roundTime}")),
            "ts": self._co("snapshot_collected_at" if c("snapshot_collected_at") else None, "collected_at" if c("collected_at") else None, "created_at" if c("created_at") else None, "updated_at" if c("updated_at") else None, "now()"),
            "active": "active" if c("active") else "NULL",
        }

    def _primary_rounds(self, league_id: int, since_round: int, limit: int, desc: bool = False) -> List[Dict[str, Any]]:
        if not self.primary_enabled:
            return []
        e = self._mult_exprs()
        pred = f"({e['ended']} IS NOT NULL)"
        if e["active"] != "NULL":
            pred = f"({e['ended']} IS NOT NULL OR {e['active']} = FALSE)"
        direction = "DESC" if desc else "ASC"
        sql = f"""
        SELECT
          {e['ts']} AS snapshot_ts,
          {e['league']} AS league_id,
          {e['round']} AS round_id,
          {e['block']} AS block_number,
          {e['mult']} AS multiplier,
          {e['gmt_fund']} AS gmt_fund,
          {e['gmt_pb']} AS gmt_per_block,
          {e['total_power']} AS league_th,
          {e['blocks_mined']} AS blocks_mined,
          {e['eff_league']} AS efficiency_league,
          {e['ended']} AS ended_at,
          {e['duration']} AS round_duration_sec
        FROM gomining.multiplier_snapshots
        WHERE {e['league']} = %s
          AND {e['round']} IS NOT NULL
          AND {e['round']} > %s
          AND {pred}
        ORDER BY {e['round']} {direction}
        LIMIT %s
        """
        return self.query(sql, (league_id, since_round, limit))

    def _fallback_rounds(self, league_id: int, since_round: int, limit: int, desc: bool = False) -> List[Dict[str, Any]]:
        direction = "DESC" if desc else "ASC"
        sql = f"""
        WITH src AS (
          SELECT
            COALESCE(collected_at, created_at, now()) AS snapshot_ts,
            league_id,
            COALESCE(
              round_id,
              CASE WHEN (source_payload #>> '{{data,roundId}}') ~ '^-?[0-9]+$' THEN (source_payload #>> '{{data,roundId}}')::bigint END,
              CASE WHEN (source_payload #>> '{{roundId}}') ~ '^-?[0-9]+$' THEN (source_payload #>> '{{roundId}}')::bigint END
            ) AS round_id,
            COALESCE(
              block_number,
              CASE WHEN (source_payload #>> '{{data,blockNumber}}') ~ '^-?[0-9]+$' THEN (source_payload #>> '{{data,blockNumber}}')::bigint END,
              CASE WHEN (source_payload #>> '{{blockNumber}}') ~ '^-?[0-9]+$' THEN (source_payload #>> '{{blockNumber}}')::bigint END
            ) AS block_number,
            COALESCE(
              CASE WHEN (source_payload #>> '{{data,multiplier}}') ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN (source_payload #>> '{{data,multiplier}}')::numeric END,
              CASE WHEN (source_payload #>> '{{multiplier}}') ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN (source_payload #>> '{{multiplier}}')::numeric END
            ) AS multiplier,
            NULL::numeric AS gmt_fund,
            NULL::numeric AS gmt_per_block,
            NULL::numeric AS league_th,
            NULL::integer AS blocks_mined,
            NULL::numeric AS efficiency_league,
            COALESCE(
              NULLIF(source_payload #>> '{{data,endedAt}}','')::timestamptz,
              NULLIF(source_payload #>> '{{endedAt}}','')::timestamptz
            ) AS ended_at,
            COALESCE(
              CASE WHEN (source_payload #>> '{{data,roundTime}}') ~ '^-?[0-9]+$' THEN (source_payload #>> '{{data,roundTime}}')::integer END,
              CASE WHEN (source_payload #>> '{{roundTime}}') ~ '^-?[0-9]+$' THEN (source_payload #>> '{{roundTime}}')::integer END
            ) AS round_duration_sec
          FROM gomining.abilities_snapshots
          WHERE league_id = %s
        )
        SELECT * FROM src
        WHERE round_id IS NOT NULL AND round_id > %s AND ended_at IS NOT NULL
        ORDER BY round_id {direction}
        LIMIT %s
        """
        return self.query(sql, (league_id, since_round, limit))

    def fetch_completed_rounds_from_db(self, league_id: int, since_round: int, limit: int = MAX_ROUNDS_PER_POLL) -> List[Dict[str, Any]]:
        try:
            rows = self._primary_rounds(league_id, since_round, limit, desc=False)
            if rows:
                return rows
            if self.primary_enabled:
                return []
        except Exception as e:
            log_warn("db.rounds_primary_failed", league_id=league_id, err=repr(e))
            self.primary_enabled = False
        try:
            return self._fallback_rounds(league_id, since_round, limit, desc=False)
        except Exception as e:
            log_error("db.rounds_fallback_failed", league_id=league_id, err=repr(e))
            return []

    def fetch_latest_completed_round_id(self, league_id: int) -> Optional[int]:
        try:
            rows = self._primary_rounds(league_id, -1, 1, desc=True)
            if rows:
                return safe_int(rows[0].get("round_id"))
        except Exception:
            self.primary_enabled = False
        try:
            rows = self._fallback_rounds(league_id, -1, 1, desc=True)
            if rows:
                return safe_int(rows[0].get("round_id"))
        except Exception:
            pass
        return None

    def load_ability_catalog(self) -> List[Tuple[str, str, int]]:
        sql = """
        SELECT ability_id::text AS ability_id, ability_name::text AS ability_name,
               COALESCE(sort_order, 9999) AS sort_order
        FROM gomining.ability_dim
        ORDER BY COALESCE(sort_order, 9999), ability_name
        """
        try:
            rows = self.query(sql)
            out: List[Tuple[str, str, int]] = []
            for r in rows:
                aid = str(r.get("ability_id") or "").strip()
                aname = str(r.get("ability_name") or "").strip()
                srt = safe_int(r.get("sort_order")) or 9999
                if aid and aname:
                    out.append((aid, aname, srt))
            if out:
                return out
        except Exception as e:
            log_warn("db.ability_dim_unavailable", err=repr(e))

        rows = self.query(
            "SELECT ability_id::text AS ability_id, "
            "COALESCE(NULLIF(MAX(item_payload->>'abilityName'),''), ability_id::text) AS ability_name "
            "FROM gomining.abilities_snapshot_items GROUP BY ability_id ORDER BY ability_name"
        )
        out = []
        for r in rows:
            aid = str(r.get("ability_id") or "").strip()
            aname = str(r.get("ability_name") or "").strip()
            if aid and aname:
                out.append((aid, aname, 9999))
        return out

    def fetch_ability_counts_for_rounds(self, league_id: int, round_ids: Sequence[int]) -> Dict[int, Dict[str, int]]:
        if not round_ids:
            return {}
        ids = sorted({int(x) for x in round_ids})
        placeholders = ", ".join(["%s"] * len(ids))
        sql = f"""
        WITH latest AS (
          SELECT
            COALESCE(
              s.round_id,
              CASE WHEN (s.source_payload #>> '{{data,roundId}}') ~ '^-?[0-9]+$' THEN (s.source_payload #>> '{{data,roundId}}')::bigint END,
              CASE WHEN (s.source_payload #>> '{{roundId}}') ~ '^-?[0-9]+$' THEN (s.source_payload #>> '{{roundId}}')::bigint END
            ) AS round_id,
            MAX(s.abilities_snapshot_id) AS abilities_snapshot_id
          FROM gomining.abilities_snapshots s
          WHERE s.league_id = %s
            AND COALESCE(
              s.round_id,
              CASE WHEN (s.source_payload #>> '{{data,roundId}}') ~ '^-?[0-9]+$' THEN (s.source_payload #>> '{{data,roundId}}')::bigint END,
              CASE WHEN (s.source_payload #>> '{{roundId}}') ~ '^-?[0-9]+$' THEN (s.source_payload #>> '{{roundId}}')::bigint END
            ) IN ({placeholders})
          GROUP BY 1
        ),
        agg AS (
          SELECT l.round_id, i.ability_id::text AS ability_id, SUM(i.ability_count)::bigint AS cnt
          FROM latest l JOIN gomining.abilities_snapshot_items i ON i.abilities_snapshot_id = l.abilities_snapshot_id
          WHERE i.entity_type = 'aggregate'
          GROUP BY l.round_id, i.ability_id
        ),
        part AS (
          SELECT l.round_id, i.ability_id::text AS ability_id, SUM(i.ability_count)::bigint AS cnt
          FROM latest l JOIN gomining.abilities_snapshot_items i ON i.abilities_snapshot_id = l.abilities_snapshot_id
          WHERE i.entity_type = 'participant'
          GROUP BY l.round_id, i.ability_id
        )
        SELECT COALESCE(a.round_id, p.round_id) AS round_id,
               COALESCE(a.ability_id, p.ability_id) AS ability_id,
               COALESCE(a.cnt, p.cnt, 0)::bigint AS cnt
        FROM agg a FULL OUTER JOIN part p ON p.round_id = a.round_id AND p.ability_id = a.ability_id
        """
        rows = self.query(sql, [league_id] + ids)
        out: Dict[int, Dict[str, int]] = {}
        for r in rows:
            rid = safe_int(r.get("round_id"))
            aid = str(r.get("ability_id") or "").strip()
            cnt = safe_int(r.get("cnt")) or 0
            if rid is None or not aid:
                continue
            out.setdefault(rid, {})[aid] = int(cnt)
        return out

    def fetch_power_up_gmt_sum_for_rounds(self, league_id: int, round_ids: Sequence[int]) -> Dict[int, float]:
        """
        Sum Power Up GMT cost per round from participant usage:
          ability_count * (PPS_FACTOR * nft_power / mean_energy_efficiency) * POWER_UP_GMT_FACTOR
        Power/eff are resolved by priority:
          1) exact round-linked player snapshot membership
          2) latest same-league membership at or before round
          3) latest membership at or before round (any league)
        """
        if not round_ids:
            return {}
        ids = sorted({int(x) for x in round_ids})
        placeholders = ", ".join(["%s"] * len(ids))
        sql = f"""
        WITH pu AS (
          SELECT ability_id
          FROM gomining.ability_dim
          WHERE lower(ability_name) IN ('power up boost', 'power-up boost')
          UNION
          SELECT DISTINCT i.ability_id::text
          FROM gomining.abilities_snapshot_items i
          WHERE lower(COALESCE(i.item_payload->>'abilityName', '')) IN ('power up boost', 'power-up boost')
        ),
        latest AS (
          SELECT
            COALESCE(
              s.round_id,
              CASE WHEN (s.source_payload #>> '{{data,roundId}}') ~ '^-?[0-9]+$' THEN (s.source_payload #>> '{{data,roundId}}')::bigint END,
              CASE WHEN (s.source_payload #>> '{{roundId}}') ~ '^-?[0-9]+$' THEN (s.source_payload #>> '{{roundId}}')::bigint END
            ) AS round_id,
            MAX(s.abilities_snapshot_id) AS abilities_snapshot_id
          FROM gomining.abilities_snapshots s
          WHERE s.league_id = %s
            AND COALESCE(
              s.round_id,
              CASE WHEN (s.source_payload #>> '{{data,roundId}}') ~ '^-?[0-9]+$' THEN (s.source_payload #>> '{{data,roundId}}')::bigint END,
              CASE WHEN (s.source_payload #>> '{{roundId}}') ~ '^-?[0-9]+$' THEN (s.source_payload #>> '{{roundId}}')::bigint END
            ) IN ({placeholders})
          GROUP BY 1
        ),
        uses AS (
          SELECT
            l.round_id,
            i.entity_id::bigint AS user_id,
            SUM(COALESCE(i.ability_count, 0))::numeric AS ability_count
          FROM latest l
          JOIN gomining.abilities_snapshot_items i ON i.abilities_snapshot_id = l.abilities_snapshot_id
          WHERE i.entity_type = 'participant'
            AND i.entity_id IS NOT NULL
            AND COALESCE(i.ability_count, 0) > 0
            AND (
              i.ability_id IN (SELECT ability_id FROM pu)
              OR lower(COALESCE(i.item_payload->>'abilityName', '')) IN ('power up boost', 'power-up boost')
            )
          GROUP BY l.round_id, i.entity_id
        ),
        linked AS (
          SELECT l.round_id, l.player_snapshot_id
          FROM gomining.round_player_snapshot_links l
          WHERE l.league_id = %s
            AND l.round_id IN ({placeholders})
        ),
        resolved AS (
          SELECT
            u.round_id,
            u.user_id,
            u.ability_count,
            COALESCE(pm_exact.nft_power, pm_league.nft_power, pm_any.nft_power) AS nft_power,
            COALESCE(pm_exact.mean_energy_efficiency, pm_league.mean_energy_efficiency, pm_any.mean_energy_efficiency) AS mean_eff
          FROM uses u
          LEFT JOIN linked lk ON lk.round_id = u.round_id
          LEFT JOIN gomining.player_snapshot_memberships pm_exact
            ON pm_exact.player_snapshot_id = lk.player_snapshot_id
           AND pm_exact.user_id = u.user_id
           AND pm_exact.nft_power IS NOT NULL
           AND pm_exact.nft_power > 0
           AND pm_exact.mean_energy_efficiency IS NOT NULL
           AND pm_exact.mean_energy_efficiency > 0
          LEFT JOIN LATERAL (
            SELECT m.nft_power, m.mean_energy_efficiency
            FROM gomining.player_snapshot_memberships m
            WHERE m.user_id = u.user_id
              AND m.league_id = %s
              AND m.nft_power IS NOT NULL
              AND m.nft_power > 0
              AND m.mean_energy_efficiency IS NOT NULL
              AND m.mean_energy_efficiency > 0
              AND (m.round_id IS NULL OR m.round_id <= u.round_id)
            ORDER BY m.round_id DESC NULLS LAST, m.created_at DESC
            LIMIT 1
          ) pm_league ON TRUE
          LEFT JOIN LATERAL (
            SELECT m.nft_power, m.mean_energy_efficiency
            FROM gomining.player_snapshot_memberships m
            WHERE m.user_id = u.user_id
              AND m.nft_power IS NOT NULL
              AND m.nft_power > 0
              AND m.mean_energy_efficiency IS NOT NULL
              AND m.mean_energy_efficiency > 0
              AND (m.round_id IS NULL OR m.round_id <= u.round_id)
            ORDER BY m.round_id DESC NULLS LAST, m.created_at DESC
            LIMIT 1
          ) pm_any ON TRUE
        )
        SELECT
          round_id,
          SUM(ability_count * ({PPS_FACTOR} * nft_power / mean_eff) * {POWER_UP_GMT_FACTOR})::numeric AS total_power_up_gmt
        FROM resolved
        WHERE nft_power IS NOT NULL AND mean_eff IS NOT NULL AND mean_eff > 0
        GROUP BY round_id
        """
        params: List[Any] = [league_id] + ids + [league_id] + ids + [league_id]
        rows = self.query(sql, params)
        out: Dict[int, float] = {}
        for r in rows:
            rid = safe_int(r.get("round_id"))
            val = safe_float(r.get("total_power_up_gmt"))
            if rid is None or val is None:
                continue
            out[rid] = val
        return out

    def fetch_clan_shield_rows_for_rounds(
        self,
        league_id: int,
        round_ids: Sequence[int],
        min_member_coverage: float = CLAN_EXACT_MEMBER_COVERAGE_THRESHOLD,
        min_power_coverage: float = CLAN_EXACT_POWER_COVERAGE_THRESHOLD,
        min_snapshot_coverage: float = CLAN_SNAPSHOT_MIN_COVERAGE,
        fallback_round_window: int = CLAN_SNAPSHOT_FALLBACK_ROUND_WINDOW,
    ) -> Dict[int, List[Dict[str, Any]]]:
        if not round_ids:
            return {}
        ids = sorted({int(x) for x in round_ids})
        placeholders = ", ".join(["%s"] * len(ids))
        round_window = max(0, int(fallback_round_window))
        window_lo = min(ids) - round_window
        window_hi = max(ids) + round_window
        sql = f"""
        WITH base_links AS (
          SELECT
            l.round_id,
            l.player_snapshot_id,
            l.player_snapshot_round_id,
            l.snapshot_calculated_at
          FROM gomining.round_player_snapshot_links l
          WHERE l.league_id = %s
            AND l.round_id IN ({placeholders})
            AND l.player_snapshot_id IS NOT NULL
        ),
        candidate_links AS (
          SELECT
            l.round_id,
            l.player_snapshot_id,
            l.player_snapshot_round_id,
            l.snapshot_calculated_at
          FROM gomining.round_player_snapshot_links l
          WHERE l.league_id = %s
            AND l.round_id BETWEEN %s AND %s
            AND l.player_snapshot_id IS NOT NULL
        ),
        candidate_snapshot_ids AS (
          SELECT DISTINCT player_snapshot_id FROM candidate_links
        ),
        snapshot_quality AS (
          SELECT
            s.player_snapshot_id,
            COALESCE(ct.members_total, 0)::numeric AS members_total,
            COALESCE(pm.members_seen, 0)::numeric AS members_seen,
            CASE
              WHEN COALESCE(ct.members_total, 0) > 0
                THEN COALESCE(pm.members_seen, 0)::numeric / ct.members_total
              ELSE 0::numeric
            END AS member_coverage
          FROM candidate_snapshot_ids s
          LEFT JOIN (
            SELECT
              t.player_snapshot_id,
              SUM(COALESCE(t.member_count, 0))::numeric AS members_total
            FROM gomining.player_snapshot_clan_totals t
            WHERE t.player_snapshot_id IN (SELECT player_snapshot_id FROM candidate_snapshot_ids)
            GROUP BY t.player_snapshot_id
          ) ct ON ct.player_snapshot_id = s.player_snapshot_id
          LEFT JOIN (
            SELECT
              m.player_snapshot_id,
              COUNT(*)::numeric AS members_seen
            FROM gomining.player_snapshot_memberships m
            WHERE m.player_snapshot_id IN (SELECT player_snapshot_id FROM candidate_snapshot_ids)
            GROUP BY m.player_snapshot_id
          ) pm ON pm.player_snapshot_id = s.player_snapshot_id
        ),
        effective_links AS (
          SELECT
            b.round_id,
            pick.player_snapshot_id,
            pick.player_snapshot_round_id,
            pick.snapshot_calculated_at
          FROM base_links b
          LEFT JOIN snapshot_quality bq
            ON bq.player_snapshot_id = b.player_snapshot_id
          JOIN LATERAL (
            SELECT
              c.player_snapshot_id,
              c.player_snapshot_round_id,
              c.snapshot_calculated_at
            FROM candidate_links c
            LEFT JOIN snapshot_quality q
              ON q.player_snapshot_id = c.player_snapshot_id
            ORDER BY
              CASE
                WHEN COALESCE(bq.member_coverage, 0) >= %s
                  THEN CASE WHEN c.player_snapshot_id = b.player_snapshot_id THEN 0 ELSE 1 END
                ELSE
                  CASE WHEN COALESCE(q.member_coverage, 0) >= %s THEN 0 ELSE 1 END
              END,
              ABS(c.round_id - b.round_id),
              CASE WHEN c.player_snapshot_id = b.player_snapshot_id THEN 0 ELSE 1 END,
              c.snapshot_calculated_at DESC
            LIMIT 1
          ) pick ON TRUE
        ),
        round_meta AS (
          SELECT
            round_id,
            MAX(player_snapshot_round_id) AS snapshot_round_id,
            MAX(snapshot_calculated_at) AS snapshot_ts
          FROM effective_links
          GROUP BY round_id
        ),
        eff AS (
          SELECT
            m.round_id,
            MAX(m.efficiency_league) AS efficiency_league
          FROM gomining.multiplier_snapshots m
          WHERE m.league_id = %s
            AND m.round_id IN ({placeholders})
          GROUP BY m.round_id
        ),
        ct AS (
          SELECT
            l.round_id,
            t.clan_id,
            SUM(COALESCE(t.member_count, 0))::numeric AS members_total,
            SUM(COALESCE(t.total_nft_power, 0))::numeric AS team_th
          FROM effective_links l
          JOIN gomining.player_snapshot_clan_totals t
            ON t.player_snapshot_id = l.player_snapshot_id
          GROUP BY l.round_id, t.clan_id
        ),
        pm AS (
          SELECT
            l.round_id,
            m.clan_id,
            COUNT(*)::numeric AS members_seen,
            SUM(COALESCE(m.nft_power, 0))::numeric AS power_seen,
            SUM(
              CASE
                WHEN m.nft_power IS NOT NULL
                     AND m.mean_energy_efficiency IS NOT NULL
                     AND m.mean_energy_efficiency > 0
                  THEN (m.nft_power / m.mean_energy_efficiency)
                ELSE 0
              END
            )::numeric AS sum_th_over_w
          FROM effective_links l
          JOIN gomining.player_snapshot_memberships m
            ON m.player_snapshot_id = l.player_snapshot_id
          GROUP BY l.round_id, m.clan_id
        ),
        cn AS (
          SELECT
            c.round_id AS snapshot_round_id,
            c.clan_id,
            MAX(NULLIF(c.clan_name, '')) AS clan_name
          FROM gomining.clan_snapshot_memberships c
          WHERE c.league_id = %s
            AND c.round_id IN (SELECT DISTINCT snapshot_round_id FROM round_meta)
          GROUP BY c.round_id, c.clan_id
        )
        SELECT
          ct.round_id,
          rm.snapshot_round_id,
          rm.snapshot_ts,
          ct.clan_id,
          COALESCE(cn.clan_name, 'clan-' || ct.clan_id::text) AS clan_name,
          ct.members_total,
          COALESCE(pm.members_seen, 0)::numeric AS members_seen,
          CASE
            WHEN ct.members_total > 0 THEN COALESCE(pm.members_seen, 0) / ct.members_total
            ELSE NULL
          END AS member_coverage,
          ct.team_th,
          COALESCE(pm.power_seen, 0)::numeric AS power_seen,
          CASE
            WHEN ct.team_th > 0 THEN COALESCE(pm.power_seen, 0) / ct.team_th
            ELSE NULL
          END AS power_coverage,
          CASE
            WHEN ct.members_total > 0
                 AND (COALESCE(pm.members_seen, 0) / ct.members_total) >= %s
                 AND ct.team_th > 0
                 AND (COALESCE(pm.power_seen, 0) / ct.team_th) >= %s
                 AND COALESCE(pm.sum_th_over_w, 0) > 0
              THEN ({PPS_FACTOR} * pm.sum_th_over_w)
            WHEN e.efficiency_league IS NOT NULL
                 AND e.efficiency_league > 0
                 AND ct.team_th > 0
              THEN ({PPS_FACTOR} * ct.team_th / e.efficiency_league)
            ELSE NULL
          END AS team_pps,
          CASE
            WHEN ct.members_total > 0
                 AND (COALESCE(pm.members_seen, 0) / ct.members_total) >= %s
                 AND ct.team_th > 0
                 AND (COALESCE(pm.power_seen, 0) / ct.team_th) >= %s
                 AND COALESCE(pm.sum_th_over_w, 0) > 0
              THEN 'player_exact'
            WHEN e.efficiency_league IS NOT NULL
                 AND e.efficiency_league > 0
                 AND ct.team_th > 0
              THEN 'league_fallback'
            ELSE 'missing'
          END AS calc_mode
        FROM ct
        JOIN round_meta rm ON rm.round_id = ct.round_id
        LEFT JOIN pm ON pm.round_id = ct.round_id AND pm.clan_id = ct.clan_id
        LEFT JOIN eff e ON e.round_id = ct.round_id
        LEFT JOIN cn ON cn.snapshot_round_id = rm.snapshot_round_id AND cn.clan_id = ct.clan_id
        ORDER BY ct.round_id ASC, team_pps DESC NULLS LAST, ct.clan_id ASC
        """
        params: List[Any] = (
            [league_id]
            + ids
            + [league_id, window_lo, window_hi, min_snapshot_coverage, min_snapshot_coverage]
            + [league_id]
            + ids
            + [league_id]
        )
        params.extend(
            [
                min_member_coverage,
                min_power_coverage,
                min_member_coverage,
                min_power_coverage,
            ]
        )
        rows = self.query(sql, params)
        out: Dict[int, List[Dict[str, Any]]] = {}
        for r in rows:
            rid = safe_int(r.get("round_id"))
            if rid is None:
                continue
            team_pps = safe_float(r.get("team_pps"))
            out.setdefault(rid, []).append(
                {
                    "round_id": rid,
                    "snapshot_round_id": safe_int(r.get("snapshot_round_id")),
                    "snapshot_ts": r.get("snapshot_ts"),
                    "clan_id": safe_int(r.get("clan_id")),
                    "clan_name": str(r.get("clan_name") or ""),
                    "members_total": safe_int(r.get("members_total")) or 0,
                    "members_seen": safe_int(r.get("members_seen")) or 0,
                    "member_coverage": safe_float(r.get("member_coverage")),
                    "team_th": safe_float(r.get("team_th")),
                    "team_pps": team_pps,
                    "clan_shield_gmt": calc_clan_shield_gmt(team_pps),
                    "calc_mode": str(r.get("calc_mode") or "missing"),
                }
            )
        return out


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
    if s == CLAN_SHEET_MARKER.lower():
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
    checked_history = enable_history_shift and (ctx.ws_id in MIGRATION_CHECKED_SHEETS)
    if enable_history_shift and LEAGUE_TH_HEADER in expected_header:
        old_expected = _expected_without_league_th(expected_header)
        if not header_ok and existing_header and _header_matches_existing(existing_header, old_expected):
            needs_history_shift = True
            shift_reason = "missing_league_th_header"
        elif header_ok and (not checked_history) and _history_shift_detected(ctx.ws, read_limiter):
            needs_history_shift = True
            shift_reason = "shifted_history_detected"

    if cached_ok and header_ok and not needs_history_shift:
        return
    if DRY_RUN:
        reason_parts: List[str] = []
        if not cached_ok:
            reason_parts.append("cache_mismatch")
        if not header_ok:
            reason_parts.append("header_mismatch")
        if needs_history_shift:
            reason_parts.append("history_shift")
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
        elif marker == CLAN_SHEET_MARKER:
            clan_contexts[ws.id] = SheetContext(
                ws_id=ws.id,
                title=ws.title,
                ws=ws,
                league_id=league_id,
                kind="clan",
                expected_cols=len(clan_expected_header),
                round_col_idx=2,
            )
        elif DEBUG_VERBOSE:
            log_debug("sheet.refresh_skip_unknown_marker", sheet=ws.title, ws_id=ws.id, marker=marker)

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


def build_canonical_row(
    rec: Dict[str, Any],
    ability_headers: List[str],
    counts_by_name: Dict[str, int],
    price_cutover_round: Optional[int],
    power_up_gmt_value: Optional[float] = None,
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

    power_up_gmt: Any = ""
    if round_id_num is not None and (price_cutover_round is None or round_id_num > price_cutover_round):
        # After cutover we persist explicit zero when no Power Up usage was observed.
        power_up_gmt = 0.0 if power_up_gmt_value is None else power_up_gmt_value
    row.append(power_up_gmt)
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
        clan_shield_gmt = safe_float(clan.get("clan_shield_gmt"))
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
                "" if clan_shield_gmt is None else clan_shield_gmt,
                str(clan.get("calc_mode") or "missing"),
            ]
        )
    return out


def enqueue_main_sheet_ops(
    db: DBClient,
    state: StateStore,
    contexts: Dict[int, SheetContext],
    ability_id_to_name: Dict[str, str],
    ability_headers: List[str],
) -> int:
    enqueued = 0
    for ws_id, ctx in contexts.items():
        state.upsert_sheet_meta(ws_id, ctx.title, ctx.league_id, layout_sig=None)
        last_synced = state.get_last_synced_round(ws_id)

        if last_synced is None:
            cutover = db.fetch_latest_completed_round_id(ctx.league_id)
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
        rounds = db.fetch_completed_rounds_from_db(ctx.league_id, since_round, MAX_ROUNDS_PER_POLL)
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
        counts_by_round = db.fetch_ability_counts_for_rounds(ctx.league_id, round_ids)
        power_up_gmt_by_round = db.fetch_power_up_gmt_sum_for_rounds(ctx.league_id, round_ids)

        for rec in rounds_sorted:
            rid = safe_int(rec.get("round_id"))
            if rid is None:
                continue

            counts_by_id = counts_by_round.get(rid, {})
            counts_by_name: Dict[str, int] = {}
            for aid, cnt in counts_by_id.items():
                aname = ability_id_to_name.get(aid)
                if not aname:
                    continue
                counts_by_name[aname] = counts_by_name.get(aname, 0) + int(cnt)

            row = build_canonical_row(
                rec,
                ability_headers,
                counts_by_name,
                price_cutover_round=price_cutover,
                power_up_gmt_value=safe_float(power_up_gmt_by_round.get(rid)),
            )
            checksum = row_checksum(row)
            finalized = 1 if rid <= (max_round - STABILIZATION_ROUNDS) else 0
            mapped = row_maps.get(rid)

            if mapped is None:
                if rid > last_synced:
                    state.enqueue_op(ws_id, "append_round", rid, checksum, {"row": row, "finalized": finalized})
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
                elif DEBUG_VERBOSE:
                    log_debug("sync.round_skip_no_map_old_round", sheet=ctx.title, round_id=rid, last_synced=last_synced)
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

    return enqueued


def enqueue_clan_sheet_ops(
    db: DBClient,
    state: StateStore,
    clan_contexts: Dict[int, SheetContext],
    clan_api: GoMiningClanApiClient,
) -> int:
    enqueued = 0
    for ws_id, ctx in clan_contexts.items():
        state.upsert_sheet_meta(ws_id, ctx.title, ctx.league_id, layout_sig=None)
        last_synced = state.get_last_synced_round(ws_id)

        if last_synced is None:
            cutover = db.fetch_latest_completed_round_id(ctx.league_id)
            if cutover is None:
                cutover = 0
            state.set_last_synced_round(ws_id, cutover)
            log_info("sync.clan_init_cutover", sheet=ctx.title, league_id=ctx.league_id, cutover_round=cutover)
            continue

        rounds = db.fetch_completed_rounds_from_db(ctx.league_id, max(0, last_synced), MAX_ROUNDS_PER_POLL)
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
            mapped = row_maps.get(rid)
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


def main() -> None:
    lock = SingleInstanceLock(LOCK_FILE_PATH)
    lock.acquire()
    state: Optional[StateStore] = None
    db: Optional[DBClient] = None
    try:
        db = DBClient()
        db.connect()

        catalog = db.load_ability_catalog()
        if not catalog:
            raise RuntimeError("No abilities found in ability_dim or abilities_snapshot_items")

        ability_id_to_name = build_ability_id_to_header(catalog)
        ability_headers = list(ABILITY_HEADER_ORDER)
        if not ability_id_to_name:
            raise RuntimeError("No matching boost abilities found in catalog for configured fixed header set.")
        log_info("sync.ability_mapping_loaded", mapped_ids=len(ability_id_to_name), columns=len(ability_headers))

        main_expected_header = BASE_HEADERS + ability_headers + [POWER_UP_PRICE_HEADER]
        clan_expected_header = list(CLAN_HEADERS)

        state = StateStore(STATE_DB_PATH)
        write_limiter = TokenBucket(GS_WRITE_REQ_PER_MIN, name="sheets_write")
        read_limiter = TokenBucket(GS_READ_REQ_PER_MIN, name="sheets_read")
        gomining_limiter = TokenBucket(GOMINING_API_REQ_PER_MIN, name="gomining_api")
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
        clan_api = GoMiningClanApiClient(
            effective_bearer,
            limiter=gomining_limiter,
            leaderboard_url=CLAN_LEADERBOARD_API_URL,
            clan_get_by_id_url=CLAN_GET_BY_ID_API_URL,
            page_limit=CLAN_API_PAGE_LIMIT,
            timeout_seconds=CLAN_API_TIMEOUT_SECONDS,
            max_retries=CLAN_API_MAX_RETRIES,
            token_fetcher=token_fetcher,
        )

        sh = open_spreadsheet()
        main_contexts, clan_contexts = refresh_sheet_contexts(
            sh,
            state,
            main_expected_header,
            clan_expected_header,
            write_limiter,
            read_limiter,
        )
        contexts_all = {**main_contexts, **clan_contexts}
        purge_stale_queue_ops(state, contexts_all)
        api_leagues = fetch_league_catalog_from_api(clan_api.bearer_token)

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
            clan_api_page_limit=CLAN_API_PAGE_LIMIT,
            clan_api_timeout_s=CLAN_API_TIMEOUT_SECONDS,
            clan_api_max_retries=CLAN_API_MAX_RETRIES,
            token_url_set=bool(TOKEN_URL),
            token_method=(TOKEN_METHOD if TOKEN_METHOD in {"GET", "POST"} else "GET"),
            token_verify_ssl=TOKEN_VERIFY_SSL,
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
        last_heartbeat = 0.0

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
                )
                contexts_all = {**main_contexts, **clan_contexts}
                purge_stale_queue_ops(state, contexts_all)
                last_refresh = now
                log_info("sync.sheet_refresh_done", active_main=len(main_contexts), active_clan=len(clan_contexts))

            if now - last_league_api_poll >= LEAGUES_API_POLL_SECONDS:
                api_leagues = fetch_league_catalog_from_api(clan_api.bearer_token)
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
                enq_main = enqueue_main_sheet_ops(db, state, main_contexts, ability_id_to_name, ability_headers)
                enq_clan = enqueue_clan_sheet_ops(db, state, clan_contexts, clan_api)
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
                last_heartbeat = now

            time.sleep(1.0)

    finally:
        try:
            if state is not None:
                state.close()
        except Exception:
            pass
        try:
            if db is not None:
                db.close()
        except Exception:
            pass
        lock.release()


if __name__ == "__main__":
    main()
