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

from ..api.auth import _is_jwt_expired_response, _shared_token_cached, _shared_token_fetch
from ..config import *
from ..domain.pricing import calc_team_th_and_pps_from_users
from ..logging_utils import log_warn
from ..runtime.rate_limit import AdaptiveRateController, TokenBucket
from ..utils import safe_float, safe_int, utc_now_iso

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
