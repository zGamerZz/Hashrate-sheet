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
from ..logging_utils import log_warn
from ..runtime.rate_limit import AdaptiveRateController, TokenBucket
from ..utils import safe_float, safe_int

def _get_nested_value(obj: Any, path: Sequence[str]) -> Any:
    cur = obj
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur

def _first_safe_int_from_paths(obj: Any, paths: Sequence[Sequence[str]]) -> Optional[int]:
    for path in paths:
        val = _get_nested_value(obj, path)
        out = safe_int(val)
        if out is not None:
            return out
    return None

def _extract_participant_user_id(participant: Dict[str, Any]) -> Optional[int]:
    return _first_safe_int_from_paths(
        participant,
        (
            ("user", "id"),
            ("user", "userId"),
            ("user", "user_id"),
            ("user", "profile", "id"),
            ("user", "profile", "userId"),
            ("participant", "userId"),
            ("participant", "user", "id"),
            ("miner", "userId"),
            ("miner", "user", "id"),
            ("userId",),
            ("user_id",),
        ),
    )

class GoMiningRoundAbilityApiClient:
    def __init__(
        self,
        bearer_token: str,
        limiter: TokenBucket,
        user_leaderboard_url: str = ROUND_USER_LEADERBOARD_API_URL,
        player_leaderboard_url: str = f"{GOMINING_API_BASE_URL}{PLAYER_LEADERBOARD_PATH}",
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
        self.player_leaderboard_url = player_leaderboard_url
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
        self.round_tracked_user_blocks_mined_cache: Dict[int, Optional[float]] = {}

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

    def _post_json_with_retry(
        self,
        payload: Dict[str, Any],
        *,
        url: Optional[str] = None,
        **ctx: Any,
    ) -> Optional[Dict[str, Any]]:
        self._refresh_bearer(force=False)
        request_url = str(url or self.user_leaderboard_url)
        delay_s = 0.6
        for attempt in range(1, self.max_retries + 1):
            self.limiter.wait_for_token(1.0)
            try:
                resp = self.session.post(
                    request_url,
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
            and rid in self.round_tracked_user_blocks_mined_cache
            and safe_float(self.round_tracked_user_blocks_mined_cache.get(rid)) is not None
        ):
            return dict(cached_counts)
        skip = 0
        fetched = 0
        total_count: Optional[int] = None
        page_no = 0
        max_pages = 10000
        counts: Dict[str, int] = {}
        excluded_user_counts: Dict[str, int] = {}
        tracked_user_blocks_mined: Optional[float] = None
        winner_user_id: Optional[int] = None
        round_multiplier: Optional[float] = None
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
                    self.round_ability_users_cache[rid] = {POWER_UP_ABILITY_ID: [], CLAN_POWER_UP_ABILITY_ID: []}
                    self.round_counts_cache[rid] = {}
                    self.round_excluded_user_boosts_cache[rid] = {}
                    self.round_tracked_user_blocks_mined_cache[rid] = None
                    return {}

            if winner_user_id is None:
                winner = data.get("winner")
                if isinstance(winner, dict):
                    winner_user_id = _extract_participant_user_id(winner)
            if round_multiplier is None:
                round_multiplier = safe_float(data.get("multiplier"))

            for participant in participants:
                if not isinstance(participant, dict):
                    continue
                participant_avatar_url = ""
                participant_alias = ""
                participant_user_id = _extract_participant_user_id(participant)
                user_raw = participant.get("user")
                if isinstance(user_raw, dict):
                    participant_avatar_url = str(user_raw.get("avatar") or "")
                    participant_alias = str(user_raw.get("alias") or "")
                participant_clan_id = None
                clan_raw = participant.get("clan")
                if isinstance(clan_raw, dict):
                    participant_clan_id = safe_int(clan_raw.get("id"))
                if participant_clan_id is None:
                    participant_clan_id = safe_int(participant.get("clanId"))
                if participant_user_id == EXCLUDED_BOOST_USER_ID:
                    tracked_user_blocks_mined = safe_float(participant.get("blocksMined"))
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
        if (
            tracked_user_blocks_mined is not None
            and winner_user_id == EXCLUDED_BOOST_USER_ID
            and round_multiplier is not None
        ):
            tracked_user_blocks_mined += round_multiplier
        self.round_tracked_user_blocks_mined_cache[rid] = tracked_user_blocks_mined
        if len(self.round_tracked_user_blocks_mined_cache) > 512:
            for old_rid in sorted(self.round_tracked_user_blocks_mined_cache.keys())[:-256]:
                self.round_tracked_user_blocks_mined_cache.pop(old_rid, None)
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

    def get_cached_tracked_user_blocks_mined_for_round(self, round_id: int) -> Optional[float]:
        rid = int(round_id)
        if rid not in self.round_tracked_user_blocks_mined_cache:
            return None
        return safe_float(self.round_tracked_user_blocks_mined_cache.get(rid))

    def fetch_tracked_user_blocks_mined_for_round(
        self,
        league_id: int,
        round_id: int,
        calculated_at: str,
        *,
        user_id: int = EXCLUDED_BOOST_USER_ID,
        progress_hook: Optional[Callable[[], Any]] = None,
    ) -> Optional[float]:
        rid = int(round_id)
        if rid in self.round_tracked_user_blocks_mined_cache:
            cached = safe_float(self.round_tracked_user_blocks_mined_cache.get(rid))
            if cached is not None:
                return cached

        calc = str(calculated_at or "").strip()
        if not calc:
            self.round_tracked_user_blocks_mined_cache[rid] = None
            return None

        skip = 0
        total_count: Optional[int] = None
        fetched = 0
        page_no = 0
        max_pages = 10000
        target_user_id = int(user_id)

        while True:
            if progress_hook is not None:
                try:
                    progress_hook()
                except Exception as e:
                    log_warn(
                        "gomining_api.player_leaderboard_progress_hook_failed",
                        league_id=league_id,
                        round_id=rid,
                        skip=skip,
                        err=repr(e),
                    )
            payload = {
                "calculatedAt": calc,
                "pagination": {"skip": skip, "limit": self.page_limit},
                "leagueId": int(league_id),
            }
            body = self._post_json_with_retry(
                payload,
                url=self.player_leaderboard_url,
                league_id=league_id,
                round_id=rid,
                skip=skip,
                op="player_leaderboard_index",
            )
            if body is None:
                return None
            data = body.get("data")
            if not isinstance(data, dict):
                log_warn("gomining_api.player_leaderboard_bad_shape", league_id=league_id, round_id=rid, skip=skip)
                return None
            participants = data.get("participants")
            if not isinstance(participants, list):
                log_warn(
                    "gomining_api.player_leaderboard_participants_bad_shape",
                    league_id=league_id,
                    round_id=rid,
                    skip=skip,
                )
                return None
            if total_count is None:
                total_count = safe_int(data.get("count"))
                if total_count is None or total_count < 0:
                    log_warn(
                        "gomining_api.player_leaderboard_count_invalid",
                        league_id=league_id,
                        round_id=rid,
                        count=data.get("count"),
                        skip=skip,
                    )
                    return None

            for participant in participants:
                if not isinstance(participant, dict):
                    continue
                participant_user_id = _extract_participant_user_id(participant)
                if participant_user_id != target_user_id:
                    continue
                blocks_mined = safe_float(participant.get("blocksMined"))
                self.round_tracked_user_blocks_mined_cache[rid] = blocks_mined
                return blocks_mined

            fetched += len(participants)
            if len(participants) < self.page_limit:
                break
            if total_count is not None and fetched >= total_count:
                break
            skip += self.page_limit
            page_no += 1
            if page_no >= max_pages:
                log_warn("gomining_api.player_leaderboard_pagination_guard", league_id=league_id, round_id=rid, pages=page_no)
                return None

        self.round_tracked_user_blocks_mined_cache[rid] = None
        return None
