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
from ..api.league_index import _to_api_calculated_at
from ..config import *
from ..logging_utils import log_info, log_warn
from ..runtime.rate_limit import AdaptiveRateController, TokenBucket
from ..utils import safe_float, safe_int, to_iso_utc, utc_now_iso

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
            "btc_fund": safe_float(clan_data.get("btcFund")),
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
