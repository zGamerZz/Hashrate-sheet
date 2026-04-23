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
from ..logging_utils import log_info, log_warn
from ..utils import safe_int

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
