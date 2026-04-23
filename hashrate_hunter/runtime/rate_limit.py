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

from ..config import ADAPTIVE_RPM_ENABLE, DEBUG_VERBOSE
from ..logging_utils import LOGGER, log_debug, log_info, log_warn

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
