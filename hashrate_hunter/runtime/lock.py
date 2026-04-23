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

from ..logging_utils import log_warn
from ..utils import utc_now_iso

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
