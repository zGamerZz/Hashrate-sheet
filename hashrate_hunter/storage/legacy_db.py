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
