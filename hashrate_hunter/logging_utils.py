from __future__ import annotations

import json
import logging
from typing import Any, List

from .config import LOG_FILE_PATH, LOG_LEVEL

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
