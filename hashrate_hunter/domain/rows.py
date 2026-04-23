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

from ..config import BASE_HEADERS, CLAN_HEADERS, CLAN_POWER_UP_PRICE_HEADER, CLAN_POWER_UP_PRICE_SENTINEL_HEADER, EXCLUDED_USER_BOOST_AUDIT_HEADER, MISSING_HEADER, POWER_UP_PRICE_HEADER, POWER_UP_PRICE_SENTINEL_HEADER
from ..utils import safe_float, safe_int, to_iso_utc

def _normalize_payload_row(row: Any, expected_cols: int, round_id: int, round_col_idx: Optional[int] = 6) -> Optional[List[Any]]:
    if not isinstance(row, list):
        return None
    out = list(row[:expected_cols])
    if len(out) < expected_cols:
        out.extend([""] * (expected_cols - len(out)))
    if round_col_idx is not None and round_col_idx >= 0 and expected_cols > round_col_idx:
        out[round_col_idx] = round_id
    return out

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
