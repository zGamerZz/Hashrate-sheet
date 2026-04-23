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

from ..config import (
    CLAN_POWER_UP_GMT_FACTOR,
    POWER_UP_GMT_FACTOR,
    PPS_BASE_EE_W_PER_TH,
)
from ..logging_utils import log_warn
from ..utils import safe_float, safe_int

def calc_league_pps(league_th: Optional[float], efficiency_league: Optional[float]) -> Optional[float]:
    if league_th is None or efficiency_league is None or efficiency_league <= 0:
        return None
    return PPS_BASE_EE_W_PER_TH * league_th / efficiency_league

def calc_power_up_gmt(league_th: Optional[float], efficiency_league: Optional[float]) -> Optional[float]:
    pps = calc_league_pps(league_th, efficiency_league)
    if pps is None:
        return None
    return pps * POWER_UP_GMT_FACTOR

def round_gmt_2(value: float) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def has_sentinel_avatar_discount(avatar_url: Any) -> bool:
    s = str(avatar_url or "").strip()
    if not s:
        return False
    base = s.split("?", 1)[0].rstrip("/")
    m = re.search(r"(\d+)(?:\.[A-Za-z0-9]+)?$", base)
    if not m:
        return False
    try:
        n = int(m.group(1))
    except Exception:
        return False
    return 200 <= n <= 299

def has_sentinel_alias_discount(alias: Any) -> bool:
    s = str(alias or "").strip().lower()
    if not s:
        return False
    # Fallback marker used by some users/clans when avatar URL is unavailable.
    return ("sentinel" in s) or ("\U0001F5B2" in s)

def has_sentinel_user_discount(avatar_url: Any, alias: Any) -> bool:
    return has_sentinel_avatar_discount(avatar_url) or has_sentinel_alias_discount(alias)

def calc_boost_gmt_from_api_round(
    rec: Dict[str, Any],
    counts_by_name: Dict[str, int],
    boost_header: str,
) -> Optional[float]:
    boost_count = int(counts_by_name.get(boost_header, 0) or 0)
    if boost_count <= 0:
        return 0.0
    league_th = safe_float(rec.get("league_th"))
    efficiency_league = safe_float(rec.get("efficiency_league"))
    per_use = calc_power_up_gmt(league_th, efficiency_league)
    if per_use is None:
        return None
    return float(boost_count) * per_use

def calc_power_up_gmt_from_api_round(
    rec: Dict[str, Any],
    counts_by_name: Dict[str, int],
) -> Optional[float]:
    return calc_boost_gmt_from_api_round(rec, counts_by_name, "Power Up Boost")

def calc_boost_gmt_pair_from_boost_users_api(
    boost_users: Sequence[Dict[str, Any]],
    clan_api: Optional["GoMiningClanApiClient"],
) -> Tuple[Optional[float], Optional[float]]:
    normal, sentinel, _missing_aliases = calc_boost_gmt_triplet_from_boost_users_api(
        boost_users,
        clan_api,
    )
    return normal, sentinel

def calc_boost_gmt_triplet_from_boost_users_api(
    boost_users: Sequence[Dict[str, Any]],
    clan_api: Optional["GoMiningClanApiClient"],
    resolution_stats: Optional[Dict[str, int]] = None,
) -> Tuple[Optional[float], Optional[float], List[str]]:
    """
    Exact API-only calculation based on users who activated boost ability (boolean activation).
    Each user is counted once per round regardless of API count.
    Per-user price is rounded to 2 decimals before summation.
    Returns (normal, sentinel_adjusted) where sentinel-adjusted applies 20% discount
    for users with avatar id in [200..299] (or sentinel marker in alias).
    Returns (None, None, missing_aliases) when no user can be resolved.
    """
    by_user_usage: Dict[int, Dict[str, Any]] = {}
    clan_ids: List[int] = []
    needed_users_by_clan: Dict[int, set[int]] = {}
    for item in boost_users:
        if not isinstance(item, dict):
            continue
        uid = safe_int(item.get("user_id"))
        cnt = safe_int(item.get("count")) or 0
        cid = safe_int(item.get("clan_id"))
        avatar_url = str(item.get("avatar_url") or "")
        alias = str(item.get("alias") or "")
        if uid is None or cnt <= 0:
            continue
        usage = by_user_usage.get(uid)
        sentinel = has_sentinel_user_discount(avatar_url, alias)
        if usage is None:
            by_user_usage[uid] = {
                "clan_id": cid,
                "sentinel": sentinel,
                "alias": alias,
            }
        else:
            if usage.get("clan_id") is None and cid is not None:
                usage["clan_id"] = cid
            usage["sentinel"] = bool(usage.get("sentinel")) or sentinel
            if not str(usage.get("alias") or "").strip() and alias:
                usage["alias"] = alias
    if resolution_stats is not None:
        resolution_stats["resolved_api"] = 0
        resolution_stats["missing"] = 0
        resolution_stats["total_users"] = len(by_user_usage)

    if not by_user_usage:
        return 0.0, 0.0, []
    by_clan_user: Dict[Tuple[int, int], Tuple[float, float]] = {}
    by_user: Dict[int, Tuple[float, float]] = {}
    if clan_api is not None:
        for uid, usage in by_user_usage.items():
            cid = safe_int(usage.get("clan_id"))
            if cid is not None:
                clan_ids.append(cid)
                needed_users_by_clan.setdefault(cid, set()).add(uid)
        try:
            by_clan_user, by_user = clan_api.fetch_user_power_ee_for_clans(
                clan_ids,
                needed_users_by_clan=needed_users_by_clan,
            )
        except TypeError:
            # Backward-compatible fallback for stubs/tests without the new named arg.
            by_clan_user, by_user = clan_api.fetch_user_power_ee_for_clans(clan_ids)

    total_gmt = Decimal("0.00")
    total_gmt_sentinel = Decimal("0.00")
    resolved_users = 0
    missing_aliases: List[str] = []

    for uid, usage in by_user_usage.items():
        cid = safe_int(usage.get("clan_id"))
        sentinel = bool(usage.get("sentinel"))
        alias = str(usage.get("alias") or "").strip() or f"user_{uid}"
        power_ee: Optional[Tuple[float, float]] = None
        resolved_source: Optional[str] = None
        if cid is not None:
            power_ee = by_clan_user.get((cid, uid))
            if power_ee is not None:
                resolved_source = "api"
        if power_ee is None:
            power_ee = by_user.get(uid)
            if power_ee is not None:
                resolved_source = "api"
        if power_ee is None:
            missing_aliases.append(alias)
            continue
        power, ee = power_ee
        per_use = calc_power_up_gmt(power, ee)
        if per_use is None:
            missing_aliases.append(alias)
            continue
        user_price = round_gmt_2(float(per_use))
        total_gmt += user_price
        total_gmt_sentinel += round_gmt_2(float(user_price * Decimal("0.8"))) if sentinel else user_price
        resolved_users += 1
        if resolution_stats is not None:
            if resolved_source == "api":
                resolution_stats["resolved_api"] = int(resolution_stats.get("resolved_api", 0)) + 1

    if resolution_stats is not None:
        resolution_stats["missing"] = len(missing_aliases)

    if resolved_users <= 0:
        return None, None, missing_aliases
    return float(total_gmt), float(total_gmt_sentinel), missing_aliases

def calc_power_up_gmt_pair_from_power_up_users_api(
    power_up_users: Sequence[Dict[str, Any]],
    clan_api: Optional["GoMiningClanApiClient"],
) -> Tuple[Optional[float], Optional[float]]:
    return calc_boost_gmt_pair_from_boost_users_api(power_up_users, clan_api)

def calc_power_up_gmt_triplet_from_power_up_users_api(
    power_up_users: Sequence[Dict[str, Any]],
    clan_api: Optional["GoMiningClanApiClient"],
    resolution_stats: Optional[Dict[str, int]] = None,
) -> Tuple[Optional[float], Optional[float], List[str]]:
    return calc_boost_gmt_triplet_from_boost_users_api(
        power_up_users,
        clan_api,
        resolution_stats=resolution_stats,
    )

def calc_clan_power_up_gmt_pair_from_boost_users_api(
    clan_power_up_users: Sequence[Dict[str, Any]],
    clan_api: Optional["GoMiningClanApiClient"],
) -> Tuple[Optional[float], Optional[float]]:
    """
    Exact clan-based calculation for Clan Power Up:
      per_use_gmt(clan) = (SUM_over_clan_members(PPS_BASE_EE_W_PER_TH * power / ee)) * CLAN_POWER_UP_GMT_FACTOR
      total = SUM(per_user_once * per_use_gmt(user_clan))
    Sentinel-adjusted value applies 20% discount for users with avatar id in [200..299]
    (or sentinel marker in alias).
    Returns (None, None) when users exist but clan pricing cannot be fully resolved.
    """
    try:
        by_user_usage: Dict[int, Dict[str, Any]] = {}
        clan_ids: List[int] = []
        for item in clan_power_up_users:
            if not isinstance(item, dict):
                continue
            uid = safe_int(item.get("user_id"))
            cnt = safe_int(item.get("count")) or 0
            cid = safe_int(item.get("clan_id"))
            avatar_url = str(item.get("avatar_url") or "")
            alias = str(item.get("alias") or "")
            if uid is None or cnt <= 0:
                continue
            usage = by_user_usage.get(uid)
            sentinel = has_sentinel_user_discount(avatar_url, alias)
            if usage is None:
                by_user_usage[uid] = {
                    "clan_id": cid,
                    "sentinel": sentinel,
                }
            else:
                if usage.get("clan_id") is None and cid is not None:
                    usage["clan_id"] = cid
                usage["sentinel"] = bool(usage.get("sentinel")) or sentinel
        if not by_user_usage:
            return 0.0, 0.0
        for usage in by_user_usage.values():
            cid = safe_int(usage.get("clan_id"))
            if cid is not None:
                clan_ids.append(cid)
        if clan_api is None:
            return None, None
        team_pps_by_clan = clan_api.fetch_clan_team_pps_for_clans(clan_ids)
        if not team_pps_by_clan:
            return None, None

        total_gmt = Decimal("0.00")
        total_gmt_sentinel = Decimal("0.00")
        resolved_users = 0
        for _uid, usage in by_user_usage.items():
            cid = safe_int(usage.get("clan_id"))
            sentinel = bool(usage.get("sentinel"))
            if cid is None:
                continue
            team_pps = safe_float(team_pps_by_clan.get(cid))
            if team_pps is None or team_pps <= 0:
                continue
            per_use = calc_clan_power_up_gmt(team_pps)
            if per_use is None:
                continue
            user_price = round_gmt_2(float(per_use))
            total_gmt += user_price
            total_gmt_sentinel += round_gmt_2(float(user_price * Decimal("0.8"))) if sentinel else user_price
            resolved_users += 1
        if resolved_users <= 0:
            return None, None
        return float(total_gmt), float(total_gmt_sentinel)
    except Exception as e:
        log_warn(
            "sync.clan_power_up_calc_failed",
            err=repr(e),
            users=len([u for u in clan_power_up_users if isinstance(u, dict)]),
        )
        return None, None

def calc_team_pps_exact(sum_th_over_w: Optional[float]) -> Optional[float]:
    if sum_th_over_w is None or sum_th_over_w <= 0:
        return None
    return PPS_BASE_EE_W_PER_TH * sum_th_over_w

def calc_team_pps_fallback(team_th: Optional[float], efficiency_league: Optional[float]) -> Optional[float]:
    if team_th is None or team_th <= 0:
        return None
    return calc_league_pps(team_th, efficiency_league)

def calc_clan_power_up_gmt(team_pps: Optional[float]) -> Optional[float]:
    if team_pps is None:
        return None
    return team_pps * CLAN_POWER_UP_GMT_FACTOR

def calc_team_th_and_pps_from_users(users: Sequence[Dict[str, Any]]) -> Tuple[float, float]:
    team_th = 0.0
    team_pps = 0.0
    for user in users:
        if not isinstance(user, dict):
            continue
        pwr = safe_float(user.get("power"))
        ee = safe_float(user.get("ee"))
        if pwr is None or pwr <= 0:
            continue
        team_th += pwr
        if ee is None or ee <= 0:
            continue
        team_pps += PPS_BASE_EE_W_PER_TH * pwr / ee
    return team_th, team_pps
