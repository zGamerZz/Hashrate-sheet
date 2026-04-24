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
from ..logging_utils import log_debug
from ..utils import safe_float, safe_int, to_iso_utc, utc_now_iso

class StateStore:
    def __init__(self, path: str) -> None:
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

    def close(self) -> None:
        self.conn.close()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS sheet_state (
              sheet_id INTEGER PRIMARY KEY,
              sheet_title TEXT NOT NULL,
              league_id INTEGER,
              last_synced_round INTEGER,
              price_cutover_round INTEGER,
              layout_sig TEXT,
              updated_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS round_row_map (
              sheet_id INTEGER NOT NULL,
              round_id INTEGER NOT NULL,
              row_idx INTEGER NOT NULL,
              checksum TEXT NOT NULL,
              finalized INTEGER NOT NULL DEFAULT 0,
              updated_at REAL NOT NULL,
              PRIMARY KEY (sheet_id, round_id)
            );

            CREATE TABLE IF NOT EXISTS pending_ops (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              sheet_id INTEGER NOT NULL,
              op_type TEXT NOT NULL,
              round_id INTEGER NOT NULL,
              checksum TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              retry_count INTEGER NOT NULL DEFAULT 0,
              next_attempt_ts REAL NOT NULL,
              created_at REAL NOT NULL
            );

            CREATE UNIQUE INDEX IF NOT EXISTS ux_pending_op_round ON pending_ops(sheet_id, op_type, round_id);
            CREATE INDEX IF NOT EXISTS ix_pending_due ON pending_ops(next_attempt_ts, id);
            CREATE INDEX IF NOT EXISTS ix_rowmap ON round_row_map(sheet_id, round_id DESC);

            CREATE TABLE IF NOT EXISTS api_round_cache (
              league_id INTEGER NOT NULL,
              round_id INTEGER NOT NULL,
              snapshot_ts TEXT,
              block_number INTEGER,
              multiplier REAL,
              gmt_fund REAL,
              gmt_per_block REAL,
              league_th REAL,
              blocks_mined INTEGER,
              efficiency_league REAL,
              btc_fund REAL,
              ended_at TEXT,
              round_duration_sec INTEGER,
              source TEXT NOT NULL DEFAULT 'api_triplet',
              updated_at REAL NOT NULL,
              PRIMARY KEY (league_id, round_id)
            );
            CREATE INDEX IF NOT EXISTS ix_api_round_cache_lookup
              ON api_round_cache(league_id, round_id DESC);

            CREATE TABLE IF NOT EXISTS round_processing_state (
              league_id INTEGER NOT NULL,
              round_id INTEGER NOT NULL,
              metrics_status TEXT NOT NULL DEFAULT 'pending',
              abilities_status TEXT NOT NULL DEFAULT 'pending',
              sheet_status TEXT NOT NULL DEFAULT 'pending',
              last_error TEXT,
              retry_after_ts REAL NOT NULL DEFAULT 0,
              attempt_count INTEGER NOT NULL DEFAULT 0,
              updated_at REAL NOT NULL,
              PRIMARY KEY (league_id, round_id)
            );
            CREATE INDEX IF NOT EXISTS ix_round_processing_retry
              ON round_processing_state(retry_after_ts, league_id, round_id);
            """
        )
        self._ensure_sheet_state_columns()
        self._ensure_api_round_cache_columns()
        self.conn.commit()

    def _ensure_sheet_state_columns(self) -> None:
        rows = self.conn.execute("PRAGMA table_info(sheet_state)").fetchall()
        cols = {str(r["name"]) for r in rows}
        if "price_cutover_round" not in cols:
            self.conn.execute("ALTER TABLE sheet_state ADD COLUMN price_cutover_round INTEGER")

    def _ensure_api_round_cache_columns(self) -> None:
        rows = self.conn.execute("PRAGMA table_info(api_round_cache)").fetchall()
        cols = {str(r["name"]) for r in rows}
        if "btc_fund" not in cols:
            self.conn.execute("ALTER TABLE api_round_cache ADD COLUMN btc_fund REAL")

    def get_sheet_state(self, sheet_id: int) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT sheet_id,sheet_title,league_id,last_synced_round,price_cutover_round,layout_sig "
            "FROM sheet_state WHERE sheet_id=?",
            (sheet_id,),
        ).fetchone()
        return dict(row) if row else None

    def upsert_sheet_meta(self, sheet_id: int, sheet_title: str, league_id: Optional[int], layout_sig: Optional[str] = None) -> None:
        now = time.time()
        self.conn.execute(
            """
            INSERT INTO sheet_state(sheet_id,sheet_title,league_id,last_synced_round,price_cutover_round,layout_sig,updated_at)
            VALUES(?,?,?,NULL,NULL,?,?)
            ON CONFLICT(sheet_id) DO UPDATE SET
              sheet_title=excluded.sheet_title,
              league_id=excluded.league_id,
              layout_sig=COALESCE(excluded.layout_sig, sheet_state.layout_sig),
              updated_at=excluded.updated_at
            """,
            (sheet_id, sheet_title, league_id, layout_sig, now),
        )
        self.conn.commit()

    def get_last_synced_round(self, sheet_id: int) -> Optional[int]:
        row = self.conn.execute("SELECT last_synced_round FROM sheet_state WHERE sheet_id=?", (sheet_id,)).fetchone()
        return safe_int(row[0]) if row else None

    def set_last_synced_round(self, sheet_id: int, round_id: int) -> None:
        cur = self.get_last_synced_round(sheet_id)
        if cur is not None and round_id <= cur:
            return
        self.conn.execute(
            "UPDATE sheet_state SET last_synced_round=?, updated_at=? WHERE sheet_id=?",
            (round_id, time.time(), sheet_id),
        )
        self.conn.commit()

    def get_price_cutover_round(self, sheet_id: int) -> Optional[int]:
        row = self.conn.execute("SELECT price_cutover_round FROM sheet_state WHERE sheet_id=?", (sheet_id,)).fetchone()
        return safe_int(row[0]) if row else None

    def set_price_cutover_round(self, sheet_id: int, round_id: int) -> None:
        cur = self.get_price_cutover_round(sheet_id)
        if cur is not None:
            return
        self.conn.execute(
            "UPDATE sheet_state SET price_cutover_round=?, updated_at=? WHERE sheet_id=?",
            (round_id, time.time(), sheet_id),
        )
        self.conn.commit()

    def upsert_round_processing_state(
        self,
        league_id: int,
        round_id: int,
        *,
        metrics_status: Optional[str] = None,
        abilities_status: Optional[str] = None,
        sheet_status: Optional[str] = None,
        last_error: Optional[str] = None,
        retry_after_ts: Optional[float] = None,
        attempt_count: Optional[int] = None,
    ) -> None:
        lid = safe_int(league_id)
        rid = safe_int(round_id)
        if lid is None or rid is None:
            return
        prev = self.get_round_processing_state(lid, rid) or {}
        now = time.time()
        self.conn.execute(
            """
            INSERT INTO round_processing_state(
              league_id,round_id,metrics_status,abilities_status,sheet_status,last_error,retry_after_ts,attempt_count,updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(league_id,round_id) DO UPDATE SET
              metrics_status=excluded.metrics_status,
              abilities_status=excluded.abilities_status,
              sheet_status=excluded.sheet_status,
              last_error=excluded.last_error,
              retry_after_ts=excluded.retry_after_ts,
              attempt_count=excluded.attempt_count,
              updated_at=excluded.updated_at
            """,
            (
                lid,
                rid,
                str(metrics_status or prev.get("metrics_status") or "pending"),
                str(abilities_status or prev.get("abilities_status") or "pending"),
                str(sheet_status or prev.get("sheet_status") or "pending"),
                (str(last_error)[:4000] if last_error else prev.get("last_error")),
                float(retry_after_ts if retry_after_ts is not None else prev.get("retry_after_ts") or 0.0),
                int(attempt_count if attempt_count is not None else prev.get("attempt_count") or 0),
                now,
            ),
        )
        self.conn.commit()

    def get_round_processing_state(self, league_id: int, round_id: int) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            """
            SELECT league_id,round_id,metrics_status,abilities_status,sheet_status,last_error,retry_after_ts,attempt_count,updated_at
            FROM round_processing_state
            WHERE league_id=? AND round_id=?
            """,
            (league_id, round_id),
        ).fetchone()
        return dict(row) if row else None

    def list_retryable_round_states(self, league_id: int, limit: int = 200) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT league_id,round_id,metrics_status,abilities_status,sheet_status,last_error,retry_after_ts,attempt_count,updated_at
            FROM round_processing_state
            WHERE league_id=?
              AND retry_after_ts<=?
              AND (metrics_status!='ok' OR abilities_status!='ok' OR sheet_status!='ok')
            ORDER BY round_id ASC
            LIMIT ?
            """,
            (league_id, time.time(), max(1, int(limit))),
        ).fetchall()
        return [dict(r) for r in rows]

    def count_missing_rows_for_range(self, sheet_id: int, league_id: int, from_round: int, to_round: int) -> int:
        row = self.conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM api_round_cache a
            LEFT JOIN round_row_map m
              ON m.sheet_id=? AND m.round_id=a.round_id
            WHERE a.league_id=?
              AND a.round_id>=?
              AND a.round_id<=?
              AND a.ended_at IS NOT NULL
              AND m.round_id IS NULL
            """,
            (sheet_id, league_id, from_round, to_round),
        ).fetchone()
        return int(row["c"]) if row else 0

    def list_missing_row_map_round_ids(
        self,
        sheet_id: int,
        league_id: int,
        from_round: int,
        limit: int = 200,
    ) -> List[int]:
        rows = self.conn.execute(
            """
            SELECT a.round_id
            FROM api_round_cache a
            LEFT JOIN round_row_map m
              ON m.sheet_id=? AND m.round_id=a.round_id
            WHERE a.league_id=?
              AND a.round_id>=?
              AND a.ended_at IS NOT NULL
              AND m.round_id IS NULL
            ORDER BY a.round_id ASC
            LIMIT ?
            """,
            (sheet_id, league_id, max(0, int(from_round)), max(1, int(limit))),
        ).fetchall()
        out: List[int] = []
        for r in rows:
            rid = safe_int(r["round_id"])
            if rid is not None and rid > 0:
                out.append(int(rid))
        return out

    def fetch_api_round_records_by_ids(self, league_id: int, round_ids: Sequence[int]) -> List[Dict[str, Any]]:
        ids = sorted({int(x) for x in round_ids if safe_int(x) is not None and int(x) > 0})
        if not ids:
            return []
        placeholders = ",".join(["?"] * len(ids))
        rows = self.conn.execute(
            f"""
            SELECT
              snapshot_ts,
              league_id,
              round_id,
              block_number,
              multiplier,
              gmt_fund,
              gmt_per_block,
              league_th,
              blocks_mined,
              efficiency_league,
              btc_fund,
              ended_at,
              round_duration_sec
            FROM api_round_cache
            WHERE league_id=?
              AND ended_at IS NOT NULL
              AND round_id IN ({placeholders})
            ORDER BY round_id ASC
            """,
            tuple([int(league_id)] + ids),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_round_row_map_bulk(self, sheet_id: int, round_ids: Sequence[int]) -> Dict[int, Dict[str, Any]]:
        if not round_ids:
            return {}
        ids = sorted({int(x) for x in round_ids})
        placeholders = ",".join(["?"] * len(ids))
        rows = self.conn.execute(
            f"SELECT round_id,row_idx,checksum,finalized FROM round_row_map WHERE sheet_id=? AND round_id IN ({placeholders})",
            tuple([sheet_id] + ids),
        ).fetchall()
        out: Dict[int, Dict[str, Any]] = {}
        for r in rows:
            rid = safe_int(r["round_id"])
            if rid is None:
                continue
            out[rid] = {
                "row_idx": safe_int(r["row_idx"]),
                "checksum": str(r["checksum"]),
                "finalized": int(r["finalized"]),
            }
        return out

    def get_max_row_idx(self, sheet_id: int) -> Optional[int]:
        row = self.conn.execute(
            "SELECT MAX(row_idx) AS max_row_idx FROM round_row_map WHERE sheet_id=?",
            (sheet_id,),
        ).fetchone()
        if not row:
            return None
        return safe_int(row["max_row_idx"])

    def upsert_row_map(self, sheet_id: int, round_id: int, row_idx: int, checksum: str, finalized: int) -> None:
        self.conn.execute(
            """
            INSERT INTO round_row_map(sheet_id,round_id,row_idx,checksum,finalized,updated_at)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(sheet_id,round_id) DO UPDATE SET
              row_idx=excluded.row_idx,
              checksum=excluded.checksum,
              finalized=excluded.finalized,
              updated_at=excluded.updated_at
            """,
            (sheet_id, round_id, row_idx, checksum, finalized, time.time()),
        )
        self.conn.commit()

    def prune_row_map(self, sheet_id: int, keep_rows: int = ROW_MAP_KEEP_PER_SHEET) -> None:
        self.conn.execute(
            """
            DELETE FROM round_row_map
            WHERE sheet_id=?
              AND round_id NOT IN (
                SELECT round_id FROM round_row_map WHERE sheet_id=? ORDER BY round_id DESC LIMIT ?
              )
            """,
            (sheet_id, sheet_id, keep_rows),
        )
        self.conn.commit()

    def enqueue_op(self, sheet_id: int, op_type: str, round_id: int, checksum: str, payload: Dict[str, Any]) -> None:
        now = time.time()
        payload_json = json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
        self.conn.execute(
            """
            INSERT INTO pending_ops(sheet_id,op_type,round_id,checksum,payload_json,retry_count,next_attempt_ts,created_at)
            VALUES(?,?,?,?,?,0,?,?)
            ON CONFLICT(sheet_id,op_type,round_id) DO UPDATE SET
              checksum=excluded.checksum,
              payload_json=excluded.payload_json,
              next_attempt_ts=CASE
                WHEN pending_ops.next_attempt_ts > excluded.next_attempt_ts THEN excluded.next_attempt_ts
                ELSE pending_ops.next_attempt_ts
              END
            """,
            (sheet_id, op_type, round_id, checksum, payload_json, now, now),
        )
        self.conn.commit()
        if DEBUG_VERBOSE:
            preview = payload_json[:QUEUE_DEBUG_PREVIEW]
            log_debug(
                "queue.op_enqueued",
                sheet_id=sheet_id,
                op_type=op_type,
                round_id=round_id,
                checksum=checksum[:12],
                payload_preview=preview,
            )

    def fetch_due_ops(self, limit: int = QUEUE_FETCH_LIMIT) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT id,sheet_id,op_type,round_id,checksum,payload_json,retry_count,next_attempt_ts,created_at "
            "FROM pending_ops WHERE next_attempt_ts<=? ORDER BY next_attempt_ts ASC, id ASC LIMIT ?",
            (time.time(), limit),
        ).fetchall()
        out = [dict(r) for r in rows]
        if DEBUG_VERBOSE:
            log_debug("queue.fetch_due", limit=limit, due=len(out))
        return out

    def purge_ops_for_missing_sheets(self, active_sheet_ids: Sequence[int]) -> int:
        ids = sorted({int(x) for x in active_sheet_ids})
        if not ids:
            return 0
        placeholders = ",".join(["?"] * len(ids))
        cur = self.conn.execute(
            f"DELETE FROM pending_ops WHERE sheet_id NOT IN ({placeholders})",
            tuple(ids),
        )
        self.conn.commit()
        return int(cur.rowcount or 0)

    def purge_ops_by_type(self, op_types: Sequence[str]) -> int:
        types = [str(x).strip() for x in op_types if str(x).strip()]
        if not types:
            return 0
        placeholders = ",".join(["?"] * len(types))
        cur = self.conn.execute(
            f"DELETE FROM pending_ops WHERE op_type IN ({placeholders})",
            tuple(types),
        )
        self.conn.commit()
        return int(cur.rowcount or 0)

    def delete_op(self, op_id: int) -> None:
        self.conn.execute("DELETE FROM pending_ops WHERE id=?", (op_id,))
        self.conn.commit()
        if DEBUG_VERBOSE:
            log_debug("queue.op_deleted", op_id=op_id)

    def reschedule_op(self, op_id: int, retry_count: int, next_attempt_ts: float) -> None:
        self.conn.execute(
            "UPDATE pending_ops SET retry_count=?, next_attempt_ts=? WHERE id=?",
            (retry_count, next_attempt_ts, op_id),
        )
        self.conn.commit()
        if DEBUG_VERBOSE:
            log_debug("queue.op_rescheduled", op_id=op_id, retry_count=retry_count, next_attempt_ts=round(next_attempt_ts, 3))

    def queue_total_count(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) AS c FROM pending_ops").fetchone()
        return int(row["c"]) if row else 0

    def queue_due_count(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) AS c FROM pending_ops WHERE next_attempt_ts<=?", (time.time(),)).fetchone()
        return int(row["c"]) if row else 0

    def upsert_api_round_record(self, rec: Dict[str, Any], source: str = "api_triplet") -> None:
        league_id = safe_int(rec.get("league_id"))
        round_id = safe_int(rec.get("round_id"))
        if league_id is None or round_id is None:
            return
        self.conn.execute(
            """
            INSERT INTO api_round_cache(
              league_id, round_id, snapshot_ts, block_number, multiplier, gmt_fund, gmt_per_block,
              league_th, blocks_mined, efficiency_league, btc_fund, ended_at, round_duration_sec, source, updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(league_id, round_id) DO UPDATE SET
              snapshot_ts=COALESCE(excluded.snapshot_ts, api_round_cache.snapshot_ts),
              block_number=COALESCE(excluded.block_number, api_round_cache.block_number),
              multiplier=COALESCE(excluded.multiplier, api_round_cache.multiplier),
              gmt_fund=COALESCE(excluded.gmt_fund, api_round_cache.gmt_fund),
              gmt_per_block=COALESCE(excluded.gmt_per_block, api_round_cache.gmt_per_block),
              league_th=COALESCE(excluded.league_th, api_round_cache.league_th),
              blocks_mined=COALESCE(excluded.blocks_mined, api_round_cache.blocks_mined),
              efficiency_league=COALESCE(excluded.efficiency_league, api_round_cache.efficiency_league),
              btc_fund=COALESCE(excluded.btc_fund, api_round_cache.btc_fund),
              ended_at=COALESCE(excluded.ended_at, api_round_cache.ended_at),
              round_duration_sec=COALESCE(excluded.round_duration_sec, api_round_cache.round_duration_sec),
              source=excluded.source,
              updated_at=excluded.updated_at
            """,
            (
                league_id,
                round_id,
                to_iso_utc(rec.get("snapshot_ts")),
                safe_int(rec.get("block_number")),
                safe_float(rec.get("multiplier")),
                safe_float(rec.get("gmt_fund")),
                safe_float(rec.get("gmt_per_block")),
                safe_float(rec.get("league_th")),
                safe_int(rec.get("blocks_mined")),
                safe_float(rec.get("efficiency_league")),
                safe_float(rec.get("btc_fund")),
                to_iso_utc(rec.get("ended_at")),
                safe_int(rec.get("round_duration_sec")),
                str(source or "api_triplet"),
                time.time(),
            ),
        )
        self.conn.commit()

    def upsert_api_round_records(self, records: Sequence[Dict[str, Any]], source: str = "api_triplet") -> int:
        cnt = 0
        for rec in records:
            if not isinstance(rec, dict):
                continue
            league_id = safe_int(rec.get("league_id"))
            round_id = safe_int(rec.get("round_id"))
            if league_id is None or round_id is None:
                continue
            self.conn.execute(
                """
                INSERT INTO api_round_cache(
                  league_id, round_id, snapshot_ts, block_number, multiplier, gmt_fund, gmt_per_block,
                  league_th, blocks_mined, efficiency_league, btc_fund, ended_at, round_duration_sec, source, updated_at
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(league_id, round_id) DO UPDATE SET
                  snapshot_ts=COALESCE(excluded.snapshot_ts, api_round_cache.snapshot_ts),
                  block_number=COALESCE(excluded.block_number, api_round_cache.block_number),
                  multiplier=COALESCE(excluded.multiplier, api_round_cache.multiplier),
                  gmt_fund=COALESCE(excluded.gmt_fund, api_round_cache.gmt_fund),
                  gmt_per_block=COALESCE(excluded.gmt_per_block, api_round_cache.gmt_per_block),
                  league_th=COALESCE(excluded.league_th, api_round_cache.league_th),
                  blocks_mined=COALESCE(excluded.blocks_mined, api_round_cache.blocks_mined),
                  efficiency_league=COALESCE(excluded.efficiency_league, api_round_cache.efficiency_league),
                  btc_fund=COALESCE(excluded.btc_fund, api_round_cache.btc_fund),
                  ended_at=COALESCE(excluded.ended_at, api_round_cache.ended_at),
                  round_duration_sec=COALESCE(excluded.round_duration_sec, api_round_cache.round_duration_sec),
                  source=excluded.source,
                  updated_at=excluded.updated_at
                """,
                (
                    league_id,
                    round_id,
                    to_iso_utc(rec.get("snapshot_ts")),
                    safe_int(rec.get("block_number")),
                    safe_float(rec.get("multiplier")),
                    safe_float(rec.get("gmt_fund")),
                    safe_float(rec.get("gmt_per_block")),
                    safe_float(rec.get("league_th")),
                    safe_int(rec.get("blocks_mined")),
                    safe_float(rec.get("efficiency_league")),
                    safe_float(rec.get("btc_fund")),
                    to_iso_utc(rec.get("ended_at")),
                    safe_int(rec.get("round_duration_sec")),
                    str(source or "api_triplet"),
                    time.time(),
                ),
            )
            cnt += 1
        self.conn.commit()
        return cnt

    def fetch_api_completed_rounds(self, league_id: int, since_round: int, limit: int = MAX_ROUNDS_PER_POLL) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT
              snapshot_ts,
              league_id,
              round_id,
              block_number,
              multiplier,
              gmt_fund,
              gmt_per_block,
              league_th,
              blocks_mined,
              efficiency_league,
              btc_fund,
              ended_at,
              round_duration_sec
            FROM api_round_cache
            WHERE league_id=?
              AND round_id>?
              AND ended_at IS NOT NULL
            ORDER BY round_id ASC
            LIMIT ?
            """,
            (league_id, since_round, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def fetch_latest_api_completed_round_id(self, league_id: int) -> Optional[int]:
        row = self.conn.execute(
            """
            SELECT round_id
            FROM api_round_cache
            WHERE league_id=?
              AND ended_at IS NOT NULL
            ORDER BY round_id DESC
            LIMIT 1
            """,
            (league_id,),
        ).fetchone()
        return safe_int(row["round_id"]) if row else None

    def fetch_latest_api_round_record(self, league_id: int) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            """
            SELECT
              snapshot_ts,
              league_id,
              round_id,
              block_number,
              multiplier,
              gmt_fund,
              gmt_per_block,
              league_th,
              blocks_mined,
              efficiency_league,
              btc_fund,
              ended_at,
              round_duration_sec,
              source
            FROM api_round_cache
            WHERE league_id=?
            ORDER BY round_id DESC
            LIMIT 1
            """,
            (league_id,),
        ).fetchone()
        return dict(row) if row else None

    def mark_api_round_ended(self, league_id: int, round_id: int, ended_at_iso: str) -> int:
        ended_at = to_iso_utc(ended_at_iso) or utc_now_iso()
        cur = self.conn.execute(
            """
            UPDATE api_round_cache
            SET ended_at=?, updated_at=?
            WHERE league_id=?
              AND round_id=?
              AND (ended_at IS NULL OR TRIM(ended_at)='')
            """,
            (ended_at, time.time(), league_id, round_id),
        )
        self.conn.commit()
        return int(cur.rowcount or 0)
