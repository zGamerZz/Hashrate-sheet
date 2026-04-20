#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import os
import sys
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import backfill_round_gap
import main


DEFAULT_FROM_ROUND = backfill_round_gap.DEFAULT_FROM_ROUND
DEFAULT_TO_ROUND = "latest"
DEFAULT_TARGET_SUFFIX = " - Multiplier Filter"
DEFAULT_WORKERS = backfill_round_gap.DEFAULT_WORKERS
DEFAULT_GOMINING_RPM = backfill_round_gap.DEFAULT_BACKFILL_GOMINING_RPM
TARGET_MARKER = "multiplier_threshold_backfill"
REPORT_DIR = "api_debug"
BENIGN_ROUND_RECORD_ERRORS = set(backfill_round_gap.BENIGN_ROUND_RECORD_ERRORS)

OUTPUT_HEADERS: List[str] = [
    "timestamp_utc",
    "leagueId",
    "roundId",
    "multiplier",
    "blockNumber",
    "gmtFund",
    "gmtPerBlock",
    "leagueTH",
    "blocks_mined",
    "efficiency_league",
    "endedAt_utc",
    "roundDuration_sec",
]


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def resolve_target_round(to_round_arg: str, latest_completed: Optional[int]) -> Optional[int]:
    return backfill_round_gap.resolve_to_round(to_round_arg, latest_completed)


def _column_letter(col_idx_1based: int) -> str:
    n = max(1, int(col_idx_1based))
    out = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out = chr(ord("A") + rem) + out
    return out


def _target_tab_title(main_title: str, suffix: str) -> str:
    resolved_suffix = str(suffix or DEFAULT_TARGET_SUFFIX)
    title = f"{main_title}{resolved_suffix}"
    return title[:100]


def filter_round_records_by_multiplier(
    records: Sequence[Dict[str, Any]],
    min_multiplier: float,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    threshold = float(min_multiplier)
    for rec in records:
        if not isinstance(rec, dict):
            continue
        rid = main.safe_int(rec.get("round_id"))
        if rid is None:
            continue
        multiplier = main.safe_float(rec.get("multiplier"))
        if multiplier is None:
            continue
        if float(multiplier) > threshold:
            out.append(rec)
    out.sort(key=lambda r: main.safe_int(r.get("round_id")) or -1)
    return out


def build_output_rows(records: Sequence[Dict[str, Any]]) -> List[List[Any]]:
    rows: List[List[Any]] = []
    for rec in sorted(records, key=lambda r: main.safe_int(r.get("round_id")) or -1):
        rid = main.safe_int(rec.get("round_id"))
        if rid is None:
            continue
        league_id = main.safe_int(rec.get("league_id"))
        multiplier = main.safe_float(rec.get("multiplier"))
        block_number = main.safe_int(rec.get("block_number"))
        gmt_fund = main.safe_float(rec.get("gmt_fund"))
        gmt_per_block = main.safe_float(rec.get("gmt_per_block"))
        league_th = main.safe_float(rec.get("league_th"))
        blocks_mined = main.safe_int(rec.get("blocks_mined"))
        efficiency = main.safe_float(rec.get("efficiency_league"))
        ended_at = main.to_iso_utc(rec.get("ended_at"))
        round_duration = main.safe_int(rec.get("round_duration_sec"))
        ts = main.to_iso_utc(rec.get("snapshot_ts")) or ended_at or ""
        rows.append(
            [
                ts,
                "" if league_id is None else league_id,
                rid,
                "" if multiplier is None else multiplier,
                "" if block_number is None else block_number,
                "" if gmt_fund is None else gmt_fund,
                "" if gmt_per_block is None else gmt_per_block,
                "" if league_th is None else league_th,
                "" if blocks_mined is None else blocks_mined,
                "" if efficiency is None else efficiency,
                "" if ended_at is None else ended_at,
                "" if round_duration is None else round_duration,
            ]
        )
    return rows


def merge_round_records_for_league(
    *,
    candidate_round_ids: Sequence[int],
    cached_record_map: Dict[int, Dict[str, Any]],
    round_record_by_round_id: Dict[int, backfill_round_gap.RoundRecordFetchResult],
    league_id: int,
    benign_errors: Optional[Iterable[str]] = None,
) -> Tuple[List[Dict[str, Any]], List[int]]:
    benign = {str(x) for x in (benign_errors or BENIGN_ROUND_RECORD_ERRORS)}
    rec_map: Dict[int, Dict[str, Any]] = dict(cached_record_map)
    failed_round_ids: List[int] = []

    for rid in candidate_round_ids:
        rid_int = int(rid)
        if rid_int in rec_map:
            continue
        res = round_record_by_round_id.get(rid_int)
        if res is None:
            failed_round_ids.append(rid_int)
            continue
        if res.ok and isinstance(res.record, dict):
            rec_league_id = main.safe_int(res.record.get("league_id"))
            if rec_league_id == int(league_id):
                rec_map[rid_int] = res.record
            else:
                failed_round_ids.append(rid_int)
            continue
        if str(res.error) in benign:
            continue
        failed_round_ids.append(rid_int)

    merged = backfill_round_gap._normalize_round_records(
        [rec_map[rid] for rid in candidate_round_ids if int(rid) in rec_map],
        context=f"multiplier_merge_league_{league_id}",
    )
    merged.sort(key=lambda r: main.safe_int(r.get("round_id")) or -1)
    return merged, sorted({int(x) for x in failed_round_ids})


def _find_worksheet_by_title(sh: Any, title: str) -> Optional[Any]:
    try:
        worksheets = sh.worksheets()
    except Exception:
        return None
    for ws in worksheets:
        if str(getattr(ws, "title", "")) == str(title):
            return ws
    return None


def _ensure_worksheet_shape(
    ws: Any,
    *,
    required_rows: int,
    required_cols: int,
    write_limiter: main.TokenBucket,
) -> None:
    current_rows = main.safe_int(getattr(ws, "row_count", None)) or 0
    current_cols = main.safe_int(getattr(ws, "col_count", None)) or 0

    if required_rows > current_rows:
        write_limiter.wait_for_token(1)
        ws.add_rows(int(required_rows - current_rows))
        current_rows = required_rows
        _ = current_rows

    if required_cols > current_cols:
        write_limiter.wait_for_token(1)
        ws.add_cols(int(required_cols - current_cols))


def rebuild_target_sheet(
    *,
    ws: Any,
    league_id: int,
    from_round: int,
    to_round: int,
    min_multiplier: float,
    rows: Sequence[Sequence[Any]],
    write_limiter: main.TokenBucket,
    execute: bool,
) -> Dict[str, Any]:
    generated_at = main.utc_now_iso()
    if not execute:
        return {
            "executed": False,
            "written_rows": int(len(rows)),
            "generated_at": generated_at,
        }

    required_cols = max(26, len(OUTPUT_HEADERS) + 2)
    required_last_row = max(3, 3 + len(rows))
    required_rows = max(1000, required_last_row + 20)
    _ensure_worksheet_shape(
        ws,
        required_rows=required_rows,
        required_cols=required_cols,
        write_limiter=write_limiter,
    )

    clear_cols = max(main.safe_int(getattr(ws, "col_count", None)) or required_cols, required_cols)
    clear_rows = max(main.safe_int(getattr(ws, "row_count", None)) or required_rows, required_last_row)
    end_col = _column_letter(clear_cols)

    write_limiter.wait_for_token(1)
    ws.batch_clear([f"A1:{end_col}2", f"A3:{end_col}{clear_rows}"])

    meta_row = [TARGET_MARKER, int(league_id), int(from_round), int(to_round), float(min_multiplier), generated_at]
    write_limiter.wait_for_token(1)
    ws.batch_update(
        [
            {"range": "A1", "values": [meta_row]},
            {"range": "A3", "values": [OUTPUT_HEADERS]},
        ],
        value_input_option="RAW",
    )

    start_row = main.LOG_START_ROW
    chunk_size = 500
    for offset in range(0, len(rows), chunk_size):
        chunk = [list(x) for x in rows[offset : offset + chunk_size]]
        if not chunk:
            continue
        write_limiter.wait_for_token(1)
        ws.update(
            range_name=f"A{start_row + offset}",
            values=chunk,
            value_input_option="RAW",
        )

    return {
        "executed": True,
        "written_rows": int(len(rows)),
        "generated_at": generated_at,
    }


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill rounds in a fixed window and write per-league tabs with "
            "records where multiplier > threshold."
        ),
    )
    parser.add_argument(
        "--from-round",
        type=int,
        default=DEFAULT_FROM_ROUND,
        help=f"Inclusive start round (default: {DEFAULT_FROM_ROUND})",
    )
    parser.add_argument(
        "--to-round",
        type=str,
        default=DEFAULT_TO_ROUND,
        help="Inclusive end round or 'latest' (default: latest)",
    )
    parser.add_argument(
        "--min-multiplier",
        type=float,
        required=True,
        help="Strict multiplier threshold; only rounds with multiplier > threshold are written.",
    )
    parser.add_argument(
        "--league-ids",
        type=str,
        default="",
        help="Optional comma-separated league IDs, e.g. 1,3,4",
    )
    parser.add_argument(
        "--target-suffix",
        type=str,
        default=DEFAULT_TARGET_SUFFIX,
        help=f"Suffix appended to each source main tab title (default: {DEFAULT_TARGET_SUFFIX!r})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Workers for missing round fetch (default: {DEFAULT_WORKERS})",
    )
    parser.add_argument(
        "--gomining-rpm",
        type=int,
        default=DEFAULT_GOMINING_RPM,
        help=f"GoMining API rate limit for this run (default: {DEFAULT_GOMINING_RPM})",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply writes to sheets and DB cache. Without this flag script runs as dry-run.",
    )
    return parser


def main_cli() -> int:
    parser = _build_arg_parser()
    args = parser.parse_args()

    if int(args.from_round) <= 0:
        raise ValueError("--from-round must be > 0")
    if int(args.workers) <= 0:
        raise ValueError("--workers must be > 0")
    if int(args.gomining_rpm) <= 0:
        raise ValueError("--gomining-rpm must be > 0")
    if str(args.target_suffix or "").strip() == "":
        raise ValueError("--target-suffix must not be empty")

    lock = main.SingleInstanceLock(main.LOCK_FILE_PATH)
    state: Optional[main.StateStore] = None
    lock.acquire()
    try:
        state = main.StateStore(main.STATE_DB_PATH)
        read_limiter = main.TokenBucket(main.GS_READ_REQ_PER_MIN, name="sheets_read")
        write_limiter = main.TokenBucket(main.GS_WRITE_REQ_PER_MIN, name="sheets_write")
        gomining_guard = backfill_round_gap.ThreadSafeRateGuard(int(args.gomining_rpm), name="gomining_api")
        effective_bearer, token_fetcher = backfill_round_gap._resolve_effective_bearer()

        expected_main_cols = len(main.BASE_HEADERS) + len(main.ABILITY_HEADER_ORDER) + 6
        sh = main.open_spreadsheet()
        contexts = backfill_round_gap._load_main_contexts(sh, read_limiter, expected_main_cols)

        selected_leagues = backfill_round_gap._parse_league_ids(args.league_ids)
        if selected_leagues is not None:
            contexts = {ws_id: ctx for ws_id, ctx in contexts.items() if int(ctx.league_id) in selected_leagues}
        if not contexts:
            print("[INFO] No matching main sheets found. Nothing to do.")
            return 0

        round_metrics_discovery_api = main.GoMiningRoundMetricsApiClient(
            bearer_token=effective_bearer,
            limiter=gomining_guard,
            base_url=main.GOMINING_API_BASE_URL,
            multiplier_path=main.MULTIPLIER_PATH,
            round_clan_leaderboard_path=main.ROUND_CLAN_LEADERBOARD_PATH,
            player_leaderboard_path=main.PLAYER_LEADERBOARD_PATH,
            clan_leaderboard_path=main.ROUND_METRICS_CLAN_PATH,
            user_page_limit=main.ROUND_METRICS_USER_PAGE_LIMIT,
            clan_page_limit=main.ROUND_METRICS_CLAN_PAGE_LIMIT,
            round_scan_lookback=main.ROUND_GET_LAST_SCAN_LOOKBACK,
            timeout_seconds=main.ROUND_METRICS_TIMEOUT_SECONDS,
            max_retries=main.ROUND_METRICS_MAX_RETRIES,
            token_fetcher=token_fetcher,
        )

        to_round_is_latest = str(args.to_round or "").strip().lower() in {"", "latest"}
        plan_rows: List[Dict[str, Any]] = []
        candidate_round_ids_by_sheet: Dict[int, List[int]] = {}
        cached_record_map_by_sheet: Dict[int, Dict[int, Dict[str, Any]]] = {}
        round_record_tasks: List[int] = []
        seen_round_record_tasks: set[int] = set()

        for ws_id, ctx in contexts.items():
            latest_completed_cache = state.fetch_latest_api_completed_round_id(ctx.league_id)
            latest_completed = latest_completed_cache
            latest_source = "cache"
            latest_seed_record: Optional[Dict[str, Any]] = None

            if to_round_is_latest and latest_completed is None:
                latest_seed_record = backfill_round_gap._fetch_latest_completed_round_from_api(
                    round_metrics_discovery_api,
                    ctx.league_id,
                )
                latest_seed_round = main.safe_int((latest_seed_record or {}).get("round_id"))
                if latest_seed_round is not None:
                    latest_completed = latest_seed_round
                    latest_source = "live_api"
                else:
                    latest_source = "missing"

            to_round = resolve_target_round(args.to_round, latest_completed)
            if to_round is None or int(to_round) < int(args.from_round):
                plan_rows.append(
                    {
                        "ws_id": ws_id,
                        "sheet": ctx.title,
                        "league_id": ctx.league_id,
                        "from_round": int(args.from_round),
                        "to_round": to_round,
                        "latest_completed": latest_completed,
                        "latest_source": latest_source,
                        "candidate_rounds": 0,
                        "cached_round_records": 0,
                        "unresolved_round_records": 0,
                        "planned": False,
                        "reason": "no_completed_rounds_in_window",
                    }
                )
                continue

            cached_records = backfill_round_gap._fetch_completed_round_records_in_range(
                state=state,
                league_id=ctx.league_id,
                from_round=int(args.from_round),
                to_round=int(to_round),
            )
            cached_record_map: Dict[int, Dict[str, Any]] = {}
            for rec in cached_records:
                rid = main.safe_int(rec.get("round_id"))
                if rid is None:
                    continue
                cached_record_map[int(rid)] = rec

            if isinstance(latest_seed_record, dict):
                seed_rid = main.safe_int(latest_seed_record.get("round_id"))
                if (
                    seed_rid is not None
                    and seed_rid >= int(args.from_round)
                    and seed_rid <= int(to_round)
                    and int(seed_rid) not in cached_record_map
                ):
                    cached_record_map[int(seed_rid)] = latest_seed_record

            candidate_round_ids = list(range(int(args.from_round), int(to_round) + 1))
            unresolved_round_ids = [rid for rid in candidate_round_ids if rid not in cached_record_map]
            for rid in unresolved_round_ids:
                rid_int = int(rid)
                if rid_int in seen_round_record_tasks:
                    continue
                seen_round_record_tasks.add(rid_int)
                round_record_tasks.append(rid_int)

            candidate_round_ids_by_sheet[ws_id] = candidate_round_ids
            cached_record_map_by_sheet[ws_id] = cached_record_map
            plan_rows.append(
                {
                    "ws_id": ws_id,
                    "sheet": ctx.title,
                    "league_id": ctx.league_id,
                    "from_round": int(args.from_round),
                    "to_round": int(to_round),
                    "latest_completed": latest_completed,
                    "latest_source": latest_source,
                    "candidate_rounds": len(candidate_round_ids),
                    "cached_round_records": len(cached_record_map),
                    "unresolved_round_records": len(unresolved_round_ids),
                    "planned": True,
                    "reason": "",
                }
            )

        round_record_tasks.sort()
        print(
            f"[PLAN] sheets={len(contexts)} from_round={int(args.from_round)} to_round={args.to_round!r} "
            f"min_multiplier={float(args.min_multiplier)} unresolved_round_records={len(round_record_tasks)} execute={bool(args.execute)}"
        )
        for row in plan_rows:
            print(
                f"[PLAN] sheet='{row['sheet']}' league={row['league_id']} planned={row['planned']} "
                f"range={row['from_round']}..{row['to_round']} candidates={row['candidate_rounds']} "
                f"cached={row['cached_round_records']} unresolved={row['unresolved_round_records']} "
                f"latest_source={row['latest_source']} reason={row['reason'] or '-'}"
            )

        round_record_results: List[backfill_round_gap.RoundRecordFetchResult] = []
        if round_record_tasks:
            round_record_results = backfill_round_gap._fetch_round_records_parallel(
                tasks=round_record_tasks,
                workers=int(args.workers),
                probe_league_id=min(ctx.league_id for ctx in contexts.values()),
                expected_league_ids={ctx.league_id for ctx in contexts.values()},
                bearer_token=effective_bearer,
                token_fetcher=token_fetcher,
                limiter=gomining_guard,
            )

        round_record_by_round_id: Dict[int, backfill_round_gap.RoundRecordFetchResult] = {
            int(r.round_id): r for r in round_record_results
        }
        round_records_ok = len([r for r in round_record_results if r.ok and isinstance(r.record, dict)])
        round_records_benign = len([r for r in round_record_results if (not r.ok) and str(r.error) in BENIGN_ROUND_RECORD_ERRORS])
        round_records_failed = len(round_record_results) - round_records_ok - round_records_benign

        fetched_ok_records = [r.record for r in round_record_results if r.ok and isinstance(r.record, dict)]
        if bool(args.execute) and fetched_ok_records:
            state.upsert_api_round_records(
                fetched_ok_records,  # type: ignore[arg-type]
                source="backfill_multiplier_threshold",
            )

        report_rows: List[Dict[str, Any]] = []
        any_partial = round_records_failed > 0

        for ws_id, ctx in contexts.items():
            base = next((x for x in plan_rows if int(x.get("ws_id") or -1) == int(ws_id)), None)
            if base is None:
                continue
            if not base.get("planned"):
                report_rows.append(dict(base))
                continue

            candidate_round_ids = candidate_round_ids_by_sheet.get(ws_id, [])
            cached_record_map = cached_record_map_by_sheet.get(ws_id, {})
            merged_records, failed_round_ids = merge_round_records_for_league(
                candidate_round_ids=candidate_round_ids,
                cached_record_map=cached_record_map,
                round_record_by_round_id=round_record_by_round_id,
                league_id=ctx.league_id,
            )
            filtered_records = filter_round_records_by_multiplier(merged_records, float(args.min_multiplier))
            output_rows = build_output_rows(filtered_records)
            target_title = _target_tab_title(ctx.title, str(args.target_suffix))

            sheet_stats = {
                "executed": False,
                "written_rows": len(output_rows),
                "generated_at": main.utc_now_iso(),
            }
            created = False
            if bool(args.execute):
                target_ws = _find_worksheet_by_title(sh, target_title)
                if target_ws is None:
                    est_rows = max(1000, main.LOG_START_ROW + max(1, len(output_rows)) + 50)
                    est_cols = max(26, len(OUTPUT_HEADERS) + 2)
                    write_limiter.wait_for_token(1)
                    target_ws = sh.add_worksheet(title=target_title, rows=est_rows, cols=est_cols)
                    created = True
                sheet_stats = rebuild_target_sheet(
                    ws=target_ws,
                    league_id=ctx.league_id,
                    from_round=int(base.get("from_round") or args.from_round),
                    to_round=int(base.get("to_round") or args.from_round),
                    min_multiplier=float(args.min_multiplier),
                    rows=output_rows,
                    write_limiter=write_limiter,
                    execute=True,
                )

            if failed_round_ids:
                any_partial = True
                main.log_warn(
                    "multiplier_backfill.round_record_failed_rounds",
                    sheet=ctx.title,
                    league_id=ctx.league_id,
                    failed_rounds=len(failed_round_ids),
                    failed_round_ids=failed_round_ids[:20],
                )

            row_report = dict(base)
            row_report.update(
                {
                    "target_sheet": target_title,
                    "target_sheet_created": bool(created),
                    "records_merged": len(merged_records),
                    "records_filtered": len(filtered_records),
                    "records_failed": len(failed_round_ids),
                    "record_failed_round_ids": failed_round_ids,
                    "execute": bool(args.execute),
                    "sheet_written_rows": int(sheet_stats.get("written_rows", 0)),
                    "sheet_generated_at": str(sheet_stats.get("generated_at") or ""),
                }
            )
            report_rows.append(row_report)

        report = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "execute": bool(args.execute),
            "from_round": int(args.from_round),
            "to_round_arg": str(args.to_round),
            "min_multiplier": float(args.min_multiplier),
            "workers": int(args.workers),
            "gomining_rpm": int(args.gomining_rpm),
            "target_suffix": str(args.target_suffix),
            "round_record_fetch_total": len(round_record_results),
            "round_record_fetch_ok": round_records_ok,
            "round_record_fetch_benign": round_records_benign,
            "round_record_fetch_failed": round_records_failed,
            "rows": report_rows,
        }
        os.makedirs(REPORT_DIR, exist_ok=True)
        report_path = os.path.join(REPORT_DIR, f"backfill_multiplier_threshold_report_{_utc_stamp()}.json")
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        print(f"[REPORT] {report_path}")
        print(
            f"[SUMMARY] execute={bool(args.execute)} sheets={len(report_rows)} "
            f"round_records_ok={round_records_ok} round_records_benign={round_records_benign} "
            f"round_records_failed={round_records_failed}"
        )

        if any_partial:
            print("[RESULT] completed with partial failures (exit=2).")
            return 2
        print("[RESULT] completed successfully (exit=0).")
        return 0
    except Exception as e:
        tb_text = traceback.format_exc()
        main.log_warn("multiplier_backfill.fatal", err=repr(e), traceback=tb_text[:8000])
        print(f"[ERROR] {e!r}")
        print(tb_text)
        return 1
    finally:
        try:
            if state is not None:
                state.close()
        except Exception:
            pass
        lock.release()


if __name__ == "__main__":
    sys.exit(main_cli())
