#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import os
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import backfill_round_gap
import main


DEFAULT_LEAGUE_ID = 3
DEFAULT_TARGET_SHEET = "Eclipse-backfill"
DEFAULT_FROM_ROUND = 924321
DEFAULT_START_ENDED_AT = "2026-04-09T00:00:00Z"
DEFAULT_TO_ROUND = "latest"
DEFAULT_TARGET_ROWS = 1000
EXPECTED_HEADER_COLS = 33
REPORT_PREFIX = "backfill_eclipse_backfill_report"
TARGET_MARKER_CELL = "D1"
TARGET_LEAGUE_CELL = "E1"


@dataclass
class TargetSetupResult:
    ctx: main.SheetContext
    created: bool = False
    wrote_layout: bool = False
    reset_rows: bool = False
    existing_rounds: int = 0


def build_target_header(ability_headers: Optional[Sequence[str]] = None) -> List[str]:
    abilities = list(ability_headers if ability_headers is not None else main.ABILITY_HEADER_ORDER)
    return (
        list(main.BASE_HEADERS)
        + abilities
        + [
            main.POWER_UP_PRICE_HEADER,
            main.POWER_UP_PRICE_SENTINEL_HEADER,
            main.CLAN_POWER_UP_PRICE_HEADER,
            main.CLAN_POWER_UP_PRICE_SENTINEL_HEADER,
            main.MISSING_HEADER,
            main.EXCLUDED_USER_BOOST_AUDIT_HEADER,
            main.BTC_FUND_HEADER,
            main.TRACKED_USER_BLOCKS_MINED_HEADER,
        ]
    )


def _column_letter(col_num: int) -> str:
    n = max(1, int(col_num))
    letters = ""
    while n:
        n, rem = divmod(n - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


def _find_worksheet_by_title(sh: Any, title: str) -> Optional[Any]:
    for ws in sh.worksheets():
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


def _read_existing_round_ids(ctx: main.SheetContext, read_limiter: main.TokenBucket) -> set[int]:
    if getattr(ctx, "ws", None) is None:
        return set()
    idx = main._read_round_row_index(ctx.ws, read_limiter, round_col_index=(ctx.round_col_idx or 6) + 1)
    return {int(rid) for rid in idx.keys() if main.safe_int(rid) is not None}


def ensure_target_sheet_context(
    *,
    sh: Any,
    target_title: str,
    league_id: int,
    header: Sequence[str],
    write_limiter: main.TokenBucket,
    read_limiter: main.TokenBucket,
    execute: bool,
    reset_target: bool = False,
) -> TargetSetupResult:
    expected_cols = len(list(header))
    required_cols = max(EXPECTED_HEADER_COLS, expected_cols)
    ws = _find_worksheet_by_title(sh, target_title)
    created = False

    if ws is None:
        if not execute:
            ctx = main.SheetContext(
                ws_id=-1,
                title=target_title,
                ws=None,
                league_id=int(league_id),
                kind="main",
                expected_cols=expected_cols,
                round_col_idx=6,
                next_row=main.LOG_START_ROW,
            )
            return TargetSetupResult(ctx=ctx, created=False, wrote_layout=False, reset_rows=False)
        write_limiter.wait_for_token(1)
        ws = sh.add_worksheet(
            title=target_title,
            rows=DEFAULT_TARGET_ROWS,
            cols=max(26, required_cols),
        )
        created = True

    ctx = main.SheetContext(
        ws_id=int(ws.id),
        title=str(ws.title),
        ws=ws,
        league_id=int(league_id),
        kind="main",
        expected_cols=expected_cols,
        round_col_idx=6,
    )

    if execute:
        _ensure_worksheet_shape(
            ws,
            required_rows=DEFAULT_TARGET_ROWS,
            required_cols=max(26, required_cols),
            write_limiter=write_limiter,
        )
        write_limiter.wait_for_token(1)
        ws.batch_update(
            [
                {"range": "A1", "values": [[""]]},
                {"range": "B1", "values": [[""]]},
                {"range": TARGET_MARKER_CELL, "values": [[main.MAIN_SHEET_MARKER]]},
                {"range": TARGET_LEAGUE_CELL, "values": [[int(league_id)]]},
                {"range": "A3", "values": [list(header)]},
                {"range": "G1", "values": [["last_roundId"]]},
                {"range": "I1", "values": [["last_abilities_logged_roundId"]]},
            ],
            value_input_option="RAW",
        )

    if execute and reset_target:
        current_rows = main.safe_int(getattr(ws, "row_count", None)) or DEFAULT_TARGET_ROWS
        current_cols = main.safe_int(getattr(ws, "col_count", None)) or required_cols
        end_col = _column_letter(max(current_cols, required_cols))
        write_limiter.wait_for_token(1)
        ws.batch_clear([f"A{main.LOG_START_ROW}:{end_col}{max(current_rows, main.LOG_START_ROW)}"])
        ctx.next_row = main.LOG_START_ROW
        existing_rounds = 0
    else:
        ctx.next_row = main._detect_next_row(ws, read_limiter, round_col_index=(ctx.round_col_idx or 6) + 1)
        existing_rounds = len(_read_existing_round_ids(ctx, read_limiter))

    return TargetSetupResult(
        ctx=ctx,
        created=created,
        wrote_layout=bool(execute),
        reset_rows=bool(execute and reset_target),
        existing_rounds=existing_rounds,
    )


def _parse_utc_datetime(value: Any) -> Optional[datetime]:
    iso = main.to_iso_utc(value)
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def filter_records_for_target(
    records: Sequence[Dict[str, Any]],
    *,
    league_id: int,
    start_ended_at: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    start_dt = _parse_utc_datetime(start_ended_at)
    if start_dt is None:
        raise ValueError(f"Invalid --start-ended-at value: {start_ended_at!r}")

    accepted: List[Dict[str, Any]] = []
    stats = {
        "input": 0,
        "accepted": 0,
        "wrong_league": 0,
        "missing_ended_at": 0,
        "before_start": 0,
    }
    for rec in records:
        if not isinstance(rec, dict):
            continue
        stats["input"] += 1
        rec_league_id = main.safe_int(rec.get("league_id"))
        if rec_league_id != int(league_id):
            stats["wrong_league"] += 1
            continue
        ended_dt = _parse_utc_datetime(rec.get("ended_at"))
        if ended_dt is None:
            stats["missing_ended_at"] += 1
            continue
        if ended_dt < start_dt:
            stats["before_start"] += 1
            continue
        accepted.append(rec)

    accepted.sort(key=lambda r: main.safe_int(r.get("round_id")) or -1)
    stats["accepted"] = len(accepted)
    return accepted, stats


def _sync_target_row_map_from_sheet(
    *,
    state: main.StateStore,
    ctx: main.SheetContext,
    read_limiter: main.TokenBucket,
    from_round: int,
    to_round: int,
) -> int:
    if getattr(ctx, "ws", None) is None:
        return 0
    round_index = main._read_round_row_index(ctx.ws, read_limiter, round_col_index=(ctx.round_col_idx or 6) + 1)
    synced = 0
    for rid, row_idx in sorted(round_index.items()):
        rid_int = main.safe_int(rid)
        row_idx_int = main.safe_int(row_idx)
        if rid_int is None or row_idx_int is None:
            continue
        if rid_int < int(from_round) or rid_int > int(to_round):
            continue
        state.upsert_row_map(ctx.ws_id, rid_int, row_idx_int, "", 0)
        synced += 1
    return synced


def prepare_target_state(
    *,
    state: main.StateStore,
    ctx: main.SheetContext,
    from_round: int,
    to_round: int,
    execute: bool,
    reset_target: bool,
    read_limiter: Optional[main.TokenBucket] = None,
) -> Dict[str, Any]:
    cutover_round = int(from_round) - 1
    if not execute:
        return {
            "executed": False,
            "cutover_round": cutover_round,
            "row_map_synced": 0,
            "pending_deleted": 0,
            "row_map_deleted": 0,
        }

    state.upsert_sheet_meta(ctx.ws_id, ctx.title, ctx.league_id, layout_sig=None)
    state.conn.execute(
        "UPDATE sheet_state SET last_synced_round=?, price_cutover_round=?, updated_at=? WHERE sheet_id=?",
        (cutover_round, cutover_round, time.time(), ctx.ws_id),
    )
    pending_cur = state.conn.execute(
        "DELETE FROM pending_ops "
        "WHERE sheet_id=? AND op_type IN ('append_round','update_round') AND round_id>=? AND round_id<=?",
        (ctx.ws_id, int(from_round), int(to_round)),
    )
    row_map_cur = state.conn.execute(
        "DELETE FROM round_row_map WHERE sheet_id=? AND round_id>=? AND round_id<=?",
        (ctx.ws_id, int(from_round), int(to_round)),
    )
    state.conn.commit()

    row_map_synced = 0
    if not reset_target and read_limiter is not None:
        row_map_synced = _sync_target_row_map_from_sheet(
            state=state,
            ctx=ctx,
            read_limiter=read_limiter,
            from_round=int(from_round),
            to_round=int(to_round),
        )

    return {
        "executed": True,
        "cutover_round": cutover_round,
        "row_map_synced": row_map_synced,
        "pending_deleted": int(pending_cur.rowcount or 0),
        "row_map_deleted": int(row_map_cur.rowcount or 0),
    }


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_report(report: Dict[str, Any]) -> str:
    os.makedirs("api_debug", exist_ok=True)
    path = os.path.join("api_debug", f"{REPORT_PREFIX}_{_utc_stamp()}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    return path


def _build_round_metrics_client(
    bearer_token: str,
    limiter: Any,
    token_fetcher: Optional[Any],
) -> main.GoMiningRoundMetricsApiClient:
    return main.GoMiningRoundMetricsApiClient(
        bearer_token=bearer_token,
        limiter=limiter,
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


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fully backfill the Eclipse-backfill sheet for leagueId=3 from 2026-04-09 UTC.",
    )
    parser.add_argument("--league-id", type=int, default=DEFAULT_LEAGUE_ID, help=f"Target leagueId (default: {DEFAULT_LEAGUE_ID}).")
    parser.add_argument("--target-sheet", type=str, default=DEFAULT_TARGET_SHEET, help=f"Target sheet title (default: {DEFAULT_TARGET_SHEET}).")
    parser.add_argument("--from-round", type=int, default=DEFAULT_FROM_ROUND, help=f"Inclusive global start round (default: {DEFAULT_FROM_ROUND}).")
    parser.add_argument("--start-ended-at", type=str, default=DEFAULT_START_ENDED_AT, help=f"Inclusive UTC endedAt lower bound (default: {DEFAULT_START_ENDED_AT}).")
    parser.add_argument("--to-round", type=str, default=DEFAULT_TO_ROUND, help="Inclusive end round or 'latest' (default: latest).")
    parser.add_argument(
        "--profile",
        choices=sorted(backfill_round_gap.BACKFILL_PROFILE_PRESETS.keys()),
        default=backfill_round_gap.DEFAULT_BACKFILL_PROFILE,
        help=f"Backfill load profile (default: {backfill_round_gap.DEFAULT_BACKFILL_PROFILE}).",
    )
    parser.add_argument("--workers", type=int, default=None, help="Parallel fetch/prefetch workers. Defaults to profile setting.")
    parser.add_argument("--gomining-rpm", type=int, default=None, help="GoMining API requests per minute. Defaults to profile setting.")
    parser.add_argument("--prefetch-attempts", type=int, default=None, help="Ability prefetch retry passes. Defaults to profile setting.")
    parser.add_argument("--max-cycles", type=int, default=backfill_round_gap.DEFAULT_MAX_CYCLES, help=f"Max enqueue/flush cycles (default: {backfill_round_gap.DEFAULT_MAX_CYCLES}).")
    parser.add_argument(
        "--continue-on-error",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Continue with successfully fetched rounds when some API calls fail (default: true).",
    )
    parser.add_argument("--reset-target", action="store_true", help="With --execute, clear target data rows before writing.")
    parser.add_argument("--execute", action="store_true", help="Apply DB/queue/sheet mutations. Without this flag, run as dry-run.")
    return parser


def main_cli() -> int:
    parser = _build_arg_parser()
    args = parser.parse_args()

    profile_name, profile_cfg = backfill_round_gap._resolve_backfill_profile(args.profile)
    args.profile = profile_name
    if args.workers is None:
        args.workers = int(profile_cfg["workers"])
    if args.gomining_rpm is None:
        args.gomining_rpm = int(profile_cfg["gomining_rpm"])
    if args.prefetch_attempts is None:
        args.prefetch_attempts = int(profile_cfg["prefetch_attempts"])

    if args.league_id <= 0:
        raise ValueError("--league-id must be > 0")
    if args.from_round <= 0:
        raise ValueError("--from-round must be > 0")
    if args.workers <= 0:
        raise ValueError("--workers must be > 0")
    if args.gomining_rpm <= 0:
        raise ValueError("--gomining-rpm must be > 0")
    if args.prefetch_attempts <= 0:
        raise ValueError("--prefetch-attempts must be > 0")
    if args.max_cycles <= 0:
        raise ValueError("--max-cycles must be > 0")
    if _parse_utc_datetime(args.start_ended_at) is None:
        raise ValueError(f"Invalid --start-ended-at value: {args.start_ended_at!r}")

    header = build_target_header()
    if len(header) != EXPECTED_HEADER_COLS:
        raise RuntimeError(f"target header has {len(header)} columns; expected {EXPECTED_HEADER_COLS}")

    lock = main.SingleInstanceLock(main.LOCK_FILE_PATH)
    state: Optional[main.StateStore] = None
    exit_code = 0
    lock.acquire()
    try:
        write_limiter = main.TokenBucket(main.GS_WRITE_REQ_PER_MIN, name="sheets_write")
        read_limiter = main.TokenBucket(main.GS_READ_REQ_PER_MIN, name="sheets_read")
        gomining_guard = backfill_round_gap.ThreadSafeRateGuard(int(args.gomining_rpm), name="gomining_api")

        effective_bearer, token_fetcher = backfill_round_gap._resolve_effective_bearer()
        round_metrics_discovery_api = _build_round_metrics_client(effective_bearer, gomining_guard, token_fetcher)

        latest_seed_record = backfill_round_gap._fetch_latest_completed_round_from_api(
            round_metrics_discovery_api,
            int(args.league_id),
        )
        latest_completed = main.safe_int((latest_seed_record or {}).get("round_id"))
        to_round = backfill_round_gap.resolve_to_round(str(args.to_round), latest_completed)
        if to_round is None:
            raise RuntimeError(f"Could not resolve --to-round={args.to_round!r}; latest round unavailable for league {args.league_id}")
        if int(to_round) < int(args.from_round):
            raise RuntimeError(f"No completed rounds in requested window: {args.from_round}..{to_round}")

        sh = main.open_spreadsheet()
        setup = ensure_target_sheet_context(
            sh=sh,
            target_title=str(args.target_sheet),
            league_id=int(args.league_id),
            header=header,
            write_limiter=write_limiter,
            read_limiter=read_limiter,
            execute=bool(args.execute),
            reset_target=bool(args.reset_target),
        )
        ctx = setup.ctx

        round_record_tasks = list(range(int(args.from_round), int(to_round) + 1))
        main.log_info(
            "eclipse_backfill.planned",
            target_sheet=ctx.title,
            league_id=args.league_id,
            from_round=args.from_round,
            to_round=to_round,
            tasks=len(round_record_tasks),
            execute=bool(args.execute),
            reset_target=bool(args.reset_target),
            profile=args.profile,
            workers=args.workers,
            gomining_rpm=args.gomining_rpm,
        )

        round_record_results = backfill_round_gap._fetch_round_records_parallel(
            tasks=round_record_tasks,
            workers=int(args.workers),
            probe_league_id=int(args.league_id),
            expected_league_ids={int(args.league_id)},
            bearer_token=effective_bearer,
            token_fetcher=token_fetcher,
            limiter=gomining_guard,
            progress_reporter=None,
        )
        round_records_ok = len([r for r in round_record_results if r.ok and isinstance(r.record, dict)])
        round_records_benign = len(
            [
                r
                for r in round_record_results
                if (not r.ok) and str(r.error) in backfill_round_gap.BENIGN_ROUND_RECORD_ERRORS
            ]
        )
        round_records_failed = len(round_record_results) - round_records_ok - round_records_benign
        if round_records_failed and not args.continue_on_error:
            failed_preview = [r.round_id for r in round_record_results if (not r.ok) and str(r.error) not in backfill_round_gap.BENIGN_ROUND_RECORD_ERRORS][:20]
            raise RuntimeError(f"Round record fetch failed for {round_records_failed} rounds; first failed roundIds={failed_preview}")

        fetched_records = [r.record for r in round_record_results if r.ok and isinstance(r.record, dict)]
        target_records, filter_stats = filter_records_for_target(
            fetched_records,  # type: ignore[arg-type]
            league_id=int(args.league_id),
            start_ended_at=str(args.start_ended_at),
        )

        ability_tasks: List[Tuple[int, int]] = []
        for rec in target_records:
            rid = main.safe_int(rec.get("round_id"))
            if rid is not None:
                ability_tasks.append((int(args.league_id), int(rid)))
        ability_tasks = sorted(set(ability_tasks))

        prefetch_results = backfill_round_gap._prefetch_rounds_with_retries(
            tasks=ability_tasks,
            workers=int(args.workers),
            attempts=int(args.prefetch_attempts),
            bearer_token=effective_bearer,
            token_fetcher=token_fetcher,
            limiter=gomining_guard,
            progress_reporter=None,
        )
        prefetch_ok = len([r for r in prefetch_results if r.ok])
        prefetch_failed = len(prefetch_results) - prefetch_ok
        if prefetch_failed and not args.continue_on_error:
            failed_preview = [r.round_id for r in prefetch_results if not r.ok][:20]
            raise RuntimeError(f"Ability prefetch failed for {prefetch_failed} rounds; first failed roundIds={failed_preview}")

        state_stats: Dict[str, Any] = {
            "executed": False,
            "cutover_round": int(args.from_round) - 1,
            "row_map_synced": 0,
            "pending_deleted": 0,
            "row_map_deleted": 0,
        }
        enqueued = 0
        flushed = 0
        cycles = 0
        completed = True

        if args.execute:
            if ctx.ws_id < 0 or getattr(ctx, "ws", None) is None:
                raise RuntimeError("Target sheet is missing and could not be created.")
            state = main.StateStore(main.STATE_DB_PATH)
            state_stats = prepare_target_state(
                state=state,
                ctx=ctx,
                from_round=int(args.from_round),
                to_round=int(to_round),
                execute=True,
                reset_target=bool(args.reset_target),
                read_limiter=read_limiter,
            )
            if target_records:
                state.upsert_api_round_records(target_records, source="backfill_eclipse_backfill")

            ability_id_to_name = main.build_ability_id_to_header(main.ABILITY_DIM_STATIC)
            ability_headers = list(main.ABILITY_HEADER_ORDER)
            round_ability_api = main.GoMiningRoundAbilityApiClient(
                bearer_token=effective_bearer,
                limiter=gomining_guard,
                user_leaderboard_url=main.ROUND_USER_LEADERBOARD_API_URL,
                page_limit=main.ROUND_API_PAGE_LIMIT,
                timeout_seconds=main.ROUND_API_TIMEOUT_SECONDS,
                max_retries=main.ROUND_API_MAX_RETRIES,
                token_fetcher=token_fetcher,
            )
            power_up_clan_api = main.GoMiningClanApiClient(
                bearer_token=effective_bearer,
                limiter=gomining_guard,
                leaderboard_url=main.CLAN_LEADERBOARD_API_URL,
                clan_get_by_id_url=main.CLAN_GET_BY_ID_API_URL,
                page_limit=main.CLAN_API_PAGE_LIMIT,
                timeout_seconds=main.CLAN_API_TIMEOUT_SECONDS,
                max_retries=main.CLAN_API_MAX_RETRIES,
                token_fetcher=token_fetcher,
            )
            backfill_round_gap.merge_prefetch_results_into_client(round_ability_api, prefetch_results)
            enqueued, flushed, cycles, completed = backfill_round_gap._run_enqueue_flush_for_sheet(
                state=state,
                ctx=ctx,
                records_for_sheet=target_records,
                round_ability_api=round_ability_api,
                power_up_clan_api=power_up_clan_api,
                ability_id_to_name=ability_id_to_name,
                ability_headers=ability_headers,
                write_limiter=write_limiter,
                read_limiter=read_limiter,
                max_cycles=int(args.max_cycles),
            )
            if completed and round_records_failed == 0:
                state.set_last_synced_round(ctx.ws_id, int(to_round))

        any_partial = bool(round_records_failed or prefetch_failed or not completed)
        report = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "execute": bool(args.execute),
            "target_sheet": str(args.target_sheet),
            "league_id": int(args.league_id),
            "from_round": int(args.from_round),
            "to_round": int(to_round),
            "start_ended_at": str(args.start_ended_at),
            "profile": str(args.profile),
            "workers": int(args.workers),
            "gomining_rpm": int(args.gomining_rpm),
            "prefetch_attempts": int(args.prefetch_attempts),
            "continue_on_error": bool(args.continue_on_error),
            "reset_target": bool(args.reset_target),
            "target_setup": {
                "created": bool(setup.created),
                "wrote_layout": bool(setup.wrote_layout),
                "reset_rows": bool(setup.reset_rows),
                "existing_rounds": int(setup.existing_rounds),
                "ws_id": int(ctx.ws_id),
            },
            "round_record_fetch_total": len(round_record_results),
            "round_record_fetch_ok": round_records_ok,
            "round_record_fetch_benign": round_records_benign,
            "round_record_fetch_failed": round_records_failed,
            "filter_stats": filter_stats,
            "target_records": len(target_records),
            "prefetch_total": len(prefetch_results),
            "prefetch_ok": prefetch_ok,
            "prefetch_failed": prefetch_failed,
            "state": state_stats,
            "enqueued": enqueued,
            "flushed": flushed,
            "cycles": cycles,
            "completed": completed,
        }
        report_path = _write_report(report)
        print(f"[REPORT] {report_path}")
        print(
            f"[SUMMARY] execute={bool(args.execute)} sheet={args.target_sheet!r} league={args.league_id} "
            f"range={args.from_round}..{to_round} tasks={len(round_record_tasks)} "
            f"records_ok={round_records_ok} benign={round_records_benign} failed={round_records_failed} "
            f"target_records={len(target_records)} prefetch_ok={prefetch_ok} prefetch_failed={prefetch_failed} "
            f"enqueued={enqueued} flushed={flushed} completed={completed}"
        )
        if any_partial:
            exit_code = 2
            print("[RESULT] completed with partial failures (exit=2).")
        else:
            exit_code = 0
            print("[RESULT] completed successfully (exit=0).")
        return exit_code
    except Exception as e:
        tb_text = traceback.format_exc()
        main.log_warn("eclipse_backfill.fatal", err=repr(e), traceback=tb_text[:8000])
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
    raise SystemExit(main_cli())
