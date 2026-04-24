#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from typing import Any, Callable, Dict, List, Optional, Tuple

import main


DEFAULT_FROM_ROUND = 943349


def _resolve_effective_bearer() -> Tuple[str, Optional[Callable[[], Optional[str]]]]:
    token_fetcher: Optional[Callable[[], Optional[str]]] = None
    effective_bearer = main.GOMINING_BEARER_TOKEN.strip()
    if main.TOKEN_URL and main.TOKEN_X_AUTH:
        token_fetcher = main.fetch_bearer_token_from_auth_api
        fetched = (token_fetcher() or "").strip()
        if fetched:
            effective_bearer = fetched
        elif not effective_bearer:
            raise RuntimeError("Failed to fetch bearer token from TOKEN_URL and no GOMINING_BEARER_TOKEN fallback is set.")
        else:
            main.log_warn("token_api.startup_failed_using_fallback", token_url=main.TOKEN_URL)
    if not effective_bearer:
        raise RuntimeError("GOMINING_BEARER_TOKEN is required when TOKEN_URL/TOKEN_X_AUTH is not configured.")
    return effective_bearer, token_fetcher


def _load_main_contexts(sh: Any, read_limiter: main.TokenBucket, expected_cols: int) -> Dict[int, main.SheetContext]:
    contexts: Dict[int, main.SheetContext] = {}
    worksheets = sh.worksheets()
    selectors = main.read_sheet_selectors(sh, worksheets, read_limiter)
    for ws in worksheets:
        sel = selectors.get(ws.id, {})
        marker = str(sel.get("marker") or "")
        league_id = main.safe_int(sel.get("league_id"))
        if marker != main.MAIN_SHEET_MARKER or league_id is None:
            continue
        ctx = main.SheetContext(
            ws_id=ws.id,
            title=ws.title,
            ws=ws,
            league_id=league_id,
            kind="main",
            expected_cols=expected_cols,
            round_col_idx=6,
        )
        ctx.next_row = main._detect_next_row(ws, read_limiter, round_col_index=(ctx.round_col_idx or 6) + 1)
        contexts[ws.id] = ctx
    return contexts


def _prepare_backfill_window(
    state: main.StateStore,
    contexts: Dict[int, main.SheetContext],
    from_round: int,
    execute: bool,
) -> List[Dict[str, Any]]:
    cutover_round = from_round - 1
    out: List[Dict[str, Any]] = []
    for ws_id, ctx in contexts.items():
        state.upsert_sheet_meta(ws_id, ctx.title, ctx.league_id, layout_sig=None)
        before = state.get_sheet_state(ws_id) or {}
        prev_last = main.safe_int(before.get("last_synced_round"))
        prev_cutover = main.safe_int(before.get("price_cutover_round"))
        out.append(
            {
                "ws_id": ws_id,
                "sheet": ctx.title,
                "league_id": ctx.league_id,
                "prev_last_synced": prev_last,
                "prev_price_cutover": prev_cutover,
                "new_last_synced": cutover_round,
                "new_price_cutover": cutover_round,
            }
        )
        if not execute:
            continue
        # Force reprocessing from from_round and enable Power Up GMT filling from that point.
        state.conn.execute(
            "UPDATE sheet_state SET last_synced_round=?, price_cutover_round=?, updated_at=? WHERE sheet_id=?",
            (cutover_round, cutover_round, main.time.time(), ws_id),
        )
        # Reset pending main ops in the target window to avoid stale retries/checksums.
        state.conn.execute(
            "DELETE FROM pending_ops "
            "WHERE sheet_id=? AND op_type IN ('append_round','update_round') AND round_id>=?",
            (ws_id, from_round),
        )
    if execute:
        state.conn.commit()
    return out


def _run_backfill_cycles(
    db: main.DBClient,
    state: main.StateStore,
    contexts: Dict[int, main.SheetContext],
    ability_id_to_name: Dict[str, str],
    ability_headers: List[str],
    round_ability_api: main.GoMiningRoundAbilityApiClient,
    power_up_clan_api: Optional[main.GoMiningClanApiClient],
    write_limiter: main.TokenBucket,
    max_cycles: int,
) -> Tuple[int, int, int, bool]:
    total_enqueued = 0
    total_flushed = 0
    completed = False

    for cycle in range(1, max_cycles + 1):
        enq = main.enqueue_main_sheet_ops(
            db,
            state,
            contexts,
            ability_id_to_name,
            ability_headers,
            round_ability_api,
            power_up_clan_api=power_up_clan_api,
            flush_tick=None,
        )
        total_enqueued += enq

        flushed_cycle = 0
        while True:
            done = main.flush_sheet_queue_with_rate_limit(state, contexts, write_limiter)
            if done <= 0:
                break
            flushed_cycle += done
            total_flushed += done

        queue_total = state.queue_total_count()
        queue_due = state.queue_due_count()
        main.log_info(
            "backfill.cycle",
            cycle=cycle,
            enqueued=enq,
            flushed=flushed_cycle,
            queue_total=queue_total,
            queue_due=queue_due,
        )

        if enq == 0 and queue_due == 0:
            completed = True
            return total_enqueued, total_flushed, cycle, completed

    return total_enqueued, total_flushed, max_cycles, completed


def main_cli() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill Power Up GMT Price in main sheets from a target round onward.",
    )
    parser.add_argument(
        "--from-round",
        type=int,
        default=DEFAULT_FROM_ROUND,
        help=f"First round to include in backfill (default: {DEFAULT_FROM_ROUND}).",
    )
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=60,
        help="Max enqueue/flush cycles before stopping (default: 60).",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Perform backfill. Without this flag, script only prints the planned changes.",
    )
    args = parser.parse_args()

    if args.from_round <= 0:
        raise ValueError("--from-round must be > 0")
    if args.max_cycles <= 0:
        raise ValueError("--max-cycles must be > 0")

    lock = main.SingleInstanceLock(main.LOCK_FILE_PATH)
    state: Optional[main.StateStore] = None
    db: Optional[main.DBClient] = None
    lock.acquire()
    try:
        db = main.DBClient()
        db.connect()
        state = main.StateStore(main.STATE_DB_PATH)

        write_limiter = main.TokenBucket(main.GS_WRITE_REQ_PER_MIN, name="sheets_write")
        read_limiter = main.TokenBucket(main.GS_READ_REQ_PER_MIN, name="sheets_read")
        gomining_limiter = main.TokenBucket(main.GOMINING_API_REQ_PER_MIN, name="gomining_api")

        effective_bearer, token_fetcher = _resolve_effective_bearer()
        round_ability_api = main.GoMiningRoundAbilityApiClient(
            effective_bearer,
            limiter=gomining_limiter,
            user_leaderboard_url=main.ROUND_USER_LEADERBOARD_API_URL,
            page_limit=main.ROUND_API_PAGE_LIMIT,
            timeout_seconds=main.ROUND_API_TIMEOUT_SECONDS,
            max_retries=main.ROUND_API_MAX_RETRIES,
            token_fetcher=token_fetcher,
        )
        power_up_clan_api: Optional[main.GoMiningClanApiClient] = main.GoMiningClanApiClient(
            effective_bearer,
            limiter=gomining_limiter,
            leaderboard_url=main.CLAN_LEADERBOARD_API_URL,
            clan_get_by_id_url=main.CLAN_GET_BY_ID_API_URL,
            page_limit=main.CLAN_API_PAGE_LIMIT,
            timeout_seconds=main.CLAN_API_TIMEOUT_SECONDS,
            max_retries=main.CLAN_API_MAX_RETRIES,
            token_fetcher=token_fetcher,
        )

        ability_id_to_name = main.build_ability_id_to_header(main.ABILITY_DIM_STATIC)
        ability_headers = list(main.ABILITY_HEADER_ORDER)
        expected_cols = len(main.BASE_HEADERS) + len(ability_headers) + 7

        sh = main.open_spreadsheet()
        contexts = _load_main_contexts(sh, read_limiter, expected_cols)
        if not contexts:
            print("[INFO] No main sheets found (marker not set or league id missing). Nothing to do.")
            return

        planned = _prepare_backfill_window(state, contexts, args.from_round, execute=args.execute)
        print(f"[INFO] Main sheets found: {len(planned)} | from_round={args.from_round} | execute={args.execute}")
        for row in planned:
            print(
                f"[PLAN] sheet='{row['sheet']}' ws_id={row['ws_id']} league={row['league_id']} "
                f"last_synced: {row['prev_last_synced']} -> {row['new_last_synced']} "
                f"price_cutover: {row['prev_price_cutover']} -> {row['new_price_cutover']}"
            )

        if not args.execute:
            print("[DRY] No changes applied. Re-run with --execute to start backfill.")
            return

        if main.DRY_RUN:
            print("[WARN] DRY_RUN=1 in environment. Queue processing will skip actual sheet writes.")

        total_enq, total_flushed, cycles, completed = _run_backfill_cycles(
            db,
            state,
            contexts,
            ability_id_to_name,
            ability_headers,
            round_ability_api,
            power_up_clan_api,
            write_limiter,
            max_cycles=args.max_cycles,
        )

        print(
            f"[DONE] cycles={cycles} completed={completed} "
            f"enqueued_total={total_enq} flushed_total={total_flushed} "
            f"queue_total={state.queue_total_count()} queue_due={state.queue_due_count()}"
        )
        if not completed:
            print(
                "[WARN] Backfill stopped due to --max-cycles limit or delayed retries. "
                "Run again with higher --max-cycles if needed."
            )
    finally:
        try:
            if state is not None:
                state.close()
        except Exception:
            pass
        try:
            if db is not None:
                db.close()
        except Exception:
            pass
        lock.release()


if __name__ == "__main__":
    main_cli()
