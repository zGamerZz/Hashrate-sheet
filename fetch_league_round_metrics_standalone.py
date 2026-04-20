#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import os
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests


TOKEN_FIELDS = ("jwtToken", "token", "jwt", "access_token", "accessToken")
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

PATH_ROUND_GET_LAST = "/api/nft-game/round/get-last"
PATH_ROUND_CLAN_LEADERBOARD = "/api/nft-game/round/clan-leaderboard"
PATH_USER_LEADERBOARD_INDEX = "/api/nft-game/user-leaderboard/index"
PATH_CLAN_LEADERBOARD_INDEX_V2 = "/api/nft-game/clan-leaderboard/index-v2"

CSV_COLUMNS = [
    "timestamp_utc",
    "leagueId",
    "blockNumber",
    "multiplier",
    "gmtFund",
    "gmtPerBlock",
    "roundId",
    "League global TH",
    "endedAt_utc",
    "roundDuration_sec",
    "blocks_mined",
    "efficiency_league",
]


class FatalError(RuntimeError):
    pass


@dataclass
class RetryConfig:
    timeout_sec: int
    max_retries: int
    backoff_base_sec: float
    backoff_max_sec: float


def eprint(message: str) -> None:
    print(message, file=sys.stderr)


def parse_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(int(value))
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def parse_timestamp(value: Any) -> Optional[datetime]:
    if not value or not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_utc_iso_z(value: datetime) -> str:
    utc = value.astimezone(timezone.utc)
    return utc.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def now_utc_iso_z() -> str:
    return to_utc_iso_z(datetime.now(timezone.utc))


def unwrap_data(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = payload.get("data")
    if isinstance(data, dict):
        return data
    return payload


def build_url(api_base_url: str, path: str) -> str:
    if path.startswith("http://") or path.startswith("https://"):
        return path
    return f"{api_base_url.rstrip('/')}/{path.lstrip('/')}"


def sleep_with_backoff(cfg: RetryConfig, attempt_index: int) -> None:
    delay = min(cfg.backoff_max_sec, cfg.backoff_base_sec * (2**attempt_index))
    jitter = random.uniform(0.8, 1.2)
    time.sleep(delay * jitter)


def _request_json(
    *,
    session: requests.Session,
    method: str,
    url: str,
    cfg: RetryConfig,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    attempts = cfg.max_retries + 1
    for attempt in range(attempts):
        try:
            response = session.request(
                method=method.upper(),
                url=url,
                params=params,
                json=json_body,
                headers=headers,
                timeout=cfg.timeout_sec,
            )
        except requests.RequestException as exc:
            if attempt < attempts - 1:
                sleep_with_backoff(cfg, attempt)
                continue
            raise FatalError(f"request failed after {attempts} attempts: {method.upper()} {url}: {exc}") from exc

        if response.status_code in RETRYABLE_STATUS_CODES and attempt < attempts - 1:
            sleep_with_backoff(cfg, attempt)
            continue

        if response.status_code >= 400:
            body = response.text[:500].strip()
            raise FatalError(
                f"HTTP {response.status_code} for {method.upper()} {url}"
                f"{': ' + body if body else ''}"
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise FatalError(f"non-JSON response from {method.upper()} {url}") from exc

        if not isinstance(payload, dict):
            raise FatalError(f"JSON root must be an object for {method.upper()} {url}")
        return payload

    raise FatalError(f"request failed after {attempts} attempts: {method.upper()} {url}")


def fetch_token_from_service(
    *,
    session: requests.Session,
    token_url: str,
    token_auth: str,
    cfg: RetryConfig,
) -> str:
    headers = {"X-Auth": token_auth}
    payload = _request_json(
        session=session,
        method="GET",
        url=token_url,
        cfg=cfg,
        headers=headers,
    )
    for field in TOKEN_FIELDS:
        value = payload.get(field)
        if value:
            token = str(value).strip()
            if token:
                return token
    raise FatalError("token service response does not contain a supported token field")


def resolve_bearer_token(
    *,
    session: requests.Session,
    token_url: Optional[str],
    token_auth: Optional[str],
    jwt_fallback: Optional[str],
    cfg: RetryConfig,
) -> str:
    fallback_token = (jwt_fallback or "").strip()

    service_ready = bool((token_url or "").strip() and (token_auth or "").strip())
    if service_ready:
        try:
            return fetch_token_from_service(
                session=session,
                token_url=str(token_url).strip(),
                token_auth=str(token_auth).strip(),
                cfg=cfg,
            )
        except FatalError as exc:
            if fallback_token:
                eprint(f"WARN token service failed; falling back to JWT: {exc}")
                return fallback_token
            raise

    if fallback_token:
        return fallback_token

    raise FatalError(
        "no auth available: provide TOKEN_URL+TOKEN_AUTH or pass --jwt / set JWT_TOKEN"
    )


class ApiClient:
    def __init__(
        self,
        *,
        session: requests.Session,
        api_base_url: str,
        bearer_token: str,
        cfg: RetryConfig,
    ) -> None:
        self.session = session
        self.api_base_url = api_base_url
        self.bearer_token = bearer_token
        self.cfg = cfg

    def request_json(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "Content-Type": "application/json",
        }
        return _request_json(
            session=self.session,
            method=method,
            url=build_url(self.api_base_url, path),
            cfg=self.cfg,
            headers=headers,
            params=params,
            json_body=json_body,
        )


def payload_round_id(payload: Dict[str, Any]) -> Optional[int]:
    data = unwrap_data(payload)
    return parse_int(data.get("roundId") or data.get("id"))


def payload_league_id(payload: Dict[str, Any]) -> Optional[int]:
    data = unwrap_data(payload)
    return parse_int(data.get("leagueId"))


def is_round_not_found(exc: Exception) -> bool:
    text = str(exc).lower()
    return "http 404" in text and "round not found" in text


def fetch_round_clan_leaderboard_by_round_id(client: ApiClient, round_id: int) -> Dict[str, Any]:
    return client.request_json(
        "POST",
        PATH_ROUND_CLAN_LEADERBOARD,
        json_body={
            "roundId": int(round_id),
            "pagination": {"skip": 0, "limit": 1},
        },
    )


def fetch_get_last_with_probe(client: ApiClient, league_id: int) -> tuple[Dict[str, Any], bool]:
    primary = client.request_json(
        "GET",
        PATH_ROUND_GET_LAST,
        params={"leagueId": int(league_id)},
    )
    primary_league = payload_league_id(primary)
    if primary_league == league_id:
        return primary, False

    secondary: Optional[Dict[str, Any]] = None
    try:
        secondary = client.request_json(
            "POST",
            PATH_ROUND_GET_LAST,
            json_body={"leagueId": int(league_id)},
        )
    except FatalError as exc:
        eprint(f"WARN get-last POST probe failed; continuing with GET payload: {exc}")

    if secondary is not None and payload_league_id(secondary) == league_id:
        return secondary, False

    if secondary is None:
        return primary, True

    primary_round = payload_round_id(primary)
    secondary_round = payload_round_id(secondary)
    if primary_round is None:
        return secondary, True
    if secondary_round is None:
        return primary, True
    return (secondary if secondary_round >= primary_round else primary), True


def scan_closed_round_for_league(
    *,
    client: ApiClient,
    league_id: int,
    start_round_id: int,
    lookback: int,
) -> Optional[int]:
    min_round = max(1, start_round_id - lookback)
    for round_id in range(start_round_id, min_round - 1, -1):
        try:
            payload = fetch_round_clan_leaderboard_by_round_id(client, round_id)
        except FatalError as exc:
            if is_round_not_found(exc):
                continue
            raise

        data = unwrap_data(payload)
        found_league = parse_int(data.get("leagueId"))
        found_round = parse_int(data.get("roundId") or data.get("id"))
        active = data.get("active")
        if found_league == league_id and found_round is not None and active is False:
            return found_round
    return None


def require_int(name: str, value: Any) -> int:
    parsed = parse_int(value)
    if parsed is None:
        raise FatalError(f"missing or invalid integer field: {name}")
    return parsed


def require_float(name: str, value: Any) -> float:
    parsed = parse_float(value)
    if parsed is None:
        raise FatalError(f"missing or invalid numeric field: {name}")
    return parsed


def compute_round_duration_sec(round_payload: Dict[str, Any], ended_at: datetime) -> int:
    data = unwrap_data(round_payload)
    started_at = parse_timestamp(data.get("startedAt"))
    round_time = parse_int(data.get("roundTime"))

    if started_at is not None:
        duration = int(round((ended_at - started_at).total_seconds()))
        if duration < 0:
            raise FatalError("round duration is negative (endedAt < startedAt)")
        return duration
    if round_time is not None:
        return round_time
    raise FatalError("missing both startedAt and roundTime for roundDuration_sec")


def fetch_metric_indexes(
    *,
    client: ApiClient,
    league_id: int,
    calculated_at: str,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    user_payload = client.request_json(
        "POST",
        PATH_USER_LEADERBOARD_INDEX,
        json_body={
            "leagueId": int(league_id),
            "calculatedAt": calculated_at,
            "pagination": {"skip": 0, "limit": 1},
        },
    )
    clan_payload = client.request_json(
        "POST",
        PATH_CLAN_LEADERBOARD_INDEX_V2,
        json_body={
            "leagueId": int(league_id),
            "calculatedAt": calculated_at,
            "pagination": {"skip": 0, "limit": 1},
        },
    )
    return user_payload, clan_payload


def build_csv_text(row: Dict[str, Any]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=CSV_COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerow(row)
    return buffer.getvalue()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch final-round metrics for one leagueId from GoMining APIs "
            "and print exactly one CSV row."
        )
    )
    parser.add_argument("--league-id", type=int, required=True, help="Target league id.")
    parser.add_argument(
        "--api-base-url",
        default=os.getenv("API_BASE_URL", "https://api.gomining.com"),
        help="API base URL (env fallback: API_BASE_URL).",
    )
    parser.add_argument(
        "--token-url",
        default=os.getenv("TOKEN_URL"),
        help="Token service URL (env fallback: TOKEN_URL).",
    )
    parser.add_argument(
        "--token-auth",
        default=os.getenv("TOKEN_AUTH"),
        help="Token service X-Auth value (env fallback: TOKEN_AUTH).",
    )
    parser.add_argument(
        "--jwt",
        default=os.getenv("JWT_TOKEN"),
        help="Direct JWT fallback token (env fallback: JWT_TOKEN).",
    )
    parser.add_argument("--timeout-sec", type=int, default=20, help="HTTP timeout in seconds.")
    parser.add_argument("--max-retries", type=int, default=5, help="HTTP retries per request.")
    parser.add_argument("--backoff-base-sec", type=float, default=0.5, help="Backoff base seconds.")
    parser.add_argument("--backoff-max-sec", type=float, default=20.0, help="Backoff max seconds.")
    parser.add_argument(
        "--round-scan-lookback",
        type=int,
        default=200,
        help="Rounds to scan backwards when get-last league mismatch happens.",
    )
    parser.add_argument(
        "--output-file",
        default="",
        help="Optional output CSV file path (same content as stdout).",
    )
    return parser.parse_args()


def run() -> int:
    args = parse_args()

    if args.league_id <= 0:
        raise FatalError("--league-id must be > 0")
    if args.timeout_sec <= 0:
        raise FatalError("--timeout-sec must be > 0")
    if args.max_retries < 0:
        raise FatalError("--max-retries must be >= 0")
    if args.backoff_base_sec < 0:
        raise FatalError("--backoff-base-sec must be >= 0")
    if args.backoff_max_sec <= 0:
        raise FatalError("--backoff-max-sec must be > 0")
    if args.round_scan_lookback < 1:
        raise FatalError("--round-scan-lookback must be >= 1")

    cfg = RetryConfig(
        timeout_sec=int(args.timeout_sec),
        max_retries=int(args.max_retries),
        backoff_base_sec=float(args.backoff_base_sec),
        backoff_max_sec=float(args.backoff_max_sec),
    )

    with requests.Session() as session:
        bearer_token = resolve_bearer_token(
            session=session,
            token_url=args.token_url,
            token_auth=args.token_auth,
            jwt_fallback=args.jwt,
            cfg=cfg,
        )
        client = ApiClient(
            session=session,
            api_base_url=str(args.api_base_url).strip(),
            bearer_token=bearer_token,
            cfg=cfg,
        )

        get_last_payload, unresolved_mismatch = fetch_get_last_with_probe(client, args.league_id)
        get_last_data = unwrap_data(get_last_payload)
        previous_round_id = parse_int(get_last_data.get("previousRoundId"))
        if previous_round_id is None or previous_round_id <= 0:
            raise FatalError("final-round mode requires previousRoundId in get-last payload")

        if unresolved_mismatch:
            start_round_id = payload_round_id(get_last_payload)
            if start_round_id is None or start_round_id <= 0:
                raise FatalError("cannot start round scan: get-last payload has no valid round id")
            target_round_id = scan_closed_round_for_league(
                client=client,
                league_id=args.league_id,
                start_round_id=start_round_id,
                lookback=args.round_scan_lookback,
            )
            if target_round_id is None:
                raise FatalError(
                    f"round scan failed: no closed round found for league {args.league_id} "
                    f"within lookback {args.round_scan_lookback} from start round {start_round_id}"
                )
            eprint(
                f"INFO get-last mismatch detected; resolved target round via scan: "
                f"league={args.league_id} roundId={target_round_id}"
            )
        else:
            target_round_id = previous_round_id

        round_payload = fetch_round_clan_leaderboard_by_round_id(client, target_round_id)
        round_data = unwrap_data(round_payload)

        resolved_round_id = require_int("roundId", round_data.get("roundId") or round_data.get("id"))
        if resolved_round_id != target_round_id:
            raise FatalError(
                f"target round mismatch: requested {target_round_id}, payload has {resolved_round_id}"
            )

        resolved_league_id = require_int("leagueId", round_data.get("leagueId"))
        if resolved_league_id != args.league_id:
            raise FatalError(
                f"payload league mismatch: expected {args.league_id}, got {resolved_league_id}"
            )

        active_flag = round_data.get("active")
        if isinstance(active_flag, bool) and active_flag:
            raise FatalError(f"target round {resolved_round_id} is active; expected a finalized round")

        block_number = require_int("blockNumber", round_data.get("blockNumber"))
        multiplier = require_int("multiplier", round_data.get("multiplier"))
        ended_at = parse_timestamp(round_data.get("endedAt"))
        if ended_at is None:
            raise FatalError("missing or invalid field: endedAt")
        ended_at_utc = to_utc_iso_z(ended_at)
        round_duration_sec = compute_round_duration_sec(round_payload, ended_at)
        calculated_at = ended_at_utc

        user_index_payload, clan_index_payload = fetch_metric_indexes(
            client=client,
            league_id=args.league_id,
            calculated_at=calculated_at,
        )
        user_data = unwrap_data(user_index_payload)
        clan_data = unwrap_data(clan_index_payload)

        gmt_fund = require_float("gmtFund", user_data.get("gmtFund"))
        user_blocks_mined = parse_int(user_data.get("totalMinedBlocks"))
        clan_blocks_mined = parse_int(clan_data.get("totalMinedBlocks"))
        blocks_mined = user_blocks_mined if user_blocks_mined is not None else clan_blocks_mined
        if blocks_mined is None:
            raise FatalError("missing blocks_mined in both user and clan metric endpoints")
        if blocks_mined <= 0:
            raise FatalError(f"blocks_mined must be > 0 to compute gmtPerBlock (got {blocks_mined})")

        league_global_th = require_float("totalPower", clan_data.get("totalPower"))
        efficiency_league = require_float(
            "weightedEnergyEfficiencyPerTh",
            clan_data.get("weightedEnergyEfficiencyPerTh"),
        )
        gmt_per_block = gmt_fund / float(blocks_mined)

        csv_row: Dict[str, Any] = {
            "timestamp_utc": now_utc_iso_z(),
            "leagueId": args.league_id,
            "blockNumber": block_number,
            "multiplier": multiplier,
            "gmtFund": gmt_fund,
            "gmtPerBlock": gmt_per_block,
            "roundId": resolved_round_id,
            "League global TH": league_global_th,
            "endedAt_utc": ended_at_utc,
            "roundDuration_sec": round_duration_sec,
            "blocks_mined": blocks_mined,
            "efficiency_league": efficiency_league,
        }

        csv_text = build_csv_text(csv_row)
        sys.stdout.write(csv_text)
        sys.stdout.flush()

        output_file = str(args.output_file or "").strip()
        if output_file:
            with open(output_file, "w", encoding="utf-8", newline="") as handle:
                handle.write(csv_text)
            eprint(f"INFO wrote CSV output to {output_file}")

    return 0


def main() -> None:
    try:
        exit_code = run()
    except FatalError as exc:
        eprint(f"ERROR {exc}")
        raise SystemExit(1) from exc
    except KeyboardInterrupt:
        eprint("ERROR interrupted")
        raise SystemExit(130)
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
