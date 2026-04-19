import os
import json
import tempfile
import unittest
from typing import Any, Dict, List

import backfill_round_gap as backfill
import main


class SyncCoreTests(unittest.TestCase):
    @staticmethod
    def _seed_api_rounds(state: main.StateStore, rounds: List[Dict[str, Any]]) -> None:
        records: List[Dict[str, Any]] = []
        for rec in rounds:
            item = dict(rec)
            if not item.get("ended_at"):
                item["ended_at"] = item.get("snapshot_ts") or "2026-01-01T00:00:00+00:00"
            records.append(item)
        state.upsert_api_round_records(records, source="test")

    def test_canonical_ability_header_mapping(self) -> None:
        self.assertEqual(main.canonical_ability_header("Power Up Boost"), "Power Up Boost")
        self.assertEqual(main.canonical_ability_header("Clan Power Up Boost"), "Clan Power Up Boost")
        self.assertEqual(main.canonical_ability_header("Rocket (x1)"), "Rocket (x1)")
        self.assertEqual(main.canonical_ability_header("Boost (x10)"), "Rocket (x10)")
        self.assertEqual(main.canonical_ability_header("Instant Boost (x100)"), "Instant Boost (x100)")
        self.assertEqual(main.canonical_ability_header("Echo Boost (x10)"), "Echo Boost (x10)")
        self.assertEqual(main.canonical_ability_header("Focus Boost (x1)"), "Focus Boost (x1)")
        self.assertIsNone(main.canonical_ability_header("Stealer"))
        self.assertIsNone(main.canonical_ability_header("Maintenance"))

    def test_build_ability_id_to_header_filters_non_boost(self) -> None:
        catalog = [
            ("a", "Power Up Boost", 2),
            ("b", "Clan Power Up Boost", 1),
            ("c", "Boost (x10)", 13),
            ("d", "Stealer", 99),
        ]
        mapping = main.build_ability_id_to_header(catalog)
        self.assertEqual(mapping["a"], "Power Up Boost")
        self.assertEqual(mapping["b"], "Clan Power Up Boost")
        self.assertEqual(mapping["c"], "Rocket (x10)")
        self.assertNotIn("d", mapping)

    def test_parse_league_index_response(self) -> None:
        payload = {
            "data": {
                "array": [
                    {"id": 1, "name": "odyssey"},
                    {"id": "3", "name": "eclipse"},
                    {"id": None, "name": "bad"},
                ]
            }
        }
        parsed = main.parse_league_index_response(payload)
        self.assertEqual(parsed, {1: "odyssey", 3: "eclipse"})

    def test_parse_league_index_response_with_alternate_key(self) -> None:
        payload = {
            "data": {
                "leagues": [
                    {"id": 7, "name": "gamma"},
                    {"id": "8", "name": "delta"},
                ]
            }
        }
        parsed = main.parse_league_index_response(payload)
        self.assertEqual(parsed, {7: "gamma", 8: "delta"})

    def test_fetch_league_index_payload_retries_until_non_empty(self) -> None:
        class FakeResponse:
            def __init__(self, status_code: int, body: Dict[str, Any]) -> None:
                self.status_code = status_code
                self._body = body
                self.text = json.dumps(body)
                self.headers: Dict[str, str] = {}

            def json(self) -> Dict[str, Any]:
                return self._body

        calls: List[Dict[str, Any]] = []

        def fake_post(url: str, headers: Dict[str, str], json: Dict[str, Any], timeout: int) -> FakeResponse:
            calls.append({"url": url, "headers": headers, "json": json, "timeout": timeout})
            if len(calls) == 1:
                return FakeResponse(200, {"data": {"array": []}})
            return FakeResponse(200, {"data": {"array": [{"id": 1, "name": "odyssey"}]}})

        orig_post = main.requests.post
        orig_lookback = main.LEAGUE_INDEX_LOOKBACK_DAYS
        orig_try_without = main.LEAGUE_INDEX_TRY_WITHOUT_CALCULATED_AT
        try:
            main.requests.post = fake_post  # type: ignore[assignment]
            main.LEAGUE_INDEX_LOOKBACK_DAYS = 1
            main.LEAGUE_INDEX_TRY_WITHOUT_CALCULATED_AT = False
            payload = main.fetch_league_index_payload_from_api("token")
            self.assertIsInstance(payload, dict)
            parsed = main.parse_league_index_response(payload or {})
            self.assertEqual(parsed, {1: "odyssey"})
            self.assertEqual(len(calls), 2)
            self.assertTrue(all("calculatedAt" in c["json"] for c in calls))
        finally:
            main.requests.post = orig_post  # type: ignore[assignment]
            main.LEAGUE_INDEX_LOOKBACK_DAYS = orig_lookback
            main.LEAGUE_INDEX_TRY_WITHOUT_CALCULATED_AT = orig_try_without

    def test_round_metrics_triplet_merge_success(self) -> None:
        client = main.GoMiningRoundMetricsApiClient(
            bearer_token="token",
            limiter=main.TokenBucket(9999, name="test"),
            max_retries=1,
        )
        calls: List[Dict[str, Any]] = []

        def fake_request(
            method: str,
            url: str,
            *,
            params: Dict[str, Any] | None = None,
            json_payload: Dict[str, Any] | None = None,
            op: str,
            **ctx: Any,
        ) -> Dict[str, Any] | None:
            calls.append({"method": method, "url": url, "params": params, "json_payload": json_payload, "op": op, "ctx": ctx})
            if op == "round_get_last":
                return {
                    "data": {
                        "id": 957598,
                        "roundId": 957598,
                        "previousRoundId": 957597,
                        "leagueId": 1,
                    }
                }
            if op == "round_clan_leaderboard_by_round":
                rid = (json_payload or {}).get("roundId")
                if rid == 957597:
                    return {
                        "data": {
                            "roundId": 957597,
                            "leagueId": 1,
                            "blockNumber": 945602,
                            "multiplier": 1.0,
                            "startedAt": "2026-04-18T10:34:49.883Z",
                            "endedAt": "2026-04-18T10:35:49.883Z",
                            "active": False,
                        }
                    }
            if op == "user_leaderboard_index":
                return {"data": {"gmtFund": 189651.96424237, "totalMinedBlocks": 1172}}
            if op == "clan_leaderboard_index_v2":
                return {
                    "data": {
                        "totalMinedBlocks": 1172,
                        "totalPower": 1833891.2399650307,
                        "weightedEnergyEfficiencyPerTh": 20.05189094789897,
                    }
                }
            return None

        client._request_json_with_retry = fake_request  # type: ignore[method-assign]
        rec = client.fetch_round_metrics_triplet(league_id=1, strict_league_validation=False)
        self.assertIsInstance(rec, dict)
        self.assertEqual(calls[0]["op"], "round_get_last")
        self.assertEqual(calls[1]["op"], "round_clan_leaderboard_by_round")
        self.assertEqual(calls[2]["op"], "user_leaderboard_index")
        self.assertEqual(calls[3]["op"], "clan_leaderboard_index_v2")
        self.assertEqual(rec["league_id"], 1)
        self.assertEqual(rec["round_id"], 957597)
        self.assertEqual(rec["block_number"], 945602)
        self.assertAlmostEqual(rec["multiplier"], 1.0)
        self.assertAlmostEqual(rec["gmt_fund"], 189651.96424237)
        self.assertEqual(rec["blocks_mined"], 1172)
        self.assertAlmostEqual(rec["gmt_per_block"], 189651.96424237 / 1172.0)
        self.assertAlmostEqual(rec["league_th"], 1833891.2399650307)
        self.assertAlmostEqual(rec["efficiency_league"], 20.05189094789897)
        self.assertEqual(rec["ended_at"], "2026-04-18T10:35:49+00:00")
        self.assertEqual(rec["round_duration_sec"], 60)
        calc_at_user = calls[2]["json_payload"]["calculatedAt"]
        calc_at_clan = calls[3]["json_payload"]["calculatedAt"]
        self.assertTrue(isinstance(calc_at_user, str) and calc_at_user.endswith(".000Z"))
        self.assertEqual(calc_at_user, calc_at_clan)
        self.assertEqual(calls[0]["params"], {"leagueId": 1})
        self.assertEqual(calls[2]["json_payload"]["leagueId"], 1)
        self.assertEqual(calls[3]["json_payload"]["leagueId"], 1)

    def test_round_metrics_triplet_blocks_mined_fallback_to_clan(self) -> None:
        client = main.GoMiningRoundMetricsApiClient(
            bearer_token="token",
            limiter=main.TokenBucket(9999, name="test"),
            max_retries=1,
        )

        def fake_request(
            method: str,
            url: str,
            *,
            params: Dict[str, Any] | None = None,
            json_payload: Dict[str, Any] | None = None,
            op: str,
            **ctx: Any,
        ) -> Dict[str, Any] | None:
            _ = (method, url, params, json_payload, ctx)
            if op == "round_get_last":
                return {"data": {"id": 1001, "roundId": 1001, "previousRoundId": 1000, "leagueId": 3}}
            if op == "round_clan_leaderboard_by_round":
                return {
                    "data": {
                        "roundId": 1000,
                        "leagueId": 3,
                        "blockNumber": 10,
                        "multiplier": 2.0,
                        "endedAt": "2026-04-18T10:00:00Z",
                        "roundTime": 60,
                        "active": False,
                    }
                }
            if op == "user_leaderboard_index":
                return {"data": {"gmtFund": 100.0, "totalMinedBlocks": None}}
            if op == "clan_leaderboard_index_v2":
                return {"data": {"totalMinedBlocks": 200, "totalPower": 3000.0, "weightedEnergyEfficiencyPerTh": 18.5}}
            return None

        client._request_json_with_retry = fake_request  # type: ignore[method-assign]
        rec = client.fetch_round_metrics_triplet(league_id=3, strict_league_validation=False)
        self.assertIsInstance(rec, dict)
        self.assertEqual(rec["round_id"], 1000)
        self.assertEqual(rec["blocks_mined"], 200)
        self.assertAlmostEqual(rec["gmt_per_block"], 0.5)

    def test_round_metrics_triplet_mismatch_post_probe_resolves(self) -> None:
        client = main.GoMiningRoundMetricsApiClient(
            bearer_token="token",
            limiter=main.TokenBucket(9999, name="test"),
            max_retries=1,
        )
        warn_events: List[Dict[str, Any]] = []
        orig_log_warn = main.log_warn

        def fake_log_warn(msg: str, **fields: Any) -> None:
            warn_events.append({"msg": msg, "fields": dict(fields)})

        def fake_request(
            method: str,
            url: str,
            *,
            params: Dict[str, Any] | None = None,
            json_payload: Dict[str, Any] | None = None,
            op: str,
            **ctx: Any,
        ) -> Dict[str, Any] | None:
            _ = (method, url, params, json_payload, ctx)
            if op == "round_get_last":
                return {"data": {"id": 2002, "roundId": 2002, "previousRoundId": 2001, "leagueId": 25}}
            if op == "round_get_last_post_probe":
                return {"data": {"id": 2002, "roundId": 2002, "previousRoundId": 2001, "leagueId": 1}}
            if op == "round_clan_leaderboard_by_round":
                return {
                    "data": {
                        "roundId": 2001,
                        "leagueId": 1,
                        "blockNumber": 11,
                        "multiplier": 1.0,
                        "endedAt": "2026-04-18T10:10:00Z",
                        "roundTime": 60,
                        "active": False,
                    }
                }
            if op == "user_leaderboard_index":
                return {"data": {"gmtFund": 50.0, "totalMinedBlocks": 10}}
            if op == "clan_leaderboard_index_v2":
                return {"data": {"totalMinedBlocks": 10, "totalPower": 2000.0, "weightedEnergyEfficiencyPerTh": 20.0}}
            return None

        try:
            main.log_warn = fake_log_warn  # type: ignore[assignment]
            client._request_json_with_retry = fake_request  # type: ignore[method-assign]
            rec = client.fetch_round_metrics_triplet(league_id=1, strict_league_validation=False)
        finally:
            main.log_warn = orig_log_warn  # type: ignore[assignment]

        self.assertIsInstance(rec, dict)
        self.assertEqual(rec["round_id"], 2001)
        mismatch_logs = [e for e in warn_events if e["msg"] == "gomining_api.round_get_last_league_mismatch"]
        self.assertGreaterEqual(len(mismatch_logs), 1)
        self.assertEqual(mismatch_logs[0]["fields"].get("requested_league_id"), 1)
        self.assertEqual(mismatch_logs[0]["fields"].get("payload_league_id"), 25)

    def test_round_metrics_triplet_mismatch_scan_finds_closed_round(self) -> None:
        client = main.GoMiningRoundMetricsApiClient(
            bearer_token="token",
            limiter=main.TokenBucket(9999, name="test"),
            round_scan_lookback=5,
            max_retries=1,
        )

        def fake_request(
            method: str,
            url: str,
            *,
            params: Dict[str, Any] | None = None,
            json_payload: Dict[str, Any] | None = None,
            op: str,
            **ctx: Any,
        ) -> Dict[str, Any] | None:
            _ = (method, url, params, ctx)
            if op == "round_get_last":
                return {"data": {"id": 3005, "roundId": 3005, "previousRoundId": 3004, "leagueId": 25}}
            if op == "round_get_last_post_probe":
                return {"data": {"id": 3005, "roundId": 3005, "previousRoundId": 3004, "leagueId": 25}}
            if op == "round_clan_leaderboard_by_round":
                rid = main.safe_int((json_payload or {}).get("roundId"))
                if rid == 3005:
                    return {"data": {"roundId": 3005, "leagueId": 25, "active": True, "endedAt": None}}
                if rid == 3004:
                    return {
                        "data": {
                            "roundId": 3004,
                            "leagueId": 1,
                            "blockNumber": 21,
                            "multiplier": 4.0,
                            "endedAt": "2026-04-18T10:11:00Z",
                            "roundTime": 60,
                            "active": False,
                        }
                    }
            if op == "user_leaderboard_index":
                return {"data": {"gmtFund": 90.0, "totalMinedBlocks": 30}}
            if op == "clan_leaderboard_index_v2":
                return {"data": {"totalMinedBlocks": 30, "totalPower": 1000.0, "weightedEnergyEfficiencyPerTh": 10.0}}
            return None

        client._request_json_with_retry = fake_request  # type: ignore[method-assign]
        rec = client.fetch_round_metrics_triplet(league_id=1, strict_league_validation=False)
        self.assertIsInstance(rec, dict)
        self.assertEqual(rec["round_id"], 3004)

    def test_round_metrics_triplet_mismatch_scan_fail_returns_none(self) -> None:
        client = main.GoMiningRoundMetricsApiClient(
            bearer_token="token",
            limiter=main.TokenBucket(9999, name="test"),
            round_scan_lookback=3,
            max_retries=1,
        )

        def fake_request(
            method: str,
            url: str,
            *,
            params: Dict[str, Any] | None = None,
            json_payload: Dict[str, Any] | None = None,
            op: str,
            **ctx: Any,
        ) -> Dict[str, Any] | None:
            _ = (method, url, params, ctx)
            if op == "round_get_last":
                return {"data": {"id": 4005, "roundId": 4005, "previousRoundId": 4004, "leagueId": 25}}
            if op == "round_get_last_post_probe":
                return {"data": {"id": 4005, "roundId": 4005, "previousRoundId": 4004, "leagueId": 25}}
            if op == "round_clan_leaderboard_by_round":
                rid = main.safe_int((json_payload or {}).get("roundId")) or 0
                return {"data": {"roundId": rid, "leagueId": 25, "active": True, "endedAt": None}}
            return None

        client._request_json_with_retry = fake_request  # type: ignore[method-assign]
        rec = client.fetch_round_metrics_triplet(league_id=1, strict_league_validation=False)
        self.assertIsNone(rec)

    def test_round_metrics_triplet_validation_failure_active_round_returns_none(self) -> None:
        client = main.GoMiningRoundMetricsApiClient(
            bearer_token="token",
            limiter=main.TokenBucket(9999, name="test"),
            max_retries=1,
        )

        def fake_request(
            method: str,
            url: str,
            *,
            params: Dict[str, Any] | None = None,
            json_payload: Dict[str, Any] | None = None,
            op: str,
            **ctx: Any,
        ) -> Dict[str, Any] | None:
            _ = (method, url, params, json_payload, ctx)
            if op == "round_get_last":
                return {"data": {"id": 5001, "roundId": 5001, "previousRoundId": 5000, "leagueId": 1}}
            if op == "round_clan_leaderboard_by_round":
                return {
                    "data": {
                        "roundId": 5000,
                        "leagueId": 1,
                        "blockNumber": 22,
                        "multiplier": 1.0,
                        "endedAt": "2026-04-18T10:30:00Z",
                        "roundTime": 60,
                        "active": True,
                    }
                }
            return None

        client._request_json_with_retry = fake_request  # type: ignore[method-assign]
        rec = client.fetch_round_metrics_triplet(league_id=1, strict_league_validation=False)
        self.assertIsNone(rec)

    def test_round_metrics_triplet_validation_failure_missing_ended_at_returns_none(self) -> None:
        client = main.GoMiningRoundMetricsApiClient(
            bearer_token="token",
            limiter=main.TokenBucket(9999, name="test"),
            max_retries=1,
        )

        def fake_request(
            method: str,
            url: str,
            *,
            params: Dict[str, Any] | None = None,
            json_payload: Dict[str, Any] | None = None,
            op: str,
            **ctx: Any,
        ) -> Dict[str, Any] | None:
            _ = (method, url, params, json_payload, ctx)
            if op == "round_get_last":
                return {"data": {"id": 5101, "roundId": 5101, "previousRoundId": 5100, "leagueId": 1}}
            if op == "round_clan_leaderboard_by_round":
                return {
                    "data": {
                        "roundId": 5100,
                        "leagueId": 1,
                        "blockNumber": 22,
                        "multiplier": 1.0,
                        "roundTime": 60,
                        "active": False,
                    }
                }
            return None

        client._request_json_with_retry = fake_request  # type: ignore[method-assign]
        rec = client.fetch_round_metrics_triplet(league_id=1, strict_league_validation=False)
        self.assertIsNone(rec)

    def test_parse_row_range(self) -> None:
        self.assertEqual(main.parse_row_range("Sheet1!A10:R10"), (10, 10))
        self.assertEqual(main.parse_row_range("X!A4:R9"), (4, 9))
        self.assertIsNone(main.parse_row_range("invalid"))

    def test_build_canonical_row(self) -> None:
        rec = {
            "snapshot_ts": "2026-02-25T13:49:20.702613+00:00",
            "league_id": 1,
            "round_id": 123,
            "block_number": 99,
            "multiplier": 2.5,
            "gmt_fund": 10.0,
            "gmt_per_block": 0.5,
            "league_th": 1234.5,
            "ended_at": "2026-02-25T13:50:20.000000+00:00",
            "round_duration_sec": 60,
            "blocks_mined": 662,
            "efficiency_league": 20.49,
        }
        ability_headers = ["Echo Boost (x1)", "Rocket (x1)"]
        counts = {"Echo Boost (x1)": 3}
        row = main.build_canonical_row(
            rec,
            ability_headers,
            counts,
            price_cutover_round=122,
            power_up_gmt_value=555.0,
        )
        self.assertEqual(row[1], 1)
        self.assertEqual(row[6], 123)
        self.assertAlmostEqual(row[7], 1234.5)
        self.assertEqual(row[10], 662)
        self.assertAlmostEqual(row[11], 20.49)
        self.assertEqual(row[12], 3)
        self.assertEqual(row[13], 0)
        self.assertAlmostEqual(row[-6], 555.0)
        self.assertAlmostEqual(row[-5], 0.0)
        self.assertAlmostEqual(row[-4], 0.0)
        self.assertAlmostEqual(row[-3], 0.0)
        self.assertEqual(row[-2], "")
        self.assertEqual(row[-1], "")

    def test_build_canonical_row_with_missing_aliases(self) -> None:
        rec = {
            "snapshot_ts": "2026-02-25T13:49:20.702613+00:00",
            "league_id": 1,
            "round_id": 124,
            "league_th": 1000.0,
            "efficiency_league": 20.0,
        }
        row = main.build_canonical_row(
            rec,
            ["Power Up Boost"],
            {"Power Up Boost": 2},
            price_cutover_round=123,
            power_up_gmt_value=54.45,
            power_up_gmt_sentinel_value=43.56,
            power_up_missing_aliases=["alice", "bob"],
        )
        self.assertAlmostEqual(row[-6], 54.45)
        self.assertAlmostEqual(row[-5], 43.56)
        self.assertAlmostEqual(row[-4], 0.0)
        self.assertAlmostEqual(row[-3], 0.0)
        self.assertEqual(row[-2], "alice, bob")
        self.assertEqual(row[-1], "")

    def test_build_canonical_row_cutover_blocks_old_round(self) -> None:
        rec = {
            "snapshot_ts": "2026-02-25T13:49:20.702613+00:00",
            "league_id": 1,
            "round_id": 123,
            "league_th": 1000.0,
            "efficiency_league": 20.0,
        }
        row = main.build_canonical_row(rec, [], {}, price_cutover_round=123, power_up_gmt_value=999.0)
        self.assertEqual(row[-6], "")
        self.assertEqual(row[-5], "")
        self.assertEqual(row[-4], "")
        self.assertEqual(row[-3], "")
        self.assertEqual(row[-2], "")
        self.assertEqual(row[-1], "")

    def test_build_canonical_row_post_cutover_defaults_to_zero(self) -> None:
        rec = {
            "snapshot_ts": "2026-02-25T13:49:20.702613+00:00",
            "league_id": 1,
            "round_id": 124,
            "league_th": 1000.0,
            "efficiency_league": 20.0,
        }
        row = main.build_canonical_row(rec, [], {}, price_cutover_round=123, power_up_gmt_value=None)
        self.assertEqual(row[-6], 0.0)
        self.assertEqual(row[-5], 0.0)
        self.assertEqual(row[-4], 0.0)
        self.assertEqual(row[-3], 0.0)
        self.assertEqual(row[-2], "")
        self.assertEqual(row[-1], "")

    def test_build_canonical_row_post_cutover_power_up_unresolved_stays_blank(self) -> None:
        rec = {
            "snapshot_ts": "2026-02-25T13:49:20.702613+00:00",
            "league_id": 1,
            "round_id": 124,
            "league_th": 1000.0,
            "efficiency_league": 20.0,
        }
        row = main.build_canonical_row(
            rec,
            ["Power Up Boost"],
            {"Power Up Boost": 1},
            price_cutover_round=123,
            power_up_gmt_value=None,
            power_up_gmt_sentinel_value=None,
        )
        self.assertEqual(row[-6], "")
        self.assertEqual(row[-5], "")
        self.assertEqual(row[-4], 0.0)
        self.assertEqual(row[-3], 0.0)
        self.assertEqual(row[-2], "")
        self.assertEqual(row[-1], "")

    def test_formula_helpers(self) -> None:
        self.assertAlmostEqual(main.calc_league_pps(1000.0, 20.0), 1400.0)
        self.assertAlmostEqual(main.calc_power_up_gmt(1000.0, 20.0), 1400.0 * 0.0389)
        self.assertAlmostEqual(main.calc_team_pps_exact(10.0), 280.0)
        self.assertAlmostEqual(main.calc_team_pps_fallback(100.0, 20.0), 140.0)
        self.assertAlmostEqual(main.calc_clan_power_up_gmt(200.0), 0.111)
        self.assertIsNone(main.calc_power_up_gmt(1000.0, 0.0))
        self.assertIsNone(main.calc_team_pps_fallback(None, 20.0))

    def test_has_sentinel_avatar_discount(self) -> None:
        self.assertTrue(main.has_sentinel_avatar_discount("https://x/gominers_200.jpeg"))
        self.assertTrue(main.has_sentinel_avatar_discount("https://x/gominers_299.jpeg?x=1"))
        self.assertFalse(main.has_sentinel_avatar_discount("https://x/gominers_199.jpeg"))
        self.assertFalse(main.has_sentinel_avatar_discount("https://x/gominers_300.jpeg"))
        self.assertFalse(main.has_sentinel_avatar_discount(""))
        self.assertFalse(main.has_sentinel_avatar_discount(None))

    def test_has_sentinel_alias_discount(self) -> None:
        self.assertTrue(main.has_sentinel_alias_discount("\U0001F5B2 user"))
        self.assertTrue(main.has_sentinel_alias_discount("SENTINEL miner"))
        self.assertFalse(main.has_sentinel_alias_discount("regular alias"))
        self.assertFalse(main.has_sentinel_alias_discount(None))

    def test_calc_power_up_gmt_pair_from_power_up_users_api_applies_sentinel_discount(self) -> None:
        class StubClanApi:
            def fetch_user_power_ee_for_clans(self, clan_ids: List[int]) -> tuple[dict[tuple[int, int], tuple[float, float]], dict[int, tuple[float, float]]]:
                _ = clan_ids
                by_clan_user = {
                    (10, 1): (100.0, 15.0),
                    (10, 2): (1203.0, 17.0),
                }
                by_user = {1: (100.0, 15.0), 2: (1203.0, 17.0)}
                return by_clan_user, by_user

        power_up_users = [
            {"user_id": 1, "clan_id": 10, "count": 1, "avatar_url": "https://x/gominers_256.jpeg"},
            {"user_id": 2, "clan_id": 10, "count": 1, "avatar_url": "https://x/gominers_123.jpeg"},
        ]
        normal, sentinel = main.calc_power_up_gmt_pair_from_power_up_users_api(power_up_users, StubClanApi())  # type: ignore[arg-type]
        self.assertIsNotNone(normal)
        self.assertIsNotNone(sentinel)
        assert normal is not None
        assert sentinel is not None
        u1 = main.round_gmt_2(main.calc_power_up_gmt(100.0, 15.0) or 0.0)
        u2 = main.round_gmt_2(main.calc_power_up_gmt(1203.0, 17.0) or 0.0)
        self.assertAlmostEqual(normal, float(u1 + u2))
        self.assertAlmostEqual(sentinel, float(main.round_gmt_2(float(u1 * main.Decimal("0.8"))) + u2))

    def test_calc_power_up_gmt_pair_from_power_up_users_api_applies_alias_sentinel_discount(self) -> None:
        class StubClanApi:
            def fetch_user_power_ee_for_clans(self, clan_ids: List[int]) -> tuple[dict[tuple[int, int], tuple[float, float]], dict[int, tuple[float, float]]]:
                _ = clan_ids
                by_clan_user = {
                    (10, 1): (100.0, 15.0),
                    (10, 2): (1203.0, 17.0),
                }
                by_user = {1: (100.0, 15.0), 2: (1203.0, 17.0)}
                return by_clan_user, by_user

        power_up_users = [
            {"user_id": 1, "clan_id": 10, "count": 1, "avatar_url": "", "alias": "\U0001F5B2 miner"},
            {"user_id": 2, "clan_id": 10, "count": 1, "avatar_url": "", "alias": "regular"},
        ]
        normal, sentinel = main.calc_power_up_gmt_pair_from_power_up_users_api(power_up_users, StubClanApi())  # type: ignore[arg-type]
        self.assertIsNotNone(normal)
        self.assertIsNotNone(sentinel)
        assert normal is not None
        assert sentinel is not None
        u1 = main.round_gmt_2(main.calc_power_up_gmt(100.0, 15.0) or 0.0)
        u2 = main.round_gmt_2(main.calc_power_up_gmt(1203.0, 17.0) or 0.0)
        self.assertAlmostEqual(normal, float(u1 + u2))
        self.assertAlmostEqual(sentinel, float(main.round_gmt_2(float(u1 * main.Decimal("0.8"))) + u2))

    def test_calc_power_up_gmt_triplet_from_power_up_users_api_dedupes_count_and_tracks_missing_aliases(self) -> None:
        class StubClanApi:
            def fetch_user_power_ee_for_clans(
                self,
                clan_ids: List[int],
                needed_users_by_clan: Dict[int, set[int]] | None = None,
            ) -> tuple[dict[tuple[int, int], tuple[float, float]], dict[int, tuple[float, float]]]:
                _ = (clan_ids, needed_users_by_clan)
                by_clan_user = {
                    (10, 1): (300.0, 15.0),
                }
                by_user = {1: (300.0, 15.0)}
                return by_clan_user, by_user

        power_up_users = [
            {"user_id": 1, "clan_id": 10, "count": 3, "avatar_url": "https://x/gominers_123.jpeg"},
            {"user_id": 2, "clan_id": 10, "count": 1, "avatar_url": "https://x/gominers_123.jpeg", "alias": "ghost"},
        ]
        normal, sentinel, missing = main.calc_power_up_gmt_triplet_from_power_up_users_api(
            power_up_users,
            StubClanApi(),  # type: ignore[arg-type]
        )
        self.assertEqual(missing, ["ghost"])
        self.assertIsNotNone(normal)
        self.assertIsNotNone(sentinel)
        assert normal is not None
        user1 = main.round_gmt_2(main.calc_power_up_gmt(300.0, 15.0) or 0.0)
        self.assertAlmostEqual(normal, float(user1))
        self.assertAlmostEqual(sentinel, float(user1))

    def test_calc_power_up_gmt_triplet_api_only_no_db_fallback(self) -> None:
        class StubClanApi:
            def fetch_user_power_ee_for_clans(
                self,
                clan_ids: List[int],
                needed_users_by_clan: Dict[int, set[int]] | None = None,
            ) -> tuple[dict[tuple[int, int], tuple[float, float]], dict[int, tuple[float, float]]]:
                _ = (clan_ids, needed_users_by_clan)
                return {(10, 1): (100.0, 10.0)}, {1: (100.0, 10.0)}

        power_up_users = [
            {"user_id": 1, "clan_id": 10, "count": 1, "avatar_url": "https://x/gominers_123.jpeg", "alias": "alpha"},
            {"user_id": 2, "clan_id": 10, "count": 1, "avatar_url": "", "alias": "\U0001F5B2 beta"},
            {"user_id": 3, "clan_id": 10, "count": 1, "avatar_url": "", "alias": "ghost"},
        ]
        stats: Dict[str, int] = {}
        normal, sentinel, missing = main.calc_power_up_gmt_triplet_from_power_up_users_api(
            power_up_users,
            StubClanApi(),  # type: ignore[arg-type]
            resolution_stats=stats,
        )
        self.assertEqual(sorted(missing), ["ghost", "\U0001F5B2 beta"])
        self.assertEqual(stats["resolved_api"], 1)
        self.assertEqual(stats["missing"], 2)
        self.assertIsNotNone(normal)
        self.assertIsNotNone(sentinel)
        assert normal is not None
        assert sentinel is not None

        # user 1 only, counted once.
        u1 = main.round_gmt_2(main.calc_power_up_gmt(100.0, 10.0) or 0.0)
        self.assertAlmostEqual(normal, float(u1))
        self.assertAlmostEqual(sentinel, float(u1))

    def test_calc_clan_power_up_gmt_pair_from_boost_users_api_uses_clan_cpu(self) -> None:
        class StubClanApi:
            def __init__(self) -> None:
                self.seen: List[int] = []

            def fetch_clan_team_pps_for_clans(self, clan_ids: List[int]) -> Dict[int, float]:
                self.seen = sorted(set(clan_ids))
                return {10: 1000.0, 11: 500.0}

        clan_power_up_users = [
            {"user_id": 1, "clan_id": 10, "count": 2, "avatar_url": "https://x/gominers_250.jpeg"},
            {"user_id": 2, "clan_id": 11, "count": 1, "avatar_url": "https://x/gominers_123.jpeg"},
        ]
        stub = StubClanApi()
        normal, sentinel = main.calc_clan_power_up_gmt_pair_from_boost_users_api(clan_power_up_users, stub)  # type: ignore[arg-type]
        self.assertEqual(stub.seen, [10, 11])
        self.assertIsNotNone(normal)
        self.assertIsNotNone(sentinel)
        assert normal is not None
        assert sentinel is not None
        c10 = main.calc_clan_power_up_gmt(1000.0) or 0.0
        c11 = main.calc_clan_power_up_gmt(500.0) or 0.0
        c10r = float(main.round_gmt_2(c10))
        c11r = float(main.round_gmt_2(c11))
        self.assertAlmostEqual(normal, c10r + c11r)
        self.assertAlmostEqual(sentinel, float(main.round_gmt_2(c10r * 0.8)) + c11r)

    def test_calc_clan_power_up_gmt_pair_from_boost_users_api_requires_full_resolution(self) -> None:
        class StubClanApi:
            def fetch_clan_team_pps_for_clans(self, clan_ids: List[int]) -> Dict[int, float]:
                _ = clan_ids
                return {}

        clan_power_up_users = [
            {"user_id": 1, "clan_id": None, "count": 1, "avatar_url": "https://x/gominers_200.jpeg"},
        ]
        normal, sentinel = main.calc_clan_power_up_gmt_pair_from_boost_users_api(clan_power_up_users, StubClanApi())  # type: ignore[arg-type]
        self.assertIsNone(normal)
        self.assertIsNone(sentinel)

    def test_clan_api_fetch_clan_team_pps_for_clans_uses_cache(self) -> None:
        class StubClient(main.GoMiningClanApiClient):
            def __init__(self) -> None:
                super().__init__(
                    bearer_token="x",
                    limiter=main.TokenBucket(9999, name="test"),
                    page_limit=50,
                    max_retries=1,
                )
                self.calls: List[int] = []

            def _fetch_clan_detail_all_pages(self, clan_id: int) -> Dict[str, Any] | None:
                self.calls.append(clan_id)
                return {
                    "usersForClient": [
                        {"power": 100.0, "ee": 20.0},
                        {"power": 50.0, "ee": 10.0},
                    ]
                }

        client = StubClient()
        first = client.fetch_clan_team_pps_for_clans([10])
        second = client.fetch_clan_team_pps_for_clans([10, 10])
        self.assertIn(10, first)
        self.assertIn(10, second)
        self.assertAlmostEqual(first[10], 280.0)
        self.assertAlmostEqual(second[10], 280.0)
        self.assertEqual(client.calls, [10])

    def test_fetch_completed_rounds_prefer_api_uses_cache_only(self) -> None:
        fd, path = tempfile.mkstemp(prefix="sync_state_", suffix=".sqlite3")
        os.close(fd)
        try:
            st = main.StateStore(path)
            rec = {
                "snapshot_ts": "2026-04-01T22:00:00+00:00",
                "league_id": 1,
                "round_id": 101,
                "ended_at": "2026-04-01T22:00:00+00:00",
            }
            st.upsert_api_round_record(rec, source="test")
            rows = main.fetch_completed_rounds_prefer_api(None, st, league_id=1, since_round=100, limit=10)
            self.assertEqual(len(rows), 1)
            self.assertEqual(main.safe_int(rows[0].get("round_id")), 101)
            st.close()
        finally:
            try:
                os.remove(path)
            except OSError:
                pass

    def test_fetch_latest_completed_round_id_prefer_api_uses_cache_only(self) -> None:
        fd, path = tempfile.mkstemp(prefix="sync_state_", suffix=".sqlite3")
        os.close(fd)
        try:
            st = main.StateStore(path)
            st.upsert_api_round_record(
                {
                    "snapshot_ts": "2026-04-01T22:00:00+00:00",
                    "league_id": 1,
                    "round_id": 101,
                    "ended_at": "2026-04-01T22:00:00+00:00",
                },
                source="test",
            )
            st.upsert_api_round_record(
                {
                    "snapshot_ts": "2026-04-01T22:01:00+00:00",
                    "league_id": 1,
                    "round_id": 102,
                    "ended_at": "2026-04-01T22:01:00+00:00",
                },
                source="test",
            )
            latest = main.fetch_latest_completed_round_id_prefer_api(None, st, league_id=1)
            self.assertEqual(latest, 102)
            st.close()
        finally:
            try:
                os.remove(path)
            except OSError:
                pass

    def test_calc_team_th_and_pps_from_users_mixed(self) -> None:
        users = [
            {"power": 1000, "ee": 20},
            {"power": 500, "ee": None},
            {"power": 0, "ee": 15},
            {"power": "200", "ee": "10"},
        ]
        team_th, team_pps = main.calc_team_th_and_pps_from_users(users)
        self.assertAlmostEqual(team_th, 1700.0)
        self.assertAlmostEqual(team_pps, 1960.0)

    def test_to_api_calculated_at(self) -> None:
        out = main._to_api_calculated_at("2026-03-31T12:34:56+00:00")
        self.assertEqual(out, "2026-03-31T12:34:56.000Z")

    def test_extract_token_candidate_variants(self) -> None:
        self.assertEqual(main._extract_token_candidate({"token": "abc"}), "abc")
        self.assertEqual(main._extract_token_candidate({"data": {"access_token": "xyz"}}), "xyz")
        self.assertEqual(main._extract_token_candidate({"jwtToken": "tok_jwt"}), "tok_jwt")
        self.assertEqual(main._extract_token_candidate("Bearer qwe"), "qwe")
        self.assertEqual(main._extract_token_candidate([None, {"jwt": "j1.j2.j3"}]), "j1.j2.j3")
        self.assertIsNone(main._extract_token_candidate({"data": {}}))

    def test_clan_api_client_pagination_merge(self) -> None:
        class StubClient(main.GoMiningClanApiClient):
            def __init__(self) -> None:
                super().__init__(
                    bearer_token="x",
                    limiter=main.TokenBucket(9999, name="test"),
                    page_limit=1,
                    max_retries=1,
                )

            def _post_json_with_retry(self, url: str, payload: Dict[str, Any], op: str, **ctx: Any) -> Dict[str, Any] | None:
                if op == "clan_leaderboard":
                    skip = payload["pagination"]["skip"]
                    if skip == 0:
                        return {
                            "data": {
                                "count": 2,
                                "clansRemaining": [{"clanId": 11, "clan": {"name": "Alpha"}}],
                                "clansPromoted": [{"clanId": 22, "clan": {"name": "Beta"}}],
                                "clansRelegated": [],
                            }
                        }
                    if skip == 1:
                        return {
                            "data": {
                                "count": 2,
                                "clansRemaining": [{"clanId": 22, "clan": {"name": "Beta"}}],
                                "clansPromoted": [{"clanId": 22, "clan": {"name": "Beta"}}],
                                "clansRelegated": [],
                            }
                        }
                    return {"data": {"count": 2, "clansRemaining": [], "clansPromoted": [], "clansRelegated": []}}

                if op == "clan_get_by_id":
                    clan_id = payload["clanId"]
                    skip = payload["pagination"]["skip"]
                    if clan_id == 11 and skip == 0:
                        return {"data": {"name": "Alpha", "usersCount": 2, "usersForClient": [{"power": 100, "ee": 20}]}}
                    if clan_id == 11 and skip == 1:
                        return {"data": {"name": "Alpha", "usersCount": 2, "usersForClient": [{"power": 50, "ee": 10}]}}
                    if clan_id == 22 and skip == 0:
                        return {"data": {"name": "Beta", "usersCount": 1, "usersForClient": [{"power": 0, "ee": None}]}}
                return None

        client = StubClient()
        rows = client.fetch_clan_rows_for_round(
            league_id=1,
            round_id=123,
            calculated_at="2026-03-31T00:00:00.000Z",
            snapshot_ts="2026-03-31T00:00:00+00:00",
        )
        self.assertIsNotNone(rows)
        assert rows is not None
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["clan_id"], 11)
        self.assertAlmostEqual(rows[0]["team_th"], 150.0)
        self.assertAlmostEqual(rows[0]["team_pps"], 280.0)
        self.assertEqual(rows[0]["members_total"], 2)
        self.assertEqual(rows[0]["members_seen"], 2)
        self.assertEqual(rows[0]["calc_mode"], "api_exact")
        self.assertEqual(rows[0]["snapshot_round_id"], 123)

    def test_enqueue_clan_sheet_ops_blocks_on_incomplete_round(self) -> None:
        class FakeAPI:
            def __init__(self) -> None:
                self.calls: List[int] = []

            def fetch_clan_rows_for_round(
                self,
                league_id: int,
                round_id: int,
                calculated_at: str,
                snapshot_ts: str | None = None,
            ) -> List[Dict[str, Any]] | None:
                self.calls.append(round_id)
                if round_id == 11:
                    return None
                return [
                    {
                        "round_id": round_id,
                        "snapshot_round_id": round_id,
                        "snapshot_ts": snapshot_ts or "2026-03-31T12:00:00+00:00",
                        "clan_id": 100,
                        "clan_name": "X",
                        "members_total": 1,
                        "members_seen": 1,
                        "member_coverage": 1.0,
                        "team_th": 100.0,
                        "team_pps": 200.0,
                        "calc_mode": "api_exact",
                    }
                ]

        fd, path = tempfile.mkstemp(prefix="sync_state_", suffix=".sqlite3")
        os.close(fd)
        try:
            st = main.StateStore(path)
            st.upsert_sheet_meta(11, "tab-clan", 1)
            st.set_last_synced_round(11, 10)
            self._seed_api_rounds(
                st,
                [
                    {"round_id": 11, "league_id": 1, "snapshot_ts": "2026-03-31T12:00:00+00:00"},
                    {"round_id": 12, "league_id": 1, "snapshot_ts": "2026-03-31T12:02:00+00:00"},
                ],
            )
            ctx = main.SheetContext(
                ws_id=11,
                title="tab-clan",
                ws=None,
                league_id=1,
                kind="clan",
                expected_cols=len(main.CLAN_HEADERS),
                round_col_idx=2,
            )
            fake_api = FakeAPI()
            enq = main.enqueue_clan_sheet_ops(None, st, {11: ctx}, fake_api)
            self.assertEqual(enq, 0)
            self.assertEqual(fake_api.calls, [11])
            self.assertEqual(st.queue_total_count(), 0)
            st.close()
        finally:
            try:
                os.remove(path)
            except OSError:
                pass

    def test_enqueue_clan_sheet_ops_blocks_on_empty_round(self) -> None:
        class FakeAPI:
            def __init__(self) -> None:
                self.calls: List[int] = []

            def fetch_clan_rows_for_round(
                self,
                league_id: int,
                round_id: int,
                calculated_at: str,
                snapshot_ts: str | None = None,
            ) -> List[Dict[str, Any]] | None:
                self.calls.append(round_id)
                if round_id == 11:
                    return []
                return [
                    {
                        "round_id": round_id,
                        "snapshot_round_id": round_id,
                        "snapshot_ts": snapshot_ts or "2026-03-31T12:00:00+00:00",
                        "clan_id": 100,
                        "clan_name": "X",
                        "members_total": 1,
                        "members_seen": 1,
                        "member_coverage": 1.0,
                        "team_th": 100.0,
                        "team_pps": 200.0,
                        "calc_mode": "api_exact",
                    }
                ]

        fd, path = tempfile.mkstemp(prefix="sync_state_", suffix=".sqlite3")
        os.close(fd)
        try:
            st = main.StateStore(path)
            st.upsert_sheet_meta(11, "tab-clan", 1)
            st.set_last_synced_round(11, 10)
            self._seed_api_rounds(
                st,
                [
                    {"round_id": 11, "league_id": 1, "snapshot_ts": "2026-03-31T12:00:00+00:00"},
                    {"round_id": 12, "league_id": 1, "snapshot_ts": "2026-03-31T12:02:00+00:00"},
                ],
            )
            ctx = main.SheetContext(
                ws_id=11,
                title="tab-clan",
                ws=None,
                league_id=1,
                kind="clan",
                expected_cols=len(main.CLAN_HEADERS),
                round_col_idx=2,
            )
            fake_api = FakeAPI()
            enq = main.enqueue_clan_sheet_ops(None, st, {11: ctx}, fake_api)
            self.assertEqual(enq, 0)
            self.assertEqual(fake_api.calls, [11])
            self.assertEqual(st.queue_total_count(), 0)
            self.assertEqual(st.get_last_synced_round(11), 10)
            st.close()
        finally:
            try:
                os.remove(path)
            except OSError:
                pass

    def test_round_ability_api_pagination_aggregates_all_pages(self) -> None:
        class FakeResponse:
            def __init__(self, status_code: int, body: Dict[str, Any]) -> None:
                self.status_code = status_code
                self._body = body
                self.text = json.dumps(body)
                self.headers: Dict[str, str] = {}

            def json(self) -> Dict[str, Any]:
                return self._body

        class FakeSession:
            def __init__(self) -> None:
                self.calls: List[Dict[str, Any]] = []

            def post(self, url: str, headers: Dict[str, str], json: Dict[str, Any], timeout: int) -> FakeResponse:
                self.calls.append({"url": url, "payload": json, "timeout": timeout})
                skip = json["pagination"]["skip"]
                if skip == 0:
                    participants = [
                        {"usedAbilities": [{"nftGameAbilityId": "a1", "count": 1}]}
                        for _ in range(50)
                    ]
                    body = {"data": {"count": 55, "roundId": 901204, "leagueId": 1, "participants": participants}}
                    return FakeResponse(200, body)
                if skip == 50:
                    participants = [
                        {"usedAbilities": [{"nftGameAbilityId": "a2", "count": 2}]}
                        for _ in range(5)
                    ]
                    body = {"data": {"count": 55, "roundId": 901204, "leagueId": 1, "participants": participants}}
                    return FakeResponse(200, body)
                raise AssertionError(f"Unexpected skip={skip}")

        client = main.GoMiningRoundAbilityApiClient(
            bearer_token="x",
            limiter=main.TokenBucket(9999, name="test"),
            page_limit=50,
            timeout_seconds=5,
            max_retries=1,
        )
        fake_session = FakeSession()
        client.session = fake_session  # type: ignore[assignment]

        counts = client.fetch_round_ability_counts(901204, expected_league_id=1)
        self.assertIsNotNone(counts)
        assert counts is not None
        self.assertEqual(counts.get("a1"), 50)
        self.assertEqual(counts.get("a2"), 10)
        self.assertEqual(len(fake_session.calls), 2)

    def test_round_ability_api_caches_clan_power_up_users_with_clan_id(self) -> None:
        class FakeResponse:
            def __init__(self, status_code: int, body: Dict[str, Any]) -> None:
                self.status_code = status_code
                self._body = body
                self.text = json.dumps(body)
                self.headers: Dict[str, str] = {}

            def json(self) -> Dict[str, Any]:
                return self._body

        class FakeSession:
            def post(self, url: str, headers: Dict[str, str], json: Dict[str, Any], timeout: int) -> FakeResponse:
                _ = (url, headers, timeout)
                participants = [
                    {
                        "user": {"id": 123, "avatar": "https://x/gominers_250.jpeg", "alias": "\U0001F5B2 user"},
                        "clan": {"id": 987},
                        "usedAbilities": [
                            {"nftGameAbilityId": main.CLAN_POWER_UP_ABILITY_ID, "count": 2},
                            {"nftGameAbilityId": main.POWER_UP_ABILITY_ID, "count": 1},
                        ],
                    }
                ]
                body = {"data": {"count": 1, "roundId": 901204, "leagueId": 1, "participants": participants}}
                return FakeResponse(200, body)

        client = main.GoMiningRoundAbilityApiClient(
            bearer_token="x",
            limiter=main.TokenBucket(9999, name="test"),
            page_limit=50,
            timeout_seconds=5,
            max_retries=1,
        )
        client.session = FakeSession()  # type: ignore[assignment]

        counts = client.fetch_round_ability_counts(901204, expected_league_id=1)
        self.assertIsNotNone(counts)
        assert counts is not None
        self.assertEqual(counts.get(main.CLAN_POWER_UP_ABILITY_ID), 2)
        self.assertEqual(counts.get(main.POWER_UP_ABILITY_ID), 1)

        clan_power_up_users = client.get_cached_ability_users_for_round(901204, main.CLAN_POWER_UP_ABILITY_ID)
        self.assertEqual(len(clan_power_up_users), 1)
        self.assertEqual(clan_power_up_users[0]["user_id"], 123)
        self.assertEqual(clan_power_up_users[0]["clan_id"], 987)
        self.assertEqual(clan_power_up_users[0]["count"], 2)
        self.assertEqual(clan_power_up_users[0]["alias"], "\U0001F5B2 user")

        power_up_users = client.get_cached_power_up_users_for_round(901204)
        self.assertEqual(len(power_up_users), 1)
        self.assertEqual(power_up_users[0]["count"], 1)
        self.assertEqual(power_up_users[0]["alias"], "\U0001F5B2 user")

    def test_round_ability_api_excludes_configured_user_and_tracks_audit(self) -> None:
        class FakeResponse:
            def __init__(self, status_code: int, body: Dict[str, Any]) -> None:
                self.status_code = status_code
                self._body = body
                self.text = json.dumps(body)
                self.headers: Dict[str, str] = {}

            def json(self) -> Dict[str, Any]:
                return self._body

        class FakeSession:
            def post(self, url: str, headers: Dict[str, str], json: Dict[str, Any], timeout: int) -> FakeResponse:
                _ = (url, headers, timeout)
                participants = [
                    {
                        "user": {"id": main.EXCLUDED_BOOST_USER_ID, "avatar": "", "alias": "excluded"},
                        "clan": {"id": 555},
                        "usedAbilities": [
                            {"nftGameAbilityId": main.POWER_UP_ABILITY_ID, "count": 3},
                            {"nftGameAbilityId": "unknown-excluded-aid", "count": 2},
                        ],
                    },
                    {
                        "user": {"id": 123, "avatar": "", "alias": "included"},
                        "clan": {"id": 777},
                        "usedAbilities": [
                            {"nftGameAbilityId": main.POWER_UP_ABILITY_ID, "count": 1},
                            {"nftGameAbilityId": main.CLAN_POWER_UP_ABILITY_ID, "count": 4},
                        ],
                    },
                ]
                body = {"data": {"count": 2, "roundId": 901204, "leagueId": 1, "participants": participants}}
                return FakeResponse(200, body)

        client = main.GoMiningRoundAbilityApiClient(
            bearer_token="x",
            limiter=main.TokenBucket(9999, name="test"),
            page_limit=50,
            timeout_seconds=5,
            max_retries=1,
        )
        client.session = FakeSession()  # type: ignore[assignment]

        counts = client.fetch_round_ability_counts(901204, expected_league_id=1)
        self.assertIsNotNone(counts)
        assert counts is not None
        self.assertEqual(counts.get(main.POWER_UP_ABILITY_ID), 1)
        self.assertEqual(counts.get(main.CLAN_POWER_UP_ABILITY_ID), 4)
        self.assertNotIn("unknown-excluded-aid", counts)

        power_up_users = client.get_cached_ability_users_for_round(901204, main.POWER_UP_ABILITY_ID)
        self.assertEqual(len(power_up_users), 1)
        self.assertEqual(power_up_users[0]["user_id"], 123)

        excluded_audit = client.get_cached_excluded_user_boosts_for_round(901204)
        self.assertEqual(excluded_audit.get(main.POWER_UP_ABILITY_ID), 3)
        self.assertEqual(excluded_audit.get("unknown-excluded-aid"), 2)

    def test_round_ability_api_refreshes_on_403_jwt_expired(self) -> None:
        class FakeResponse:
            def __init__(self, status_code: int, body: Dict[str, Any]) -> None:
                self.status_code = status_code
                self._body = body
                self.text = json.dumps(body)
                self.headers: Dict[str, str] = {}

            def json(self) -> Dict[str, Any]:
                return self._body

        class FakeSession:
            def __init__(self) -> None:
                self.calls: List[Dict[str, Any]] = []

            def post(self, url: str, headers: Dict[str, str], json: Dict[str, Any], timeout: int) -> FakeResponse:
                self.calls.append({"url": url, "payload": json, "timeout": timeout, "auth": headers.get("authorization")})
                if len(self.calls) == 1:
                    return FakeResponse(
                        403,
                        {
                            "statusCode": 403,
                            "description": "JWT token is expired",
                            "statusMessage": "JWT_TOKEN_EXPIRED",
                        },
                    )
                return FakeResponse(200, {"data": {"count": 0, "roundId": 901204, "leagueId": 1, "participants": []}})

        refresh_calls: List[int] = []

        def token_fetcher() -> str:
            refresh_calls.append(1)
            return "new-token"

        client = main.GoMiningRoundAbilityApiClient(
            bearer_token="old-token",
            limiter=main.TokenBucket(9999, name="test"),
            page_limit=50,
            timeout_seconds=5,
            max_retries=2,
            token_fetcher=token_fetcher,
        )
        fake_session = FakeSession()
        client.session = fake_session  # type: ignore[assignment]

        counts = client.fetch_round_ability_counts(901204, expected_league_id=1)
        self.assertEqual(counts, {})
        self.assertEqual(len(refresh_calls), 1)
        self.assertEqual(client.headers.get("authorization"), "Bearer new-token")
        self.assertEqual(len(fake_session.calls), 2)
        self.assertEqual(fake_session.calls[0].get("auth"), "Bearer old-token")
        self.assertEqual(fake_session.calls[1].get("auth"), "Bearer new-token")

    def test_clan_api_refreshes_on_403_jwt_expired(self) -> None:
        class FakeResponse:
            def __init__(self, status_code: int, body: Dict[str, Any]) -> None:
                self.status_code = status_code
                self._body = body
                self.text = json.dumps(body)
                self.headers: Dict[str, str] = {}

            def json(self) -> Dict[str, Any]:
                return self._body

        class FakeSession:
            def __init__(self) -> None:
                self.calls: List[Dict[str, Any]] = []

            def post(self, url: str, headers: Dict[str, str], json: Dict[str, Any], timeout: int) -> FakeResponse:
                self.calls.append({"url": url, "payload": json, "timeout": timeout, "auth": headers.get("authorization")})
                if len(self.calls) == 1:
                    return FakeResponse(
                        403,
                        {
                            "statusCode": 403,
                            "description": "JWT token is expired",
                            "statusMessage": "JWT_TOKEN_EXPIRED",
                        },
                    )
                return FakeResponse(200, {"data": {"ok": True}})

        refresh_calls: List[int] = []

        def token_fetcher() -> str:
            refresh_calls.append(1)
            return "new-token"

        client = main.GoMiningClanApiClient(
            bearer_token="old-token",
            limiter=main.TokenBucket(9999, name="test"),
            page_limit=50,
            timeout_seconds=5,
            max_retries=2,
            token_fetcher=token_fetcher,
        )
        fake_session = FakeSession()
        client.session = fake_session  # type: ignore[assignment]

        body = client._post_json_with_retry("https://example.test", {"a": 1}, op="clan_test")
        self.assertEqual(body, {"data": {"ok": True}})
        self.assertEqual(len(refresh_calls), 1)
        self.assertEqual(client.headers.get("authorization"), "Bearer new-token")
        self.assertEqual(len(fake_session.calls), 2)
        self.assertEqual(fake_session.calls[0].get("auth"), "Bearer old-token")
        self.assertEqual(fake_session.calls[1].get("auth"), "Bearer new-token")

    def test_enqueue_main_sheet_ops_blocks_on_round_api_failure(self) -> None:
        class FakeRoundAPI:
            def __init__(self) -> None:
                self.calls: List[int] = []

            def fetch_round_ability_counts(
                self,
                round_id: int,
                expected_league_id: int | None = None,
                progress_hook: Any | None = None,
            ) -> Dict[str, int] | None:
                self.calls.append(round_id)
                if round_id == 101:
                    return None
                return {"a1": 1}

        fd, path = tempfile.mkstemp(prefix="sync_state_", suffix=".sqlite3")
        os.close(fd)
        orig_hybrid = main.ABILITY_FETCH_HYBRID_CONTINUE_ON_ERROR
        try:
            main.ABILITY_FETCH_HYBRID_CONTINUE_ON_ERROR = False
            st = main.StateStore(path)
            st.upsert_sheet_meta(11, "tab-main", 1)
            st.set_last_synced_round(11, 100)
            st.set_price_cutover_round(11, 100)
            self._seed_api_rounds(
                st,
                [
                    {"round_id": 101, "league_id": 1, "snapshot_ts": "2026-04-01T22:00:00+00:00"},
                    {"round_id": 102, "league_id": 1, "snapshot_ts": "2026-04-01T22:02:00+00:00"},
                ],
            )
            ctx = main.SheetContext(
                ws_id=11,
                title="tab-main",
                ws=None,
                league_id=1,
                kind="main",
                expected_cols=len(main.BASE_HEADERS) + 1 + 6,
                round_col_idx=6,
            )
            fake_round_api = FakeRoundAPI()
            enq = main.enqueue_main_sheet_ops(
                None,
                st,
                {11: ctx},
                {"a1": "Power Up Boost"},
                ["Power Up Boost"],
                fake_round_api,  # type: ignore[arg-type]
            )
            self.assertEqual(enq, 0)
            self.assertEqual(st.queue_total_count(), 0)
            self.assertEqual(fake_round_api.calls, [101])
            st.close()
        finally:
            main.ABILITY_FETCH_HYBRID_CONTINUE_ON_ERROR = orig_hybrid
            try:
                os.remove(path)
            except OSError:
                pass

    def test_enqueue_main_sheet_ops_hybrid_continues_after_round_api_failure(self) -> None:
        class FakeRoundAPI:
            def __init__(self) -> None:
                self.calls: List[int] = []

            def fetch_round_ability_counts(
                self,
                round_id: int,
                expected_league_id: int | None = None,
                progress_hook: Any | None = None,
            ) -> Dict[str, int] | None:
                _ = (expected_league_id, progress_hook)
                self.calls.append(round_id)
                if round_id == 101:
                    return None
                return {"a1": 1}

        fd, path = tempfile.mkstemp(prefix="sync_state_", suffix=".sqlite3")
        os.close(fd)
        orig_hybrid = main.ABILITY_FETCH_HYBRID_CONTINUE_ON_ERROR
        try:
            main.ABILITY_FETCH_HYBRID_CONTINUE_ON_ERROR = True
            st = main.StateStore(path)
            st.upsert_sheet_meta(11, "tab-main", 1)
            st.set_last_synced_round(11, 100)
            st.set_price_cutover_round(11, 100)
            self._seed_api_rounds(
                st,
                [
                    {"round_id": 101, "league_id": 1, "snapshot_ts": "2026-04-01T22:00:00+00:00"},
                    {"round_id": 102, "league_id": 1, "snapshot_ts": "2026-04-01T22:02:00+00:00"},
                ],
            )
            ctx = main.SheetContext(
                ws_id=11,
                title="tab-main",
                ws=None,
                league_id=1,
                kind="main",
                expected_cols=len(main.BASE_HEADERS) + 1 + 6,
                round_col_idx=6,
            )
            fake_round_api = FakeRoundAPI()
            enq = main.enqueue_main_sheet_ops(
                None,
                st,
                {11: ctx},
                {"a1": "Power Up Boost"},
                ["Power Up Boost"],
                fake_round_api,  # type: ignore[arg-type]
            )
            self.assertEqual(enq, 1)
            self.assertEqual(fake_round_api.calls, [101, 102])
            due = st.fetch_due_ops(limit=10)
            self.assertEqual(len(due), 1)
            self.assertEqual(due[0]["round_id"], 102)
            st.close()
        finally:
            main.ABILITY_FETCH_HYBRID_CONTINUE_ON_ERROR = orig_hybrid
            try:
                os.remove(path)
            except OSError:
                pass

    def test_state_advance_last_synced_round_contiguous(self) -> None:
        fd, path = tempfile.mkstemp(prefix="sync_state_", suffix=".sqlite3")
        os.close(fd)
        try:
            st = main.StateStore(path)
            st.upsert_sheet_meta(11, "tab-main", 1)
            st.set_last_synced_round(11, 100)
            st.upsert_row_map(11, 102, 10, "x102", 1)
            st.upsert_row_map(11, 103, 11, "x103", 1)
            advanced = st.advance_last_synced_round_contiguous(11)
            self.assertEqual(advanced, 100)
            self.assertEqual(st.get_last_synced_round(11), 100)
            st.upsert_row_map(11, 101, 9, "x101", 1)
            advanced = st.advance_last_synced_round_contiguous(11)
            self.assertEqual(advanced, 103)
            self.assertEqual(st.get_last_synced_round(11), 103)
            st.close()
        finally:
            try:
                os.remove(path)
            except OSError:
                pass

    def test_flush_queue_advances_last_synced_only_when_contiguous(self) -> None:
        class FakeWorksheet:
            def __init__(self) -> None:
                self.row_count = 100
                self.writes: List[Dict[str, Any]] = []

            def update(self, range_name: str, values: List[List[Any]], value_input_option: str = "RAW") -> None:
                self.writes.append({"range_name": range_name, "values": values, "value_input_option": value_input_option})

            def add_rows(self, n: int) -> None:
                self.row_count += int(n)

        fd, path = tempfile.mkstemp(prefix="sync_state_", suffix=".sqlite3")
        os.close(fd)
        try:
            st = main.StateStore(path)
            st.upsert_sheet_meta(11, "tab-main", 1)
            st.set_last_synced_round(11, 100)
            ws = FakeWorksheet()
            ctx = main.SheetContext(
                ws_id=11,
                title="tab-main",
                ws=ws,
                league_id=1,
                kind="main",
                expected_cols=len(main.BASE_HEADERS) + 1 + 6,
                round_col_idx=6,
            )
            row_102 = [""] * ctx.expected_cols
            checksum_102 = main.row_checksum(row_102)
            st.enqueue_op(
                11,
                "update_round",
                102,
                checksum_102,
                {"row_idx": 6, "row": row_102, "finalized": 1},
            )
            processed = main.flush_sheet_queue_with_rate_limit(
                st,
                {11: ctx},
                main.TokenBucket(9999, name="test_write"),
            )
            self.assertEqual(processed, 1)
            self.assertEqual(st.get_last_synced_round(11), 100)

            row_101 = [""] * ctx.expected_cols
            checksum_101 = main.row_checksum(row_101)
            st.enqueue_op(
                11,
                "update_round",
                101,
                checksum_101,
                {"row_idx": 5, "row": row_101, "finalized": 1},
            )
            processed = main.flush_sheet_queue_with_rate_limit(
                st,
                {11: ctx},
                main.TokenBucket(9999, name="test_write"),
            )
            self.assertEqual(processed, 1)
            self.assertEqual(st.get_last_synced_round(11), 102)
            st.close()
        finally:
            try:
                os.remove(path)
            except OSError:
                pass

    def test_round_ability_api_503_retry_uses_extended_budget_and_adaptive_throttle(self) -> None:
        class FakeResponse:
            def __init__(self, status_code: int, body: Dict[str, Any]) -> None:
                self.status_code = status_code
                self._body = body
                self.text = json.dumps(body)
                self.headers: Dict[str, str] = {}

            def json(self) -> Dict[str, Any]:
                return self._body

        class FakeSession:
            def __init__(self) -> None:
                self.calls: List[int] = []

            def post(self, url: str, headers: Dict[str, str], json: Dict[str, Any], timeout: int) -> FakeResponse:
                _ = (url, headers, json, timeout)
                self.calls.append(1)
                if len(self.calls) < 3:
                    return FakeResponse(503, {"statusCode": 503, "message": "Service unavailable"})
                return FakeResponse(200, {"data": {"ok": True}})

        orig_503_retries = main.ROUND_USER_503_MAX_RETRIES
        orig_backoff_max = main.ROUND_USER_BACKOFF_MAX_SECONDS
        orig_adaptive_min = main.ROUND_USER_ADAPTIVE_MIN_RPM
        orig_sleep = main.time.sleep
        orig_uniform = main.random.uniform
        orig_log_warn = main.log_warn
        orig_log_info = main.log_info
        warn_events: List[str] = []
        info_events: List[str] = []
        sleep_calls: List[float] = []
        try:
            main.ROUND_USER_503_MAX_RETRIES = 4
            main.ROUND_USER_BACKOFF_MAX_SECONDS = 1.5
            main.ROUND_USER_ADAPTIVE_MIN_RPM = 30.0
            main.time.sleep = lambda x: sleep_calls.append(float(x))  # type: ignore[assignment]
            main.random.uniform = lambda _a, _b: 0.0  # type: ignore[assignment]

            def fake_log_warn(msg: str, **fields: Any) -> None:
                _ = fields
                warn_events.append(msg)

            def fake_log_info(msg: str, **fields: Any) -> None:
                _ = fields
                info_events.append(msg)

            main.log_warn = fake_log_warn  # type: ignore[assignment]
            main.log_info = fake_log_info  # type: ignore[assignment]

            client = main.GoMiningRoundAbilityApiClient(
                bearer_token="token",
                limiter=main.TokenBucket(9999, name="test"),
                max_retries=1,
            )
            fake_session = FakeSession()
            client.session = fake_session  # type: ignore[assignment]
            body = client._post_json_with_retry({"roundId": 901204}, round_id=901204, skip=0)
            self.assertEqual(body, {"data": {"ok": True}})
            self.assertEqual(len(fake_session.calls), 3)
            self.assertIn("gomining_api.round_user_adaptive_throttle_enter", warn_events)
            self.assertIn("gomining_api.round_user_adaptive_throttle_exit", info_events)
            self.assertGreaterEqual(len(sleep_calls), 2)
        finally:
            main.ROUND_USER_503_MAX_RETRIES = orig_503_retries
            main.ROUND_USER_BACKOFF_MAX_SECONDS = orig_backoff_max
            main.ROUND_USER_ADAPTIVE_MIN_RPM = orig_adaptive_min
            main.time.sleep = orig_sleep  # type: ignore[assignment]
            main.random.uniform = orig_uniform  # type: ignore[assignment]
            main.log_warn = orig_log_warn  # type: ignore[assignment]
            main.log_info = orig_log_info  # type: ignore[assignment]

    def test_enqueue_main_sheet_ops_updates_when_abilities_arrive(self) -> None:
        rec = {
            "snapshot_ts": "2026-04-01T22:00:00+00:00",
            "league_id": 1,
            "round_id": 103,
            "block_number": 5000,
            "multiplier": 1.0,
            "gmt_fund": 10.0,
            "gmt_per_block": 0.1,
            "league_th": 1000.0,
            "ended_at": "2026-04-01T22:00:00+00:00",
            "round_duration_sec": 60,
            "blocks_mined": 100,
            "efficiency_league": 20.0,
        }

        class FakeRoundAPI:
            def fetch_round_ability_counts(
                self,
                round_id: int,
                expected_league_id: int | None = None,
                progress_hook: Any | None = None,
            ) -> Dict[str, int] | None:
                if round_id == 103:
                    return {"a1": 5}
                return {}

        fd, path = tempfile.mkstemp(prefix="sync_state_", suffix=".sqlite3")
        os.close(fd)
        try:
            st = main.StateStore(path)
            st.upsert_sheet_meta(11, "tab-main", 1)
            st.set_last_synced_round(11, 104)
            st.set_price_cutover_round(11, 100)
            self._seed_api_rounds(st, [rec])
            old_row = main.build_canonical_row(rec, ["Power Up Boost"], {}, price_cutover_round=100, power_up_gmt_value=None)
            st.upsert_row_map(11, 103, 77, main.row_checksum(old_row), 1)
            ctx = main.SheetContext(
                ws_id=11,
                title="tab-main",
                ws=None,
                league_id=1,
                kind="main",
                expected_cols=len(main.BASE_HEADERS) + 1 + 6,
                round_col_idx=6,
            )
            enq = main.enqueue_main_sheet_ops(
                None,
                st,
                {11: ctx},
                {"a1": "Power Up Boost"},
                ["Power Up Boost"],
                FakeRoundAPI(),  # type: ignore[arg-type]
            )
            self.assertEqual(enq, 1)
            due = st.fetch_due_ops(limit=10)
            self.assertEqual(len(due), 1)
            self.assertEqual(due[0]["op_type"], "update_round")
            self.assertEqual(due[0]["round_id"], 103)
            payload = json.loads(due[0]["payload_json"])
            row = payload["row"]
            self.assertEqual(row[12], 5)
            self.assertAlmostEqual(row[-6], 5.0 * main.calc_power_up_gmt(1000.0, 20.0))
            self.assertAlmostEqual(row[-5], 5.0 * main.calc_power_up_gmt(1000.0, 20.0))
            self.assertAlmostEqual(row[-4], 0.0)
            self.assertAlmostEqual(row[-3], 0.0)
            self.assertEqual(row[-2], "")
            self.assertEqual(row[-1], "")
            st.close()
        finally:
            try:
                os.remove(path)
            except OSError:
                pass

    def test_enqueue_main_sheet_ops_adds_excluded_user_audit_column(self) -> None:
        rec = {
            "snapshot_ts": "2026-04-01T22:00:00+00:00",
            "league_id": 1,
            "round_id": 103,
            "block_number": 5000,
            "multiplier": 1.0,
            "gmt_fund": 10.0,
            "gmt_per_block": 0.1,
            "league_th": 1000.0,
            "ended_at": "2026-04-01T22:00:00+00:00",
            "round_duration_sec": 60,
            "blocks_mined": 100,
            "efficiency_league": 20.0,
        }

        class FakeRoundAPI:
            def fetch_round_ability_counts(
                self,
                round_id: int,
                expected_league_id: int | None = None,
                progress_hook: Any | None = None,
            ) -> Dict[str, int] | None:
                _ = (round_id, expected_league_id, progress_hook)
                return {main.POWER_UP_ABILITY_ID: 4}

            def get_cached_ability_users_for_round(self, round_id: int, ability_id: str) -> List[Dict[str, Any]]:
                _ = (round_id, ability_id)
                return []

            def get_cached_excluded_user_boosts_for_round(self, round_id: int) -> Dict[str, int]:
                _ = round_id
                return {main.POWER_UP_ABILITY_ID: 3, "unknown-aid": 2}

        fd, path = tempfile.mkstemp(prefix="sync_state_", suffix=".sqlite3")
        os.close(fd)
        try:
            st = main.StateStore(path)
            st.upsert_sheet_meta(11, "tab-main", 1)
            st.set_last_synced_round(11, 100)
            st.set_price_cutover_round(11, 100)
            self._seed_api_rounds(st, [rec])
            ctx = main.SheetContext(
                ws_id=11,
                title="tab-main",
                ws=None,
                league_id=1,
                kind="main",
                expected_cols=len(main.BASE_HEADERS) + 1 + 6,
                round_col_idx=6,
            )
            enq = main.enqueue_main_sheet_ops(
                None,
                st,
                {11: ctx},
                {main.POWER_UP_ABILITY_ID: "Power Up Boost"},
                ["Power Up Boost"],
                FakeRoundAPI(),  # type: ignore[arg-type]
            )
            self.assertEqual(enq, 1)
            due = st.fetch_due_ops(limit=10)
            self.assertEqual(len(due), 1)
            payload = json.loads(due[0]["payload_json"])
            row = payload["row"]
            self.assertEqual(row[12], 4)
            self.assertEqual(row[-1], "Power Up Boost=3; ability_id=unknown-aid:2")
            st.close()
        finally:
            try:
                os.remove(path)
            except OSError:
                pass

    def test_enqueue_main_sheet_ops_clan_power_up_uses_clan_cpu_formula(self) -> None:
        rec = {
            "snapshot_ts": "2026-04-01T22:00:00+00:00",
            "league_id": 1,
            "round_id": 103,
            "block_number": 5000,
            "multiplier": 1.0,
            "gmt_fund": 10.0,
            "gmt_per_block": 0.1,
            "league_th": 1000.0,
            "ended_at": "2026-04-01T22:00:00+00:00",
            "round_duration_sec": 60,
            "blocks_mined": 100,
            "efficiency_league": 20.0,
        }

        class FakeRoundAPI:
            def fetch_round_ability_counts(
                self,
                round_id: int,
                expected_league_id: int | None = None,
                progress_hook: Any | None = None,
            ) -> Dict[str, int] | None:
                _ = (round_id, expected_league_id, progress_hook)
                return {main.CLAN_POWER_UP_ABILITY_ID: 2}

            def get_cached_ability_users_for_round(self, round_id: int, ability_id: str) -> List[Dict[str, Any]]:
                _ = (round_id, ability_id)
                return [
                    {"user_id": 1, "clan_id": 777, "count": 2, "avatar_url": "https://x/gominers_250.jpeg"},
                ]

        class FakeClanAPI:
            def fetch_clan_team_pps_for_clans(self, clan_ids: List[int]) -> Dict[int, float]:
                _ = clan_ids
                return {777: 2000.0}

        fd, path = tempfile.mkstemp(prefix="sync_state_", suffix=".sqlite3")
        os.close(fd)
        try:
            st = main.StateStore(path)
            st.upsert_sheet_meta(11, "tab-main", 1)
            st.set_last_synced_round(11, 104)
            st.set_price_cutover_round(11, 100)
            self._seed_api_rounds(st, [rec])
            old_row = main.build_canonical_row(rec, ["Clan Power Up Boost"], {}, price_cutover_round=100, power_up_gmt_value=None)
            st.upsert_row_map(11, 103, 77, main.row_checksum(old_row), 1)
            ctx = main.SheetContext(
                ws_id=11,
                title="tab-main",
                ws=None,
                league_id=1,
                kind="main",
                expected_cols=len(main.BASE_HEADERS) + 1 + 6,
                round_col_idx=6,
            )
            enq = main.enqueue_main_sheet_ops(
                None,
                st,
                {11: ctx},
                {main.CLAN_POWER_UP_ABILITY_ID: "Clan Power Up Boost"},
                ["Clan Power Up Boost"],
                FakeRoundAPI(),  # type: ignore[arg-type]
                power_up_clan_api=FakeClanAPI(),  # type: ignore[arg-type]
            )
            self.assertEqual(enq, 1)
            due = st.fetch_due_ops(limit=10)
            self.assertEqual(len(due), 1)
            payload = json.loads(due[0]["payload_json"])
            row = payload["row"]
            clan_cpu = main.calc_clan_power_up_gmt(2000.0) or 0.0
            self.assertEqual(row[12], 2)
            self.assertAlmostEqual(row[-6], 0.0)
            self.assertAlmostEqual(row[-5], 0.0)
            self.assertAlmostEqual(row[-4], clan_cpu)
            self.assertAlmostEqual(row[-3], float(main.round_gmt_2(clan_cpu * 0.8)))
            self.assertEqual(row[-2], "")
            self.assertEqual(row[-1], "")
            st.close()
        finally:
            try:
                os.remove(path)
            except OSError:
                pass

    def test_enqueue_main_sheet_ops_ignores_unknown_ability_ids(self) -> None:
        rec = {
            "snapshot_ts": "2026-04-01T22:58:44+00:00",
            "league_id": 1,
            "round_id": 101,
            "block_number": 5001,
            "multiplier": 1.0,
            "gmt_fund": 10.0,
            "gmt_per_block": 0.1,
            "league_th": 1000.0,
            "ended_at": "2026-04-01T22:58:44+00:00",
            "round_duration_sec": 60,
            "blocks_mined": 100,
            "efficiency_league": 20.0,
        }

        class FakeRoundAPI:
            def fetch_round_ability_counts(
                self,
                round_id: int,
                expected_league_id: int | None = None,
                progress_hook: Any | None = None,
            ) -> Dict[str, int] | None:
                if round_id == 101:
                    return {"a1": 2, "unknown_id": 999}
                return {}

        fd, path = tempfile.mkstemp(prefix="sync_state_", suffix=".sqlite3")
        os.close(fd)
        try:
            st = main.StateStore(path)
            st.upsert_sheet_meta(11, "tab-main", 1)
            st.set_last_synced_round(11, 100)
            st.set_price_cutover_round(11, 100)
            self._seed_api_rounds(st, [rec])
            ctx = main.SheetContext(
                ws_id=11,
                title="tab-main",
                ws=None,
                league_id=1,
                kind="main",
                expected_cols=len(main.BASE_HEADERS) + 1 + 6,
                round_col_idx=6,
            )
            enq = main.enqueue_main_sheet_ops(
                None,
                st,
                {11: ctx},
                {"a1": "Power Up Boost"},
                ["Power Up Boost"],
                FakeRoundAPI(),  # type: ignore[arg-type]
            )
            self.assertEqual(enq, 1)
            due = st.fetch_due_ops(limit=10)
            self.assertEqual(len(due), 1)
            self.assertEqual(due[0]["op_type"], "append_round")
            self.assertEqual(due[0]["round_id"], 101)
            payload = json.loads(due[0]["payload_json"])
            row = payload["row"]
            self.assertEqual(row[12], 2)  # Power Up Boost column
            st.close()
        finally:
            try:
                os.remove(path)
            except OSError:
                pass

    def test_enqueue_main_sheet_ops_invokes_flush_tick_during_scan(self) -> None:
        rec1 = {
            "snapshot_ts": "2026-04-01T22:00:00+00:00",
            "league_id": 1,
            "round_id": 101,
            "block_number": 5000,
            "multiplier": 1.0,
            "gmt_fund": 10.0,
            "gmt_per_block": 0.1,
            "league_th": 1000.0,
            "ended_at": "2026-04-01T22:00:00+00:00",
            "round_duration_sec": 60,
            "blocks_mined": 100,
            "efficiency_league": 20.0,
        }
        rec2 = {
            "snapshot_ts": "2026-04-01T22:02:00+00:00",
            "league_id": 1,
            "round_id": 102,
            "block_number": 5001,
            "multiplier": 1.0,
            "gmt_fund": 10.0,
            "gmt_per_block": 0.1,
            "league_th": 1000.0,
            "ended_at": "2026-04-01T22:02:00+00:00",
            "round_duration_sec": 60,
            "blocks_mined": 101,
            "efficiency_league": 20.0,
        }

        class FakeRoundAPI:
            def fetch_round_ability_counts(
                self,
                round_id: int,
                expected_league_id: int | None = None,
                progress_hook: Any | None = None,
            ) -> Dict[str, int] | None:
                if progress_hook is not None:
                    progress_hook()
                return {"a1": 1}

        flush_calls: List[int] = []

        def flush_tick() -> int:
            flush_calls.append(1)
            return 0

        fd, path = tempfile.mkstemp(prefix="sync_state_", suffix=".sqlite3")
        os.close(fd)
        try:
            st = main.StateStore(path)
            st.upsert_sheet_meta(11, "tab-main", 1)
            st.set_last_synced_round(11, 100)
            st.set_price_cutover_round(11, 100)
            self._seed_api_rounds(st, [rec1, rec2])
            ctx = main.SheetContext(
                ws_id=11,
                title="tab-main",
                ws=None,
                league_id=1,
                kind="main",
                expected_cols=len(main.BASE_HEADERS) + 1 + 6,
                round_col_idx=6,
            )
            enq = main.enqueue_main_sheet_ops(
                None,
                st,
                {11: ctx},
                {"a1": "Power Up Boost"},
                ["Power Up Boost"],
                FakeRoundAPI(),  # type: ignore[arg-type]
                flush_tick=flush_tick,
                flush_every_seconds=0.0,
            )
            self.assertEqual(enq, 2)
            self.assertGreaterEqual(len(flush_calls), 2)
            st.close()
        finally:
            try:
                os.remove(path)
            except OSError:
                pass

    def test_enqueue_main_sheet_ops_api_first_for_all_rounds(self) -> None:
        rounds: List[Dict[str, Any]] = [
            {
                "snapshot_ts": "2026-04-01T22:00:00+00:00",
                "league_id": 1,
                "round_id": 101,
                "block_number": 5000,
                "multiplier": 1.0,
                "gmt_fund": 10.0,
                "gmt_per_block": 0.1,
                "league_th": 1000.0,
                "ended_at": "2026-04-01T22:00:00+00:00",
                "round_duration_sec": 60,
                "blocks_mined": 100,
                "efficiency_league": 20.0,
            },
            {
                "snapshot_ts": "2026-04-01T22:01:00+00:00",
                "league_id": 1,
                "round_id": 102,
                "block_number": 5001,
                "multiplier": 1.0,
                "gmt_fund": 10.0,
                "gmt_per_block": 0.1,
                "league_th": 1000.0,
                "ended_at": "2026-04-01T22:01:00+00:00",
                "round_duration_sec": 60,
                "blocks_mined": 101,
                "efficiency_league": 20.0,
            },
            {
                "snapshot_ts": "2026-04-01T22:02:00+00:00",
                "league_id": 1,
                "round_id": 103,
                "block_number": 5002,
                "multiplier": 1.0,
                "gmt_fund": 10.0,
                "gmt_per_block": 0.1,
                "league_th": 1000.0,
                "ended_at": "2026-04-01T22:02:00+00:00",
                "round_duration_sec": 60,
                "blocks_mined": 102,
                "efficiency_league": 20.0,
            },
            {
                "snapshot_ts": "2026-04-01T22:03:00+00:00",
                "league_id": 1,
                "round_id": 104,
                "block_number": 5003,
                "multiplier": 1.0,
                "gmt_fund": 10.0,
                "gmt_per_block": 0.1,
                "league_th": 1000.0,
                "ended_at": "2026-04-01T22:03:00+00:00",
                "round_duration_sec": 60,
                "blocks_mined": 103,
                "efficiency_league": 20.0,
            },
        ]

        class FakeRoundAPI:
            def __init__(self) -> None:
                self.calls: List[int] = []

            def fetch_round_ability_counts(
                self,
                round_id: int,
                expected_league_id: int | None = None,
                progress_hook: Any | None = None,
            ) -> Dict[str, int] | None:
                self.calls.append(round_id)
                return {"a1": 1}

        fd, path = tempfile.mkstemp(prefix="sync_state_", suffix=".sqlite3")
        os.close(fd)
        try:
            st = main.StateStore(path)
            st.upsert_sheet_meta(11, "tab-main", 1)
            st.set_last_synced_round(11, 100)
            st.set_price_cutover_round(11, 100)
            self._seed_api_rounds(st, rounds)
            ctx = main.SheetContext(
                ws_id=11,
                title="tab-main",
                ws=None,
                league_id=1,
                kind="main",
                expected_cols=len(main.BASE_HEADERS) + 1 + 6,
                round_col_idx=6,
            )
            fake_round_api = FakeRoundAPI()
            enq = main.enqueue_main_sheet_ops(
                None,
                st,
                {11: ctx},
                {"a1": "Power Up Boost"},
                ["Power Up Boost"],
                fake_round_api,  # type: ignore[arg-type]
            )
            self.assertEqual(enq, 4)
            # API is now always the primary source regardless of round age.
            # All 4 rounds must be fetched via API; DB data is only a fallback.
            self.assertEqual(fake_round_api.calls, [101, 102, 103, 104])
            due = st.fetch_due_ops(limit=10)
            self.assertEqual(len(due), 4)
            self.assertEqual(due[0]["round_id"], 101)
            payload = json.loads(due[0]["payload_json"])
            # Ability count comes from FakeRoundAPI (returns {"a1": 1}), not DB.
            self.assertEqual(payload["row"][12], 1)
            st.close()
        finally:
            try:
                os.remove(path)
            except OSError:
                pass

    def test_build_clan_round_rows(self) -> None:
        rec = {
            "snapshot_ts": "2026-04-01T19:36:53+00:00",
            "league_id": 1,
            "round_id": 899355,
        }
        rows = main.build_clan_round_rows(
            rec,
            [
                {
                    "snapshot_round_id": 899348,
                    "clan_id": 33714,
                    "clan_name": "DOGE",
                    "members_total": 108,
                    "members_seen": 1,
                    "member_coverage": 0.0092,
                    "team_th": 366208.75,
                    "team_pps": 504041.72,
                    "calc_mode": "league_fallback",
                }
            ],
        )
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row[1], 1)
        self.assertEqual(row[2], 899355)
        self.assertEqual(row[3], 899348)
        self.assertEqual(row[4], 33714)
        self.assertEqual(row[5], "DOGE")
        self.assertEqual(row[11], "league_fallback")

    def test_state_pending_upsert(self) -> None:
        fd, path = tempfile.mkstemp(prefix="sync_state_", suffix=".sqlite3")
        os.close(fd)
        try:
            st = main.StateStore(path)
            st.upsert_sheet_meta(11, "tab", 1)
            self.assertIsNone(st.get_price_cutover_round(11))
            st.set_price_cutover_round(11, 42)
            self.assertEqual(st.get_price_cutover_round(11), 42)
            st.enqueue_op(11, "append_round", 100, "abc", {"row": [1, 2], "finalized": 0})
            st.enqueue_op(11, "append_round", 100, "def", {"row": [1, 3], "finalized": 1})
            due = st.fetch_due_ops(limit=10)
            self.assertEqual(len(due), 1)
            self.assertEqual(due[0]["checksum"], "def")
            st.close()
        finally:
            try:
                os.remove(path)
            except OSError:
                pass

    def test_state_api_round_cache_methods(self) -> None:
        fd, path = tempfile.mkstemp(prefix="sync_state_", suffix=".sqlite3")
        os.close(fd)
        try:
            st = main.StateStore(path)
            schema_row = st.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='api_round_cache'"
            ).fetchone()
            self.assertIsNotNone(schema_row)

            st.upsert_api_round_record(
                {
                    "snapshot_ts": "2026-04-18T10:00:00+00:00",
                    "league_id": 1,
                    "round_id": 10,
                    "gmt_fund": 100.0,
                    "blocks_mined": 25,
                    "gmt_per_block": 4.0,
                    "ended_at": "2026-04-18T10:01:00+00:00",
                }
            )
            st.upsert_api_round_record(
                {
                    "snapshot_ts": "2026-04-18T10:05:00+00:00",
                    "league_id": 1,
                    "round_id": 11,
                    "gmt_fund": 50.0,
                    "blocks_mined": 10,
                    "gmt_per_block": 5.0,
                    "ended_at": None,
                }
            )

            completed = st.fetch_api_completed_rounds(1, since_round=0, limit=20)
            self.assertEqual([r["round_id"] for r in completed], [10])
            latest_completed = st.fetch_latest_api_completed_round_id(1)
            self.assertEqual(latest_completed, 10)
            latest_any = st.fetch_latest_api_round_record(1)
            self.assertIsNotNone(latest_any)
            self.assertEqual(latest_any["round_id"], 11)

            marked = st.mark_api_round_ended(1, 11, "2026-04-18T10:06:00+00:00")
            self.assertEqual(marked, 1)
            latest_completed = st.fetch_latest_api_completed_round_id(1)
            self.assertEqual(latest_completed, 11)
            st.close()
        finally:
            try:
                os.remove(path)
            except OSError:
                pass

    def test_sync_api_round_cache_rollover_closes_previous_round(self) -> None:
        class StubRoundMetricsAPI:
            def __init__(self) -> None:
                self.calls: List[int] = []

            def fetch_round_metrics_triplet(
                self,
                league_id: int,
                strict_league_validation: bool = False,
            ) -> Dict[str, Any] | None:
                _ = strict_league_validation
                self.calls.append(league_id)
                return {
                    "snapshot_ts": "2026-04-18T10:30:00+00:00",
                    "league_id": league_id,
                    "round_id": 11,
                    "block_number": 100,
                    "multiplier": 1.0,
                    "gmt_fund": 10.0,
                    "blocks_mined": 2,
                    "gmt_per_block": 5.0,
                    "league_th": 1500.0,
                    "efficiency_league": 20.0,
                    "ended_at": None,
                    "round_duration_sec": None,
                }

        fd, path = tempfile.mkstemp(prefix="sync_state_", suffix=".sqlite3")
        os.close(fd)
        orig_rollover = main.ROUND_CLOSE_ON_ROLLOVER
        try:
            st = main.StateStore(path)
            st.upsert_api_round_record(
                {
                    "snapshot_ts": "2026-04-18T10:20:00+00:00",
                    "league_id": 1,
                    "round_id": 10,
                    "ended_at": None,
                }
            )
            main.ROUND_CLOSE_ON_ROLLOVER = True
            stub = StubRoundMetricsAPI()
            written, seen = main.sync_api_round_cache(st, stub, [1])
            self.assertEqual((written, seen), (1, 1))
            open_latest = st.fetch_latest_api_round_record(1)
            self.assertIsNotNone(open_latest)
            self.assertEqual(open_latest["round_id"], 11)
            completed = st.fetch_api_completed_rounds(1, since_round=0, limit=20)
            self.assertEqual([r["round_id"] for r in completed], [10])
            self.assertEqual(completed[0]["ended_at"], "2026-04-18T10:30:00+00:00")
            st.close()
        finally:
            main.ROUND_CLOSE_ON_ROLLOVER = orig_rollover
            try:
                os.remove(path)
            except OSError:
                pass

    def test_sync_api_round_cache_rollover_disabled_keeps_previous_open(self) -> None:
        class StubRoundMetricsAPI:
            def fetch_round_metrics_triplet(
                self,
                league_id: int,
                strict_league_validation: bool = False,
            ) -> Dict[str, Any] | None:
                _ = (league_id, strict_league_validation)
                return {
                    "snapshot_ts": "2026-04-18T10:30:00+00:00",
                    "league_id": 1,
                    "round_id": 11,
                    "block_number": 100,
                    "multiplier": 1.0,
                    "gmt_fund": 10.0,
                    "blocks_mined": 2,
                    "gmt_per_block": 5.0,
                    "league_th": 1500.0,
                    "efficiency_league": 20.0,
                    "ended_at": None,
                    "round_duration_sec": None,
                }

        fd, path = tempfile.mkstemp(prefix="sync_state_", suffix=".sqlite3")
        os.close(fd)
        orig_rollover = main.ROUND_CLOSE_ON_ROLLOVER
        try:
            st = main.StateStore(path)
            st.upsert_api_round_record(
                {
                    "snapshot_ts": "2026-04-18T10:20:00+00:00",
                    "league_id": 1,
                    "round_id": 10,
                    "ended_at": None,
                }
            )
            main.ROUND_CLOSE_ON_ROLLOVER = False
            written, seen = main.sync_api_round_cache(st, StubRoundMetricsAPI(), [1])
            self.assertEqual((written, seen), (1, 1))
            completed = st.fetch_api_completed_rounds(1, since_round=0, limit=20)
            self.assertEqual(len(completed), 0)
            prev = st.conn.execute(
                "SELECT ended_at FROM api_round_cache WHERE league_id=1 AND round_id=10"
            ).fetchone()
            self.assertIsNotNone(prev)
            self.assertIsNone(prev["ended_at"])
            st.close()
        finally:
            main.ROUND_CLOSE_ON_ROLLOVER = orig_rollover
            try:
                os.remove(path)
            except OSError:
                pass

    def test_sync_api_round_cache_skips_failed_league_and_continues(self) -> None:
        class StubRoundMetricsAPI:
            def fetch_round_metrics_triplet(
                self,
                league_id: int,
                strict_league_validation: bool = False,
            ) -> Dict[str, Any] | None:
                _ = strict_league_validation
                if league_id == 3:
                    return None
                return {
                    "snapshot_ts": "2026-04-18T10:30:00+00:00",
                    "league_id": league_id,
                    "round_id": 11,
                    "block_number": 100,
                    "multiplier": 1.0,
                    "gmt_fund": 10.0,
                    "blocks_mined": 2,
                    "gmt_per_block": 5.0,
                    "league_th": 1500.0,
                    "efficiency_league": 20.0,
                    "ended_at": None,
                    "round_duration_sec": None,
                }

        fd, path = tempfile.mkstemp(prefix="sync_state_", suffix=".sqlite3")
        os.close(fd)
        try:
            st = main.StateStore(path)
            written, seen = main.sync_api_round_cache(st, StubRoundMetricsAPI(), [1, 3])
            self.assertEqual((written, seen), (1, 1))
            rec_ok = st.fetch_latest_api_round_record(1)
            rec_skip = st.fetch_latest_api_round_record(3)
            self.assertIsNotNone(rec_ok)
            self.assertIsNone(rec_skip)
            st.close()
        finally:
            try:
                os.remove(path)
            except OSError:
                pass

    def test_enqueue_main_sheet_ops_no_db_round_fetch_dependency(self) -> None:
        class LegacyDB:
            def fetch_completed_rounds_from_db(self, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
                raise AssertionError("fetch_completed_rounds_from_db must not be called")

            def fetch_latest_completed_round_id(self, *args: Any, **kwargs: Any) -> int:
                raise AssertionError("fetch_latest_completed_round_id must not be called")

        class FakeRoundAPI:
            def fetch_round_ability_counts(
                self,
                round_id: int,
                expected_league_id: int | None = None,
                progress_hook: Any | None = None,
            ) -> Dict[str, int] | None:
                _ = (round_id, expected_league_id, progress_hook)
                return {"a1": 2}

        fd, path = tempfile.mkstemp(prefix="sync_state_", suffix=".sqlite3")
        os.close(fd)
        try:
            st = main.StateStore(path)
            st.upsert_sheet_meta(11, "tab-main", 1)
            st.set_last_synced_round(11, 100)
            st.set_price_cutover_round(11, 100)
            self._seed_api_rounds(
                st,
                [
                    {
                        "snapshot_ts": "2026-04-01T22:00:00+00:00",
                        "league_id": 1,
                        "round_id": 101,
                        "block_number": 5000,
                        "multiplier": 1.0,
                        "gmt_fund": 10.0,
                        "gmt_per_block": 0.1,
                        "league_th": 1000.0,
                        "ended_at": "2026-04-01T22:00:00+00:00",
                        "round_duration_sec": 60,
                        "blocks_mined": 100,
                        "efficiency_league": 20.0,
                    }
                ],
            )
            ctx = main.SheetContext(
                ws_id=11,
                title="tab-main",
                ws=None,
                league_id=1,
                kind="main",
                expected_cols=len(main.BASE_HEADERS) + 1 + 6,
                round_col_idx=6,
            )
            enq = main.enqueue_main_sheet_ops(
                LegacyDB(),  # type: ignore[arg-type]
                st,
                {11: ctx},
                {"a1": "Power Up Boost"},
                ["Power Up Boost"],
                FakeRoundAPI(),  # type: ignore[arg-type]
            )
            self.assertEqual(enq, 1)
            due = st.fetch_due_ops(limit=10)
            self.assertEqual(len(due), 1)
            self.assertEqual(due[0]["round_id"], 101)
            st.close()
        finally:
            try:
                os.remove(path)
            except OSError:
                pass

    def test_normalize_payload_row(self) -> None:
        row = ["2026-02-25T17:00:00+00:00", 1]
        norm = main._normalize_payload_row(row, expected_cols=6, round_id=999)
        self.assertEqual(len(norm), 6)
        self.assertEqual(norm[0], "2026-02-25T17:00:00+00:00")
        self.assertEqual(norm[1], 1)
        self.assertEqual(norm[2:], ["", "", "", ""])

        row2 = ["ts", 1, 2, 3, 4, 5, 6, 7]
        norm2 = main._normalize_payload_row(row2, expected_cols=8, round_id=12345)
        self.assertEqual(norm2[6], 12345)

        row3 = ["ts", 1, 2, 3]
        norm3 = main._normalize_payload_row(row3, expected_cols=6, round_id=77, round_col_idx=2)
        self.assertEqual(norm3[2], 77)

    def test_backfill_normalize_round_records_skips_non_dict(self) -> None:
        items: List[Any] = [{"round_id": 1}, "bad", 42, {"round_id": 2}]
        normalized = backfill._normalize_round_records(items, context="test")
        self.assertEqual(len(normalized), 2)
        self.assertEqual([main.safe_int(x.get("round_id")) for x in normalized], [1, 2])

    def test_backfill_profile_resolution_defaults_to_stable(self) -> None:
        name, cfg = backfill._resolve_backfill_profile("unknown-profile")
        self.assertEqual(name, "stable")
        self.assertEqual(cfg["workers"], backfill.BACKFILL_PROFILE_PRESETS["stable"]["workers"])
        self.assertEqual(cfg["gomining_rpm"], backfill.BACKFILL_PROFILE_PRESETS["stable"]["gomining_rpm"])
        self.assertEqual(cfg["prefetch_attempts"], backfill.BACKFILL_PROFILE_PRESETS["stable"]["prefetch_attempts"])
        name2, cfg2 = backfill._resolve_backfill_profile("aggressive")
        self.assertEqual(name2, "aggressive")
        self.assertEqual(cfg2["workers"], backfill.BACKFILL_PROFILE_PRESETS["aggressive"]["workers"])


if __name__ == "__main__":
    unittest.main()
