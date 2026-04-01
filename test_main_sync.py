import os
import tempfile
import unittest
from typing import Any, Dict, List

import main


class SyncCoreTests(unittest.TestCase):
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
        self.assertEqual(row[-3:-1], [3, 0])
        self.assertAlmostEqual(row[-1], 555.0)

    def test_build_canonical_row_cutover_blocks_old_round(self) -> None:
        rec = {
            "snapshot_ts": "2026-02-25T13:49:20.702613+00:00",
            "league_id": 1,
            "round_id": 123,
            "league_th": 1000.0,
            "efficiency_league": 20.0,
        }
        row = main.build_canonical_row(rec, [], {}, price_cutover_round=123, power_up_gmt_value=999.0)
        self.assertEqual(row[-1], "")

    def test_formula_helpers(self) -> None:
        self.assertAlmostEqual(main.calc_league_pps(1000.0, 20.0), 1400.0)
        self.assertAlmostEqual(main.calc_power_up_gmt(1000.0, 20.0), 1400.0 * 0.0389)
        self.assertAlmostEqual(main.calc_team_pps_exact(10.0), 280.0)
        self.assertAlmostEqual(main.calc_team_pps_fallback(100.0, 20.0), 140.0)
        self.assertAlmostEqual(main.calc_clan_shield_gmt(200.0), 0.111)
        self.assertIsNone(main.calc_power_up_gmt(1000.0, 0.0))
        self.assertIsNone(main.calc_team_pps_fallback(None, 20.0))

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
        class FakeDB:
            def fetch_latest_completed_round_id(self, league_id: int) -> int:
                return 12

            def fetch_completed_rounds_from_db(self, league_id: int, since_round: int, limit: int) -> List[Dict[str, Any]]:
                return [
                    {"round_id": 11, "league_id": league_id, "snapshot_ts": "2026-03-31T12:00:00+00:00"},
                    {"round_id": 12, "league_id": league_id, "snapshot_ts": "2026-03-31T12:02:00+00:00"},
                ]

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
                        "clan_shield_gmt": main.calc_clan_shield_gmt(200.0),
                        "calc_mode": "api_exact",
                    }
                ]

        fd, path = tempfile.mkstemp(prefix="sync_state_", suffix=".sqlite3")
        os.close(fd)
        try:
            st = main.StateStore(path)
            st.upsert_sheet_meta(11, "tab-clan", 1)
            st.set_last_synced_round(11, 10)
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
            enq = main.enqueue_clan_sheet_ops(FakeDB(), st, {11: ctx}, fake_api)  # type: ignore[arg-type]
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
        class FakeDB:
            def fetch_latest_completed_round_id(self, league_id: int) -> int:
                return 12

            def fetch_completed_rounds_from_db(self, league_id: int, since_round: int, limit: int) -> List[Dict[str, Any]]:
                return [
                    {"round_id": 11, "league_id": league_id, "snapshot_ts": "2026-03-31T12:00:00+00:00"},
                    {"round_id": 12, "league_id": league_id, "snapshot_ts": "2026-03-31T12:02:00+00:00"},
                ]

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
                        "clan_shield_gmt": main.calc_clan_shield_gmt(200.0),
                        "calc_mode": "api_exact",
                    }
                ]

        fd, path = tempfile.mkstemp(prefix="sync_state_", suffix=".sqlite3")
        os.close(fd)
        try:
            st = main.StateStore(path)
            st.upsert_sheet_meta(11, "tab-clan", 1)
            st.set_last_synced_round(11, 10)
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
            enq = main.enqueue_clan_sheet_ops(FakeDB(), st, {11: ctx}, fake_api)  # type: ignore[arg-type]
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
                    "clan_shield_gmt": 279.74,
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
        self.assertEqual(row[12], "league_fallback")

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


if __name__ == "__main__":
    unittest.main()
