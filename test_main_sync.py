import os
import tempfile
import unittest

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
        row = main.build_canonical_row(rec, ability_headers, counts, price_cutover_round=122)
        self.assertEqual(row[1], 1)
        self.assertEqual(row[6], 123)
        self.assertAlmostEqual(row[7], 1234.5)
        self.assertEqual(row[10], 662)
        self.assertAlmostEqual(row[11], 20.49)
        self.assertEqual(row[-3:-1], [3, 0])
        self.assertAlmostEqual(row[-1], main.calc_power_up_gmt(1234.5, 20.49))

    def test_build_canonical_row_cutover_blocks_old_round(self) -> None:
        rec = {
            "snapshot_ts": "2026-02-25T13:49:20.702613+00:00",
            "league_id": 1,
            "round_id": 123,
            "league_th": 1000.0,
            "efficiency_league": 20.0,
        }
        row = main.build_canonical_row(rec, [], {}, price_cutover_round=123)
        self.assertEqual(row[-1], "")

    def test_formula_helpers(self) -> None:
        self.assertAlmostEqual(main.calc_league_pps(1000.0, 20.0), 1400.0)
        self.assertAlmostEqual(main.calc_power_up_gmt(1000.0, 20.0), 1400.0 * 0.0389)
        self.assertAlmostEqual(main.calc_team_pps_exact(10.0), 280.0)
        self.assertAlmostEqual(main.calc_team_pps_fallback(100.0, 20.0), 140.0)
        self.assertAlmostEqual(main.calc_clan_shield_gmt(200.0), 0.111)
        self.assertIsNone(main.calc_power_up_gmt(1000.0, 0.0))
        self.assertIsNone(main.calc_team_pps_fallback(None, 20.0))

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
