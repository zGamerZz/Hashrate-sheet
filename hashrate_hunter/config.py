from __future__ import annotations

import os
from typing import List, Tuple

try:
    import dotenv
except Exception:  # pragma: no cover
    dotenv = None  # type: ignore

if dotenv is not None:
    dotenv.load_dotenv()

# Config
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "service_acc.json")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1bq5Sy2pV35x33Q12G5_EJ2S0Kb1iXo1Y2FCxL7QIxGg")

SYNC_POLL_SECONDS = int(os.getenv("SYNC_POLL_SECONDS", "5"))
SYNC_API_GAP_FILL_MAX = max(1, int(os.getenv("SYNC_API_GAP_FILL_MAX", "20")))
SYNC_API_GAP_FILL_PROBE_MAX = max(
    1,
    int(
        os.getenv(
            "SYNC_API_GAP_FILL_PROBE_MAX",
            str(max(SYNC_API_GAP_FILL_MAX * 10, 120)),
        )
    ),
)
SHEET_REFRESH_SECONDS = int(os.getenv("SHEET_REFRESH_SECONDS", "1800"))
GS_WRITE_REQ_PER_MIN = int(os.getenv("GS_WRITE_REQ_PER_MIN", "45"))
GS_READ_REQ_PER_MIN = int(os.getenv("GS_READ_REQ_PER_MIN", "20"))
STATE_DB_PATH = os.getenv("STATE_DB_PATH", "./sync_state.sqlite")
LOCK_FILE_PATH = os.getenv("LOCK_FILE_PATH", "./sync.lock")
LEAGUES_API_URL = os.getenv("LEAGUES_API_URL", "https://api.gomining.com/api/nft-game/league/index")
GOMINING_BEARER_TOKEN = os.getenv("GOMINING_BEARER_TOKEN", "")
GOMINING_API_BASE_URL = os.getenv("GOMINING_API_BASE_URL", "https://api.gomining.com").rstrip("/")
MULTIPLIER_PATH = os.getenv("MULTIPLIER_PATH", "/api/nft-game/round/get-last")
PLAYER_LEADERBOARD_PATH = os.getenv("PLAYER_LEADERBOARD_PATH", "/api/nft-game/user-leaderboard/index")
ROUND_METRICS_CLAN_PATH = os.getenv("ROUND_METRICS_CLAN_PATH", "/api/nft-game/clan-leaderboard/index-v2")
ROUND_CLAN_LEADERBOARD_PATH = os.getenv("ROUND_CLAN_LEADERBOARD_PATH", "/api/nft-game/round/clan-leaderboard")
ROUND_METRICS_TIMEOUT_SECONDS = int(os.getenv("ROUND_METRICS_TIMEOUT_SECONDS", "45"))
ROUND_METRICS_MAX_RETRIES = max(1, int(os.getenv("ROUND_METRICS_MAX_RETRIES", "4")))
ROUND_METRICS_USER_PAGE_LIMIT = max(1, min(50, int(os.getenv("ROUND_METRICS_USER_PAGE_LIMIT", "50"))))
ROUND_METRICS_CLAN_PAGE_LIMIT = max(1, min(50, int(os.getenv("ROUND_METRICS_CLAN_PAGE_LIMIT", "1"))))
ROUND_GET_LAST_SCAN_LOOKBACK = max(1, int(os.getenv("ROUND_GET_LAST_SCAN_LOOKBACK", "200")))
ROUND_CLOSE_ON_ROLLOVER = os.getenv("ROUND_CLOSE_ON_ROLLOVER", "1").strip() in {"1", "true", "TRUE", "yes", "YES"}
ROUND_GET_LAST_STRICT_LEAGUE_VALIDATION = os.getenv(
    "ROUND_GET_LAST_STRICT_LEAGUE_VALIDATION",
    "0",
).strip() in {"1", "true", "TRUE", "yes", "YES"}
CLAN_LEADERBOARD_API_URL = os.getenv(
    "CLAN_LEADERBOARD_API_URL",
    "https://api.gomining.com/api/nft-game/clan-leaderboard/index-v2",
)
CLAN_GET_BY_ID_API_URL = os.getenv(
    "CLAN_GET_BY_ID_API_URL",
    "https://api.gomining.com/api/nft-game/clan/get-by-id",
)
CLAN_API_PAGE_LIMIT = max(1, min(50, int(os.getenv("CLAN_API_PAGE_LIMIT", "50"))))
CLAN_API_TIMEOUT_SECONDS = int(os.getenv("CLAN_API_TIMEOUT_SECONDS", "45"))
CLAN_API_MAX_RETRIES = max(1, int(os.getenv("CLAN_API_MAX_RETRIES", "4")))
ENABLE_CLAN_SYNC = os.getenv("ENABLE_CLAN_SYNC", "0").strip() in {"1", "true", "TRUE", "yes", "YES"}
ROUND_USER_LEADERBOARD_API_URL = os.getenv(
    "ROUND_USER_LEADERBOARD_API_URL",
    "https://api.gomining.com/api/nft-game/round/user-leaderboard",
)
ROUND_API_PAGE_LIMIT = max(1, min(50, int(os.getenv("ROUND_API_PAGE_LIMIT", "50"))))
ROUND_API_TIMEOUT_SECONDS = int(os.getenv("ROUND_API_TIMEOUT_SECONDS", "45"))
ROUND_API_MAX_RETRIES = max(1, int(os.getenv("ROUND_API_MAX_RETRIES", "4")))
GOMINING_API_REQ_PER_MIN = int(os.getenv("GOMINING_API_REQ_PER_MIN", "120"))
TOKEN_URL = os.getenv("TOKEN_URL", "").strip()
TOKEN_X_AUTH = os.getenv("TOKEN_X_AUTH", "").strip()
TOKEN_METHOD = os.getenv("TOKEN_METHOD", "GET").strip().upper()
TOKEN_TIMEOUT_SECONDS = max(1, int(os.getenv("TOKEN_TIMEOUT_SECONDS", "20")))
TOKEN_VERIFY_SSL = os.getenv("TOKEN_VERIFY_SSL", "1").strip() in {"1", "true", "TRUE", "yes", "YES"}
try:
    TOKEN_SHARED_REFRESH_MIN_INTERVAL_SECONDS = max(
        0.0,
        float(os.getenv("TOKEN_SHARED_REFRESH_MIN_INTERVAL_SECONDS", "1.5")),
    )
except Exception:
    TOKEN_SHARED_REFRESH_MIN_INTERVAL_SECONDS = 1.5
try:
    TOKEN_SHARED_REFRESH_WAIT_TIMEOUT_SECONDS = max(
        0.1,
        float(os.getenv("TOKEN_SHARED_REFRESH_WAIT_TIMEOUT_SECONDS", "5.0")),
    )
except Exception:
    TOKEN_SHARED_REFRESH_WAIT_TIMEOUT_SECONDS = 5.0
LEAGUES_API_POLL_SECONDS = int(os.getenv("LEAGUES_API_POLL_SECONDS", str(SHEET_REFRESH_SECONDS)))
LEAGUE_INDEX_LOOKBACK_DAYS = max(0, int(os.getenv("LEAGUE_INDEX_LOOKBACK_DAYS", "7")))
LEAGUE_INDEX_TRY_WITHOUT_CALCULATED_AT = os.getenv(
    "LEAGUE_INDEX_TRY_WITHOUT_CALCULATED_AT",
    "1",
).strip() in {"1", "true", "TRUE", "yes", "YES"}
ENABLE_DB_FALLBACK_RAW = os.getenv("ENABLE_DB_FALLBACK", "").strip()

STABILIZATION_ROUNDS = int(os.getenv("STABILIZATION_ROUNDS", "2"))
MAX_ROUNDS_PER_POLL = int(os.getenv("MAX_ROUNDS_PER_POLL", "250"))
QUEUE_FETCH_LIMIT = int(os.getenv("QUEUE_FETCH_LIMIT", "200"))
APPEND_BATCH_SIZE = int(os.getenv("APPEND_BATCH_SIZE", "25"))
ROW_MAP_KEEP_PER_SHEET = int(os.getenv("ROW_MAP_KEEP_PER_SHEET", "500"))
MAX_MISSING_SHEET_RETRIES = int(os.getenv("MAX_MISSING_SHEET_RETRIES", "20"))
AUTO_EXPAND_SHEET_ROWS = os.getenv("AUTO_EXPAND_SHEET_ROWS", "1").strip() in {"1", "true", "TRUE", "yes", "YES"}
DROP_NON_RETRYABLE_SHEET_ERRORS = os.getenv("DROP_NON_RETRYABLE_SHEET_ERRORS", "1").strip() in {"1", "true", "TRUE", "yes", "YES"}
PURGE_QUEUE_FOR_MISSING_SHEETS = os.getenv("PURGE_QUEUE_FOR_MISSING_SHEETS", "1").strip() in {"1", "true", "TRUE", "yes", "YES"}
DRY_RUN = os.getenv("DRY_RUN", "0").strip() in {"1", "true", "TRUE", "yes", "YES"}
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").strip().upper()
DEBUG_VERBOSE = os.getenv("DEBUG_VERBOSE", "1").strip() in {"1", "true", "TRUE", "yes", "YES"}
LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", "")
QUEUE_DEBUG_PREVIEW = int(os.getenv("QUEUE_DEBUG_PREVIEW", "180"))
HEARTBEAT_SECONDS = int(os.getenv("HEARTBEAT_SECONDS", "30"))
try:
    ENQUEUE_FLUSH_EVERY_SECONDS = max(0.0, float(os.getenv("ENQUEUE_FLUSH_EVERY_SECONDS", "5")))
except Exception:
    ENQUEUE_FLUSH_EVERY_SECONDS = 5.0
RECONCILE_INTERVAL_SECONDS = int(os.getenv("RECONCILE_INTERVAL_SECONDS", "900"))
ROUND_SOFT_FAIL_MAX_ATTEMPTS = max(1, int(os.getenv("ROUND_SOFT_FAIL_MAX_ATTEMPTS", "8")))
try:
    ROUND_RETRY_BACKOFF_MIN_SECONDS = max(1.0, float(os.getenv("ROUND_RETRY_BACKOFF_MIN_SECONDS", "15")))
except Exception:
    ROUND_RETRY_BACKOFF_MIN_SECONDS = 15.0
try:
    ROUND_RETRY_BACKOFF_MAX_SECONDS = max(
        ROUND_RETRY_BACKOFF_MIN_SECONDS,
        float(os.getenv("ROUND_RETRY_BACKOFF_MAX_SECONDS", "600")),
    )
except Exception:
    ROUND_RETRY_BACKOFF_MAX_SECONDS = max(ROUND_RETRY_BACKOFF_MIN_SECONDS, 600.0)
GAP_SCAN_LOOKBACK_ROUNDS = max(10, int(os.getenv("GAP_SCAN_LOOKBACK_ROUNDS", "500")))
RECONCILE_DEEP_MISSING_PER_PASS = max(0, int(os.getenv("RECONCILE_DEEP_MISSING_PER_PASS", "50")))
ADAPTIVE_RPM_ENABLE = os.getenv("ADAPTIVE_RPM_ENABLE", "1").strip() in {"1", "true", "TRUE", "yes", "YES"}
ADAPTIVE_RPM_MIN = max(1, int(os.getenv("ADAPTIVE_RPM_MIN", "120")))
ADAPTIVE_RPM_MAX = max(ADAPTIVE_RPM_MIN, int(os.getenv("ADAPTIVE_RPM_MAX", str(GOMINING_API_REQ_PER_MIN))))

CONFIG_CELL = "B1"
LOG_START_ROW = 4

MAIN_SHEET_MARKER = "leagueId_to_log"
CLAN_SHEET_MARKER = "leagueId_to_log_clan"
CLAN_TAB_SUFFIX = " - Clan"

LEAGUE_TH_HEADER = "League global TH"
LEAGUE_TH_COL_INDEX = 7  # 0-based index, column H in the sheet.
CLAN_LEGACY_GMT_COL_INDEX = 11  # 0-based index, legacy column L in clan tabs.
MIGRATION_CHECKED_SHEETS: set[int] = set()
POWER_UP_PRICE_HEADER = "Power Up GMT Price"
POWER_UP_PRICE_SENTINEL_HEADER = "Power Up GMT Price (Sentinel)"
CLAN_POWER_UP_PRICE_HEADER = "Clan Power Up GMT Price"
CLAN_POWER_UP_PRICE_SENTINEL_HEADER = "Clan Power Up GMT Price (Sentinel)"
MISSING_HEADER = "missing"
EXCLUDED_USER_BOOST_AUDIT_HEADER = "Excluded User 2144425 Boosts (Audit)"
BTC_FUND_HEADER = "btcFund"
EXCLUDED_BOOST_USER_ID = 2144425

PPS_BASE_EE_W_PER_TH = 20.0
# Legacy alias kept to minimize churn in downstream imports/tests.
PPS_FACTOR = PPS_BASE_EE_W_PER_TH
POWER_UP_GMT_FACTOR = 0.0389
POWER_UP_ABILITY_ID = "5d6f8166-0f20-486f-b920-e898ca94dcc1"
CLAN_POWER_UP_ABILITY_ID = "8a9b57a9-8a17-4647-8b2f-a164dc52b1f4"
CLAN_POWER_UP_GMT_FACTOR = 0.000555
CLAN_EXACT_MEMBER_COVERAGE_THRESHOLD = float(os.getenv("CLAN_EXACT_MEMBER_COVERAGE_THRESHOLD", "0.90"))
CLAN_EXACT_POWER_COVERAGE_THRESHOLD = float(os.getenv("CLAN_EXACT_POWER_COVERAGE_THRESHOLD", "0.90"))
CLAN_SNAPSHOT_MIN_COVERAGE = float(os.getenv("CLAN_SNAPSHOT_MIN_COVERAGE", "0.50"))
CLAN_SNAPSHOT_FALLBACK_ROUND_WINDOW = int(os.getenv("CLAN_SNAPSHOT_FALLBACK_ROUND_WINDOW", "250"))

BASE_HEADERS: List[str] = [
    "timestamp_utc",
    "leagueId",
    "blockNumber",
    "multiplier",
    "gmtFund",
    "gmtPerBlock",
    "roundId",
    LEAGUE_TH_HEADER,
    "endedAt_utc",
    "roundDuration_sec",
    "blocks_mined",
    "efficiency_league",
]

CLAN_HEADERS: List[str] = [
    "timestamp_utc",
    "leagueId",
    "roundId",
    "snapshotRoundId",
    "clanId",
    "clanName",
    "members_total",
    "members_seen",
    "member_coverage",
    "team_th",
    "team_pps",
    "calc_mode",
]

# Fixed, clean boost layout (no stealer/maintenance/other columns).
ABILITY_HEADER_ORDER: List[str] = [
    "Power Up Boost",
    "Clan Power Up Boost",
    "Rocket (x1)",
    "Rocket (x10)",
    "Rocket (x100)",
    "Instant Boost (x1)",
    "Instant Boost (x10)",
    "Instant Boost (x100)",
    "Echo Boost (x1)",
    "Echo Boost (x10)",
    "Echo Boost (x100)",
    "Focus Boost (x1)",
    "Focus Boost (x10)",
    "Focus Boost (x100)",
]

# Static ability mapping source (API-only abilities path, independent from DB ability_dim).
# Format: (ability_id, ability_name, sort_order)
ABILITY_DIM_STATIC: List[Tuple[str, str, int]] = [
    (CLAN_POWER_UP_ABILITY_ID, "Clan Power Up Boost", 1),
    (POWER_UP_ABILITY_ID, "Power Up Boost", 2),
    ("646d3c76-fe06-414f-8d39-eb0aa7da429a", "Echo Boost (x1)", 3),
    ("2ebfd30d-2950-46fc-b3e9-498a924fd021", "Echo Boost (x10)", 4),
    ("0ffad19c-8091-4527-a600-b7c2f238147d", "Echo Boost (x100)", 5),
    ("370be7b3-d843-4e2f-a23a-eeea37648f99", "Instant Boost (x1)", 6),
    ("a7554d7f-0e50-45e0-ab01-6ac8db629bfd", "Instant Boost (x10)", 7),
    ("8b6fad11-cfc1-4922-ad17-30c41cee39b9", "Instant Boost (x100)", 8),
    ("592724c5-1f59-43b2-af13-2de8d2c97a9d", "Focus Boost (x1)", 9),
    ("a74dd2e0-14a9-46e3-86e5-1f1e8ab1f5c4", "Focus Boost (x10)", 10),
    ("bea03b75-6dbf-4a99-a2c9-24dfe79a3ea1", "Focus Boost (x100)", 11),
    ("3f735c47-cead-4d15-8dad-7db4a7b3f4e7", "Rocket (x1)", 12),
    ("17831b5e-afdf-4cb7-98d7-3a60d81e19ae", "Boost (x10)", 13),
    ("2ec728e7-f31f-4df3-b486-5e82a4976563", "Boost (x10)", 13),
    ("e7af0c32-1409-4b22-9e0b-99e7ea4939df", "Rocket (x100)", 14),
]


__all__ = [name for name in globals() if name.isupper()]
