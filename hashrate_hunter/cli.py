from __future__ import annotations

import argparse

from .sync.service import main as service_main


def cli_main() -> None:
    parser = argparse.ArgumentParser(description="Hashrate Hunter continuous sync service")
    parser.add_argument(
        "--once-reconcile",
        action="store_true",
        help="Run one reconcile pass (plus queue flush) and exit.",
    )
    args = parser.parse_args()
    service_main(once_reconcile=bool(args.once_reconcile))
