#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import sys
from types import ModuleType
from typing import Any

from hashrate_hunter import compat as _compat
from hashrate_hunter.cli import cli_main
from hashrate_hunter.compat import *  # noqa: F401,F403 - legacy facade exports


class _MainFacade(ModuleType):
    def __setattr__(self, name: str, value: Any) -> None:
        super().__setattr__(name, value)
        if not name.startswith("__"):
            _compat.propagate_assignment(name, value)


sys.modules[__name__].__class__ = _MainFacade


if __name__ == "__main__":
    cli_main()
