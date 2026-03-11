from __future__ import annotations

from pathlib import Path

APP_NAME = "bet-recorder"


def default_data_dir(home: Path | None = None) -> Path:
  base_home = home if home is not None else Path.home()
  return base_home / ".local" / "share" / APP_NAME
