from __future__ import annotations

from datetime import datetime
from pathlib import Path


def run_directory(root_dir: Path, source: str, started_at: datetime) -> Path:
  date_dir = started_at.strftime("%Y-%m-%d")
  year_dir = started_at.strftime("%Y")
  run_id = started_at.strftime("run-%Y%m%dT%H%M%SZ")
  return root_dir / "captures" / source / year_dir / date_dir / run_id
